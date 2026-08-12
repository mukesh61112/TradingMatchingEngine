# TradingMatchingEngine
# High-Performance Multi-Symbol Electronic Order Matching Engine

A production-style, low-latency order matching engine written in C++20, implementing
price-time priority matching across multiple independent symbol order books, with a
lock-free, multi-threaded pipeline modeled on real exchange architectures (order gateway
→ matching core → market data publisher).

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Repository Structure](#repository-structure)
4. [Data Structures](#data-structures)
5. [Matching Logic](#matching-logic)
6. [Threading Model](#threading-model)
7. [Time & Space Complexity](#time--space-complexity)
8. [Error Handling](#error-handling)
9. [Design Decisions & Trade-offs](#design-decisions--trade-offs)
10. [Build Instructions](#build-instructions)
11. [Running Tests](#running-tests)
12. [Running Benchmarks](#running-benchmarks)
13. [Sample Input / Output](#sample-input--output)
14. [Future Improvements](#future-improvements)

---

## Overview

This engine supports:

- **Multi-symbol order books** — each symbol has a fully independent book.
- **Order types** — Limit, Market, IOC (Immediate-or-Cancel), Cancel, Modify.
- **Price-Time priority matching** with correct partial/multiple/complete fill semantics.
- **Self-trade prevention**.
- **Market data publishing** on every book-changing event.
- **Full book snapshots** with snapshot-based recovery.
- **Execution / event logging**.
- **Deterministic, single-writer-per-symbol matching** under a multi-producer,
  multi-threaded pipeline.

The design intentionally mirrors real exchange topology: independent stages connected by
lock-free single-producer/single-consumer (SPSC) queues, so that no stage ever blocks on
another and the hot path (order → match → publish) does no locking and minimal/no heap
allocation.

---

## Architecture

```
        Producer Threads (order sources / clients)
                       │
                       ▼
        ┌───────────────────────────┐
        │   ORDER GATEWAY SERVER     │   (order_server/)
        │   - validates request      │
        │   - FIFO-sequences orders  │
        │     from multiple clients  │
        │     into one deterministic │
        │     timestamped stream     │
        └─────────────┬──────────────┘
                       │  lock-free queue (ClientRequest)
                       ▼
        ┌───────────────────────────┐
        │   MATCHING ENGINE          │   (matcher/)
        │   - one thread per symbol  │
        │   - owns that symbol's     │
        │     order book exclusively │
        │   - price-time priority    │
        │     matching                │
        │   - self-trade prevention  │
        └───────┬──────────┬─────────┘
                │          │
     lock-free  │          │  lock-free queue
     queue      ▼          ▼  (MarketUpdate)
   (ClientResponse)  ┌───────────────────────────┐
                │     │   MARKET DATA PUBLISHER    │  (market_data/)
                ▼     │   - broadcasts book deltas │
        back to        │   - snapshot_synthesizer:  │
        Order Gateway   │     rebuilds full snapshot │
        → client        │     for recovery/late join │
                        └───────────────────────────┘
```

Each arrow is a **lock-free SPSC ring buffer** (`Common/lf_queue.h`). The three stages
(gateway, matcher, publisher) never share mutable state directly — only via these queues —
which removes the need for locks on the hot path and keeps matching deterministic.

### Why one matching thread per symbol?

Matching for a given symbol must be strictly serialized to preserve price-time priority
and determinism. Running one matching thread per symbol means:

- No locking is required *within* a symbol's book (single writer).
- Symbols scale independently and in parallel across cores.
- A slow/busy symbol cannot stall matching for other symbols.

---

## Repository Structure

```
Engine/
├── Common/                        # symbol-agnostic shared infrastructure
│   ├── types.h                    # OrderId, ClientId, Price, Qty, Side, Symbol, Nanos
│   ├── lf_queue.h                 # templated lock-free SPSC ring buffer
│   ├── mem_pool.h                 # fixed-size object pool (no runtime heap alloc)
│   ├── macros.h                   # branch hints, cache-line alignment, no-copy macros
│   ├── time_utils.h               # nanosecond timestamps
│   ├── thread_utils.h             # thread creation + CPU core affinity pinning
│   ├── logging.h / Log.cpp        # async, non-blocking logger
│   └── socket_utils.h, tcp_*.h/.cpp, mcast_socket.*   # optional network I/O layer
│
├── exchange/
│   ├── order_server/
│   │   ├── client_request.h       # inbound order request schema
│   │   ├── client_response.h      # ack / reject / fill / cancel-confirm schema
│   │   ├── fifo_sequencer.h       # deterministic multi-producer merge
│   │   └── order_server.cpp/h     # gateway thread, request validation
│   │
│   ├── matcher/
│   │   ├── me_order.h/.cpp        # order node (intrusive linked-list entry)
│   │   ├── me_order_book.h/.cpp   # per-symbol book: bids/asks price levels
│   │   └── matching_engine.cpp/h  # per-symbol matching thread + dispatch logic
│   │
│   └── market_data/
│       ├── market_update.h        # outbound delta schema (Add/Modify/Cancel/Trade/Clear)
│       ├── market_data_publisher.cpp/h
│       └── snapshot_synthesizer.cpp/h   # full-book snapshot + recovery
│
├── exchange_main.cpp               # wiring: queues, threads, affinity, lifecycle
│
├── tests/
│   ├── test_order_book.cpp         # matching, partial/complete fills, priority
│   ├── test_order_types.cpp        # market/IOC/modify semantics
│   ├── test_self_trade.cpp
│   └── test_error_handling.cpp     # dup IDs, bad symbol/price/qty, unknown orders
│
├── benchmarks/
│   └── benchmark_main.cpp          # throughput + latency percentiles
│
├── sample_io/
│   ├── sample_orders.txt
│   └── sample_output.txt
│
├── CMakeLists.txt
└── README.md
```

---

## Data Structures

### Order Book (`me_order_book.h`)

Each symbol owns one `OrderBook` instance containing two sides:

| Side | Container | Ordering |
|---|---|---|
| Bids | `std::map<Price, PriceLevel, std::greater<Price>>` | highest price first |
| Asks | `std::map<Price, PriceLevel, std::less<Price>>` | lowest price first |

Each `PriceLevel` holds an **intrusive doubly-linked list** of `MEOrder` nodes in
time-priority (FIFO) order at that price. This gives:

- O(log P) to find/insert/remove a price level (P = number of distinct price levels
  currently active — typically small relative to order count).
- O(1) to append a new order to the back of a level's list (new resting order).
- O(1) to remove any specific order from a level's list (cancel/modify/fill), because
  each `MEOrder` stores direct `prev`/`next` pointers — no linear scan required.

A side auxiliary `unordered_map<OrderId, MEOrder*>` gives **O(1) average** lookup from
order ID to its node for Cancel/Modify operations.

### Order Node (`me_order.h`)

```
struct MEOrder {
    OrderId    order_id;
    ClientId   client_id;
    Symbol     symbol;
    Side       side;
    OrderType  type;
    Price      price;
    Qty        qty;
    Qty        remaining_qty;
    Nanos      priority_ts;   // used for time priority, reset on modify per rules below
    MEOrder*   prev;
    MEOrder*   next;
};
```

### Memory Pooling

`MEOrder` and `PriceLevel` objects are allocated from `Common/mem_pool.h` — a fixed-size
free-list pool sized at startup — instead of `new`/`delete`, eliminating heap allocation
from the matching hot path.

---

## Matching Logic

### Price-Time Priority

- **Buy orders**: higher price has priority; ties broken by earlier timestamp.
- **Sell orders**: lower price has priority; ties broken by earlier timestamp.
- **Trade price** is always the resting (maker) order's price.

### Order Type Behavior

| Type | Behavior |
|---|---|
| **Limit** | Matches immediately against the opposite book while possible; unfilled remainder rests on the book. |
| **Market** | Consumes best available liquidity across price levels; any unfilled remainder is cancelled (never rests). |
| **IOC** | Matches immediately like a limit order at its price; any unfilled remainder is cancelled (never rests). |
| **Cancel** | Removes an active resting order in O(1) via its stored `prev`/`next` pointers. Cancelling an already-filled/absent order is ignored gracefully (no error). |
| **Modify** | Change price and/or quantity of a resting order: <br>• **Price change** → order re-inserted at new price level, timestamp reset (loses priority). <br>• **Quantity decrease** → priority/timestamp retained (stays in place in the list). <br>• **Quantity increase** → timestamp reset (moves to back of time priority), even at the same price. |

### Self-Trade Prevention

Before executing a match, the engine checks whether the incoming order's `client_id`
equals the resting order's `client_id` at that price level. If so, the match is skipped
per the configured self-trade policy (default: cancel/skip the resting order's fill
against that specific counterparty and continue matching against the next eligible
order), and the event is logged.

---

## Threading Model

| Thread | Count | Responsibility |
|---|---|---|
| Producer / client threads | N (multiple) | Submit `ClientRequest`s |
| Order Gateway Server | 1 | Validates + FIFO-sequences requests from all producers into one deterministic, timestamp-ordered stream |
| Matching Engine | 1 per symbol | Owns and mutates exactly one symbol's order book; consumes its dedicated request queue; emits `ClientResponse` and `MarketUpdate` events |
| Market Data Publisher | 1 | Consumes `MarketUpdate` events from all symbols, broadcasts deltas |
| Snapshot Synthesizer | 1 | Mirrors the delta stream to maintain a full in-memory book copy; periodically emits complete snapshots for recovery/late joiners |

**Communication:** All cross-thread communication uses `Common/lf_queue.h`, a templated
lock-free SPSC ring buffer. Because each matching thread has its own dedicated inbound
queue (fed only by the single Order Gateway thread) and its own dedicated outbound queues,
every queue is genuinely single-producer/single-consumer — no CAS loops or locks needed
beyond simple atomic read/write indices.

**Determinism:** Determinism is preserved because (a) the Order Gateway assigns a single
global, monotonic sequence/timestamp to every incoming request regardless of which
producer thread it came from, and (b) each symbol's book is mutated by exactly one thread,
so replaying the same sequenced stream always produces the same book state and trade
sequence.

**CPU affinity:** `Common/thread_utils.h` pins each thread to a dedicated core to avoid
context-switch jitter and to keep cache lines warm for that thread's hot data.

---

## Time & Space Complexity

| Operation | Time Complexity | Notes |
|---|---|---|
| Insert new limit order (no match) | O(log P) | P = distinct price levels on that side |
| Match against best price level | O(log P) amortized per level crossed, O(1) per order matched at a level | |
| Cancel order | O(1) average | via order-id → node hash map + intrusive list unlink |
| Modify (qty decrease) | O(1) | in-place, priority retained |
| Modify (qty increase / price change) | O(log P) | re-insertion, priority reset |
| Market/IOC order | O(k log P) | k = number of price levels consumed to fill |
| Full book snapshot | O(N) | N = total resting orders in that symbol's book |

**Space:** O(N) per symbol for resting orders, O(P) for price level nodes, plus fixed
pool capacity reserved at startup (bounded, no dynamic growth on the hot path under
normal operating limits).

---

## Error Handling

The engine validates and gracefully rejects (with a reason code in `ClientResponse`)
rather than throwing/crashing on:

- Duplicate Order IDs
- Invalid prices (≤ 0, non-tick-aligned if applicable)
- Invalid quantities (≤ 0)
- Unknown/unsupported symbols
- Invalid/unsupported order types
- Cancel on an unknown or already-completed order (ignored gracefully, not an error)
- Modify on an unknown order

---

## Design Decisions & Trade-offs

- **`std::map` for price levels vs. a flat vector/array-indexed book:** `std::map` was
  chosen for simplicity and correctness first; for tick-size-bounded instruments, a
  contiguous array indexed by normalized price offset would give O(1) best-price access
  and is called out here as a future optimization (see below).
- **Lock-free SPSC over MPMC queues:** Because each matching thread has exactly one
  producer (the sequencer) and each downstream consumer has exactly one producer, SPSC
  queues are sufficient and are significantly cheaper than general MPMC lock-free queues.
- **Intrusive linked lists over `std::list`:** avoids per-node heap allocation and extra
  indirection; nodes are pool-allocated and manually linked.
- **Networking layer (`tcp_*`, `mcast_*`) kept minimal/optional:** the assignment's core
  evaluation is the matching engine itself; a full FIX/binary protocol gateway was
  deprioritized in favor of a clean in-process `ClientRequest`/`ClientResponse` API that
  the test harness and benchmark driver call directly. This is documented as a conscious
  scope trade-off, not an oversight — the queue-based interfaces make it straightforward
  to bolt a real transport on later.
- **Self-trade prevention policy:** implemented as skip-and-continue rather than
  reject-whole-order, since this is closer to common venue behavior and preserves more
  fillable liquidity; this is configurable behavior worth revisiting per exchange rules.

---

## Build Instructions

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . -j
```

Requires a C++20-capable compiler (GCC 11+/Clang 14+) and CMake 3.20+.

---

## Running Tests

```bash
cd build
ctest --output-on-failure
```

---

## Running Benchmarks

```bash
cd build
./benchmarks/benchmark_main --orders 1000000 --symbols 50
```

Reports: orders processed per second, mean latency, P50, P75, P90, P99, and maximum
latency (nanoseconds).

---

## Sample Input / Output

See `sample_io/sample_orders.txt` for a sample input order stream and
`sample_io/sample_output.txt` for the corresponding fills, rejects, and resulting book
snapshot.

---

## Future Improvements

- Replace `std::map` price levels with a tick-indexed flat array for O(1) best-price access.
- Add a real FIX/binary protocol adapter over the existing `order_server` request API.
- Add configurable self-trade prevention policies (reject-taker, reject-maker, cancel-both).
- Persist execution log to disk with periodic checkpointing tied to snapshots.