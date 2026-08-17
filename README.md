# Trading Matching Engine

A multi-symbol order matching engine written in C++20. It works like a simplified stock
exchange - clients send orders, the engine matches buyers with sellers using price-time
priority, and it publishes trades and book updates as they happen.

This was built as a technical assignment, so the goal was to keep the core matching logic
correct and fast, while keeping the rest of the system (networking, persistence, etc.)
simple and out of scope.

## How it works

There are three main pieces, and they talk to each other only through queues - no shared
state, no locks on the hot path.

```
client threads
      |
      v
Order Gateway  --------->  Matching Engine  --------->  Market Data Feed
(order_server)             (matcher)                    (market_data)
```

**Order Gateway** takes orders from however many threads want to submit them, timestamps
each one the moment it arrives, and sorts them by receive time before handing them off.
This is what keeps things deterministic even though multiple threads can be submitting
at once.

**Matching Engine** owns one order book per symbol, and each symbol gets its own thread,
its own inbound request queue, and its own outbound response/update queues. The gateway
splits its time-sorted stream out by symbol (`MatchingCore::routeRequest`), so each
symbol's thread only ever sees requests for that symbol, in the order they were
received. No two symbols ever touch each other's book or share a thread.

**Market Data Feed** listens to every symbol's book-update queue (a new order resting, a
trade, a cancel, etc.) and pushes that out to anyone subscribed. It also keeps a
mirrored copy of the book on the side so it can hand out full snapshots without
bothering the matching threads.

Every queue in the system is a lock-free single-producer/single-consumer ring buffer
(`Common/lock_free_queue.h`). Because each symbol has its own dedicated request queue
(written only by the gateway) and its own dedicated response/update queues (written only
by that symbol's own thread), every queue genuinely has exactly one writer - no locks or
CAS retry loops needed, just plain atomic read/write indices.

## Project layout

```
Common/
  types.h              basic types - OrderId, Price, Qty, Side, Symbol, Nanos, etc.
  lock_free_queue.h     the SPSC ring buffer used between every stage
  memory_pool.h         fixed-size pool allocator, so the book doesn't call new/delete
  threads.h             thread creation + pinning to a CPU core
  time.h                nanosecond timestamps
  logging.h             a simple async logger
  macros.h              branch-hint / assert helpers

exchange/
  order_server/
    order_request.h       what an incoming order looks like
    order_response.h      accept / reject / fill / cancel responses
    request_sequencer.h    sorts requests by receive time, then routes each one onward
    order_server.h          the gateway itself - ties the above together

  matcher/
    book_order.h           one resting order (also a node in its price level's list)
    matching_core.h         owns every symbol's book, queues, and matching thread
    order_book.h/.cpp       the actual per-symbol order book and matching logic
    market_update.h         book-update event schema (new order, trade, cancel...)

  market_data/
    market_data_feed.h      fans out book updates to subscribers
    book_snapshot_service.h keeps a full mirrored book for snapshot/recovery

  exchange_main.cpp     wires everything together, reads a CSV of orders, runs it

tests/
  test_order_book.cpp   unit tests for matching, fills, priority, edge cases

benchmarks/
  benchmark_main.cpp    throughput + latency numbers

sample_io/
  sample_orders.txt     example input
  sample_output.txt     what the engine produces for it
```

## Order book data structure

Each symbol gets its own `SymbolOrderBook`. Bids and asks are each stored in a
`std::map<Price, PriceLevel*>` - bids sorted highest price first, asks sorted lowest
price first, so the best price on either side is always at `.begin()`.

Every price level holds a doubly-linked list of orders in the order they arrived (FIFO),
so the earliest order at a price level always gets matched first. Cancelling or filling
an order just unlinks its node directly - no scanning through the list.

There's also a plain `unordered_map<OrderId, BookOrder*>` so cancel/modify can find the
right order straight away instead of searching through price levels.

Orders and price levels come out of a fixed-size memory pool (`Common/memory_pool.h`)
instead of `new`/`delete`, so the matching path doesn't allocate on the heap.

Rough complexity:

| operation | cost |
|---|---|
| new limit order, no match | O(log P) - P = number of price levels on that side |
| order matches and fills | O(1) per order matched, plus O(log P) if a price level empties out |
| cancel | O(1) - direct lookup + unlink |
| modify, qty decrease | O(1), keeps its place in line |
| modify, qty increase or price change | O(log P) - loses priority, gets re-inserted |
| market / IOC order | O(k) roughly, k = number of levels it has to sweep through |

Space is O(N) resting orders + O(P) price levels per symbol, on top of the pool capacity
reserved up front.

## Order types

- **Limit** - matches whatever it can right away, remainder rests on the book.
- **Market** - eats through the book at whatever price is available, cancels anything
  left unfilled instead of resting it.
- **IOC** - same as limit but never rests - unfilled part is cancelled immediately.
- **Cancel** - removes a resting order. Cancelling something that's already gone (filled
  or already cancelled) is just ignored, not treated as an error.
- **Modify** - can change price and/or quantity on a resting order:
  - changing price resets its place in line (new price level, new timestamp)
  - decreasing quantity keeps its place in line
  - increasing quantity resets its place in line, even at the same price

## Self-trade prevention

Before matching an incoming order against a resting one, the engine checks whether they
belong to the same client. If so, that resting order is skipped and matching continues
against the next one in line, instead of trading against yourself.

## Threading

- Any number of producer threads can call `submit()` on the gateway at once.
- The gateway runs one background thread that periodically drains whatever's come in,
  sorts it by receive time, and routes each request to the queue belonging to that
  request's symbol.
- **Each symbol gets its own dedicated matching thread.** `MatchingCore` starts one
  thread per symbol on `start()`, and that thread is the only thing that ever reads
  from that symbol's request queue or writes to that symbol's response/update queues.
  One symbol being busy (or slow) never blocks matching for any other symbol.
- The market data feed runs its own thread, draining every symbol's update queue,
  fanning updates out to subscribers and to the snapshot mirror.
- Threads get pinned to a CPU core on start (`Common/threads.h`) to cut down on
  context-switch noise.

Determinism comes from two things: the gateway assigns every request a single receive
timestamp before it's routed anywhere, regardless of which thread submitted it or which
symbol it's for; and each symbol's book is only ever touched by that symbol's own
thread. So replaying the same input always produces the same sequence of trades for
every symbol.

## A couple of trade-offs, worth being upfront about

- **The gateway's inbox uses a small mutex, not a lock-free MPSC queue.** Submitting
  orders isn't on the matching hot path, so a short-lived lock there is a fine trade for
  not having to hand-roll a general multi-producer lock-free queue.
- **`std::map` for price levels instead of a flat array.** Simpler and correct, at the
  cost of O(log P) instead of O(1) for level lookups. For instruments with a small,
  bounded tick range, an array indexed by price offset would be faster - left as a next
  step rather than something needed for correctness here.
- **No real network layer.** Orders go in and responses come out through an in-process
  API (what the tests and benchmark use directly) rather than over a socket. The focus
  here was the matching logic itself; wiring a TCP/FIX layer on top of the existing
  request/response queues later wouldn't need touching the matching code at all.
- **A request for an unknown symbol has nowhere to be routed to**, so it's rejected
  immediately on a small dedicated queue rather than a per-symbol one (there's no
  per-symbol queue to put it on). This keeps the "one queue, one writer" rule intact
  everywhere else.

## Building

Needs a C++20 compiler (GCC 11+ or Clang 14+) and CMake 3.20+.

```bash
mkdir build && cd build
cmake ..
make -j
```

This builds three binaries: `exchange_main`, `test_order_book`, and `benchmark_main`.

## Running the tests

```bash
cd build
./test_order_book
```

or, via ctest:

```bash
ctest --output-on-failure
```

## Running the sample

```bash
cd build
./exchange_main ../sample_io/sample_orders.txt output.txt
```

Reads the CSV of orders in `sample_io/sample_orders.txt`, runs them through the engine,
and writes out the responses and book updates. `sample_io/sample_output.txt` shows what
that looks like. (Since different symbols now run on different threads, the exact
interleaving of two symbols' output lines can vary slightly run to run - the matching
result for each individual symbol is still deterministic.)

## Running the benchmark

```bash
cd build
./benchmark_main            # defaults to 200,000 orders
./benchmark_main 1000000    # or pass a count
```

Prints throughput (orders/sec) and latency percentiles (mean, p50, p75, p90, p99, max),
measured from when an order is submitted to when its first response comes back.

