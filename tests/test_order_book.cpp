// test_order_book.cpp
//
// Small self-contained test suite for the matching engine - no external test
// framework, just a handful of check() calls and a pass/fail count at the end.
// Each test spins up its own MatchingCore instance so tests don't interfere
// with each other.

#include <iostream>
#include <vector>
#include <string>
#include <thread>
#include <chrono>

#include "../exchange/matcher/matching_core.h"

using namespace Exchange;
using namespace Common;

namespace {
  int pass_count = 0;
  int fail_count = 0;

  void check(bool condition, const std::string &description) {
    if (condition) {
      ++pass_count;
    } else {
      ++fail_count;
      std::cerr << "FAIL: " << description << "\n";
    }
  }

  // Wraps a MatchingCore + its queues so each test can just call send() and settle().
  struct Harness {
    OrderRequestQueue req_q{4096};
    OrderResponseQueue resp_q{4096};
    MarketUpdateQueue upd_q{4096};
    MatchingCore engine;
    std::vector<EngineOrderResponse> responses;

    explicit Harness(std::vector<Symbol> symbols)
        : engine(std::move(symbols), &req_q, &resp_q, &upd_q) {
      engine.start();
    }

    ~Harness() {
      engine.stop();
      std::this_thread::sleep_for(std::chrono::milliseconds(30));
    }

    void send(EngineOrderRequest r) {
      *(req_q.getNextToWriteTo()) = r;
      req_q.updateWriteIndex();
    }

    // gives the engine a moment to process, then pulls whatever responses arrived
    void settle(int wait_ms = 100) {
      std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));
      for (auto r = resp_q.getNextToRead(); resp_q.size() && r; r = resp_q.getNextToRead()) {
        responses.push_back(*r);
        resp_q.updateReadIndex();
      }
    }

    bool any(std::function<bool(const EngineOrderResponse &)> pred) const {
      for (const auto &r : responses) {
        if (pred(r)) return true;
      }
      return false;
    }
  };
}

static void testBasicMatch() {
  Harness h({"TST"});
  h.send({OrderType::LIMIT, 1, "TST", 1, Side::BUY, 100, 10});
  h.send({OrderType::LIMIT, 2, "TST", 2, Side::SELL, 100, 4});
  h.settle();

  check(h.any([](auto &r) { return r.client_order_id_ == 1 && r.status_ == ResponseStatus::FILLED && r.exec_qty_ == 4; }),
        "resting buy order gets filled for 4");
  check(h.any([](auto &r) { return r.client_order_id_ == 2 && r.status_ == ResponseStatus::FILLED && r.exec_qty_ == 4; }),
        "incoming sell order gets filled for 4");
}

static void testPartialFillLeavesRemainder() {
  Harness h({"TST"});
  h.send({OrderType::LIMIT, 1, "TST", 1, Side::BUY, 100, 10});
  h.send({OrderType::LIMIT, 2, "TST", 2, Side::SELL, 100, 4});
  h.settle();

  check(h.any([](auto &r) { return r.client_order_id_ == 1 && r.status_ == ResponseStatus::FILLED && r.leaves_qty_ == 6; }),
        "partially filled order has 6 remaining");
}

static void testPriceTimePriority() {
  Harness h({"TST"});
  h.send({OrderType::LIMIT, 1, "TST", 1, Side::BUY, 100, 5}); // worse price
  h.send({OrderType::LIMIT, 2, "TST", 2, Side::BUY, 101, 5}); // better price, should match first
  h.send({OrderType::LIMIT, 3, "TST", 3, Side::SELL, 100, 5});
  h.settle();

  check(h.any([](auto &r) { return r.client_order_id_ == 2 && r.status_ == ResponseStatus::FILLED; }),
        "better priced order matches first");
  check(!h.any([](auto &r) { return r.client_order_id_ == 1 && r.status_ == ResponseStatus::FILLED; }),
        "worse priced order is left untouched while a better price exists");
}

static void testTimePriorityAtSamePrice() {
  Harness h({"TST"});
  h.send({OrderType::LIMIT, 1, "TST", 1, Side::BUY, 100, 5}); // older
  h.send({OrderType::LIMIT, 2, "TST", 2, Side::BUY, 100, 5}); // newer, same price
  h.send({OrderType::LIMIT, 3, "TST", 3, Side::SELL, 100, 5});
  h.settle();

  check(h.any([](auto &r) { return r.client_order_id_ == 1 && r.status_ == ResponseStatus::FILLED; }),
        "older order at the same price matches first (FIFO)");
  check(!h.any([](auto &r) { return r.client_order_id_ == 2 && r.status_ == ResponseStatus::FILLED; }),
        "newer order at the same price is untouched");
}

static void testCancel() {
  Harness h({"TST"});
  h.send({OrderType::LIMIT, 1, "TST", 1, Side::BUY, 100, 5});
  h.send({OrderType::CANCEL, 1, "TST", 1, Side::INVALID, 0, 0});
  h.settle();

  check(h.any([](auto &r) { return r.client_order_id_ == 1 && r.status_ == ResponseStatus::CANCELED; }),
        "resting order is cancelled");
}

static void testCancelUnknownOrderIsGraceful() {
  Harness h({"TST"});
  h.send({OrderType::CANCEL, 1, "TST", 999, Side::INVALID, 0, 0});
  h.settle();

  check(h.any([](auto &r) { return r.client_order_id_ == 999 && r.status_ == ResponseStatus::CANCEL_REJECTED; }),
        "cancelling an order that never existed is rejected gracefully, not a crash");
}

static void testModifyQtyDecreaseKeepsPriority() {
  Harness h({"TST"});
  h.send({OrderType::LIMIT, 1, "TST", 1, Side::BUY, 100, 10}); // older
  h.send({OrderType::LIMIT, 2, "TST", 2, Side::BUY, 100, 10}); // newer
  h.send({OrderType::MODIFY, 1, "TST", 1, Side::INVALID, 100, 5}); // shrink the older one
  h.send({OrderType::LIMIT, 3, "TST", 3, Side::SELL, 100, 5});
  h.settle();

  check(h.any([](auto &r) { return r.client_order_id_ == 1 && r.status_ == ResponseStatus::FILLED; }),
        "shrinking an order's quantity keeps its place in the queue");
}

static void testModifyQtyIncreaseResetsPriority() {
  Harness h({"TST"});
  h.send({OrderType::LIMIT, 1, "TST", 1, Side::BUY, 100, 10}); // older
  h.send({OrderType::LIMIT, 2, "TST", 2, Side::BUY, 100, 10}); // newer
  h.send({OrderType::MODIFY, 1, "TST", 1, Side::INVALID, 100, 15}); // grow the older one
  h.send({OrderType::LIMIT, 3, "TST", 3, Side::SELL, 100, 10});
  h.settle();

  check(h.any([](auto &r) { return r.client_order_id_ == 2 && r.status_ == ResponseStatus::FILLED; }),
        "growing an order's quantity sends it to the back of the queue");
}

static void testModifyPriceChangeCanMatchImmediately() {
  Harness h({"TST"});
  h.send({OrderType::LIMIT, 1, "TST", 1, Side::SELL, 110, 5}); // resting ask
  h.send({OrderType::LIMIT, 2, "TST", 2, Side::BUY, 100, 5});  // doesn't cross yet
  h.send({OrderType::MODIFY, 2, "TST", 2, Side::INVALID, 110, 5}); // reprice to cross
  h.settle();

  check(h.any([](auto &r) { return r.client_order_id_ == 2 && r.status_ == ResponseStatus::FILLED; }),
        "repricing an order to cross the book matches it immediately");
}

static void testSelfTradePrevention() {
  Harness h({"TST"});
  h.send({OrderType::LIMIT, 1, "TST", 1, Side::BUY, 100, 10});
  h.send({OrderType::LIMIT, 1, "TST", 2, Side::SELL, 100, 10}); // same client
  h.settle();

  check(!h.any([](auto &r) { return r.status_ == ResponseStatus::FILLED; }),
        "an order never matches against its own resting order");
}

static void testMarketOrderCancelsUnfilledRemainder() {
  Harness h({"TST"});
  h.send({OrderType::LIMIT, 1, "TST", 1, Side::BUY, 100, 3}); // only 3 units on offer
  h.send({OrderType::MARKET, 2, "TST", 2, Side::SELL, 0, 10});
  h.settle();

  check(h.any([](auto &r) { return r.client_order_id_ == 2 && r.status_ == ResponseStatus::CANCELED && r.leaves_qty_ == 7; }),
        "market order's unfilled 7 units are cancelled once liquidity runs out");
}

static void testIOCNeverRests() {
  Harness h({"TST"});
  h.send({OrderType::IOC, 1, "TST", 1, Side::BUY, 100, 10}); // nothing to match
  h.settle();

  check(h.any([](auto &r) { return r.client_order_id_ == 1 && r.status_ == ResponseStatus::CANCELED; }),
        "an IOC order with no available liquidity is cancelled rather than resting");
}

static void testUnknownSymbolIsRejected() {
  Harness h({"TST"});
  h.send({OrderType::LIMIT, 1, "NOPE", 1, Side::BUY, 100, 10});
  h.settle();

  check(h.any([](auto &r) { return r.status_ == ResponseStatus::REJECTED; }),
        "an order for a symbol the exchange doesn't list is rejected");
}

static void testZeroQuantityIsRejected() {
  Harness h({"TST"});
  h.send({OrderType::LIMIT, 1, "TST", 1, Side::BUY, 100, 0});
  h.settle();

  check(h.any([](auto &r) { return r.status_ == ResponseStatus::REJECTED; }),
        "a zero quantity order is rejected");
}

static void testInvalidPriceIsRejected() {
  Harness h({"TST"});
  h.send({OrderType::LIMIT, 1, "TST", 1, Side::BUY, -5, 10});
  h.settle();

  check(h.any([](auto &r) { return r.status_ == ResponseStatus::REJECTED; }),
        "a negative price is rejected");
}

static void testDuplicateOrderIdIsRejected() {
  Harness h({"TST"});
  h.send({OrderType::LIMIT, 1, "TST", 1, Side::BUY, 100, 5});
  h.settle(50);
  h.send({OrderType::LIMIT, 2, "TST", 1, Side::BUY, 101, 5}); // reused id, different client
  h.settle();

  check(h.any([](auto &r) { return r.client_id_ == 2 && r.status_ == ResponseStatus::REJECTED; }),
        "reusing an order id that's already active is rejected");
}

static void testMultipleSymbolsStayIndependent() {
  Harness h({"AAA", "BBB"});
  h.send({OrderType::LIMIT, 1, "AAA", 1, Side::BUY, 100, 5});
  h.send({OrderType::LIMIT, 2, "BBB", 2, Side::SELL, 100, 5}); // shouldn't touch AAA's book
  h.settle();

  check(!h.any([](auto &r) { return r.client_order_id_ == 2 && r.status_ == ResponseStatus::FILLED; }),
        "an order on one symbol doesn't match against a different symbol's book");
}

int main() {
  testBasicMatch();
  testPartialFillLeavesRemainder();
  testPriceTimePriority();
  testTimePriorityAtSamePrice();
  testCancel();
  testCancelUnknownOrderIsGraceful();
  testModifyQtyDecreaseKeepsPriority();
  testModifyQtyIncreaseResetsPriority();
  testModifyPriceChangeCanMatchImmediately();
  testSelfTradePrevention();
  testMarketOrderCancelsUnfilledRemainder();
  testIOCNeverRests();
  testUnknownSymbolIsRejected();
  testZeroQuantityIsRejected();
  testInvalidPriceIsRejected();
  testDuplicateOrderIdIsRejected();
  testMultipleSymbolsStayIndependent();

  std::cout << "\n" << pass_count << " passed, " << fail_count << " failed\n";
  return fail_count == 0 ? 0 : 1;
}