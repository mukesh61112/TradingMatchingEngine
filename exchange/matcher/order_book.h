#pragma once

#include <map>
#include <unordered_map>
#include <functional>

#include "Common/types.h"
#include "Common/memory_pool.h"
#include "Common/logging.h"

#include "exchange/order_server/order_response.h"
#include "market_update.h"
#include "book_order.h"

namespace Exchange {
  /// Maximum number of simultaneously resting orders / distinct price levels this book
  /// will pool memory for. Sized generously; exceeding it is treated as a fatal
  /// configuration error rather than something recoverable at runtime.
  constexpr size_t MAX_RESTING_ORDERS = 128 * 1024;
  constexpr size_t MAX_PRICE_LEVELS   = 8 * 1024;

  /// One symbol's independent limit order book: separate bid/ask sides, each kept in
  /// price-time priority. Owns no thread itself - it is driven synchronously by the one
  /// matching thread MatchingCore dedicates to this symbol, and writes its responses and
  /// book updates directly onto that same symbol's own output queues.
  class SymbolOrderBook final {
  public:
    SymbolOrderBook(Common::Symbol symbol, Common::Logger *logger,
                     OrderResponseQueue *response_queue, MarketUpdateQueue *update_queue);

    ~SymbolOrderBook();

    /// Limit order: matches what it can immediately, rests any remainder on the book.
    auto addLimitOrder(Common::ClientId client_id, Common::OrderId order_id, Common::Side side,
                        Common::Price price, Common::Qty qty) noexcept -> void;

    /// Market order: consumes available liquidity regardless of price; any unfilled
    /// remainder is cancelled, never rests.
    auto addMarketOrder(Common::ClientId client_id, Common::OrderId order_id, Common::Side side,
                         Common::Qty qty) noexcept -> void;

    /// IOC order: matches like a limit order at its price; any unfilled remainder is
    /// cancelled, never rests.
    auto addIOCOrder(Common::ClientId client_id, Common::OrderId order_id, Common::Side side,
                      Common::Price price, Common::Qty qty) noexcept -> void;

    /// Cancel a resting order. Cancelling an unknown/already-completed order is ignored
    /// gracefully - a CANCEL_REJECTED response is sent, nothing crashes.
    auto cancelOrder(Common::ClientId client_id, Common::OrderId order_id) noexcept -> void;

    /// Modify a resting order's price and/or quantity:
    ///  - price change            -> re-priced, time priority reset
    ///  - quantity decrease       -> time priority retained (in place)
    ///  - quantity increase       -> time priority reset (moves to back of its level)
    auto modifyOrder(Common::ClientId client_id, Common::OrderId order_id,
                      Common::Price new_price, Common::Qty new_qty) noexcept -> void;

    /// Deleted default, copy & move constructors and assignment-operators.
    SymbolOrderBook() = delete;
    SymbolOrderBook(const SymbolOrderBook &) = delete;
    SymbolOrderBook(const SymbolOrderBook &&) = delete;
    SymbolOrderBook &operator=(const SymbolOrderBook &) = delete;
    SymbolOrderBook &operator=(const SymbolOrderBook &&) = delete;

  private:
    Common::Symbol symbol_;
    OrderResponseQueue *response_queue_ = nullptr;
    MarketUpdateQueue *update_queue_ = nullptr;
    Common::Logger *logger_ = nullptr;

    /// Bids ordered highest-price-first, asks ordered lowest-price-first - so
    /// `.begin()` on either side is always the current best (top-of-book) price level.
    std::map<Common::Price, PriceLevel *, std::greater<Common::Price>> bids_;
    std::map<Common::Price, PriceLevel *, std::less<Common::Price>>    asks_;

    /// O(1) average lookup from OrderId directly to its node, for cancel/modify.
    std::unordered_map<Common::OrderId, BookOrder *> order_index_;

    Common::MemPool<BookOrder>   order_pool_;
    Common::MemPool<PriceLevel>  level_pool_;

  private:
    auto sendResponse(const EngineOrderResponse &resp) noexcept -> void;
    auto sendBookUpdate(MarketUpdateType type, Common::OrderId order_id, Common::Side side,
                         Common::Price price, Common::Qty qty) noexcept -> void;

    /// Attempts to match an incoming order against the opposite side of the book.
    /// Decrements leaves_qty as fills occur; skips (but does not remove) resting orders
    /// belonging to the same client (self-trade prevention). When has_limit_price is
    /// false the incoming order is a Market order and will cross at any price.
    auto matchIncoming(Common::ClientId client_id, Common::OrderId order_id, Common::Side side,
                        Common::Price limit_price, bool has_limit_price, Common::Qty &leaves_qty) noexcept -> void;

    /// Insert a new resting order at the back (newest / lowest priority) of its price level.
    auto restOrder(Common::ClientId client_id, Common::OrderId order_id, Common::Side side,
                    Common::Price price, Common::Qty qty, Common::Nanos priority_ts) noexcept -> void;

    /// Unlink and fully remove a resting order from the book (used by cancel, full fills,
    /// and modify-driven re-insertion). Cleans up an emptied price level automatically.
    auto removeOrder(BookOrder *order) noexcept -> void;
  };

  /// Convenience: maps every tradeable symbol to its own independent order book.
  using OrderBookMap = std::unordered_map<Common::Symbol, std::unique_ptr<SymbolOrderBook>>;
}