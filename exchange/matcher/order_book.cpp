#include "order_book.h"
#include "matching_core.h"

namespace Exchange {

  SymbolOrderBook::SymbolOrderBook(Common::Symbol symbol, Common::Logger *logger, MatchingCore *engine)
      : symbol_(std::move(symbol)), engine_(engine), logger_(logger),
        order_pool_(MAX_RESTING_ORDERS), level_pool_(MAX_PRICE_LEVELS) {
  }

  SymbolOrderBook::~SymbolOrderBook() = default;

  // ------------------------------------------------------------------------
  // Small helpers to forward outgoing events to the parent MatchingCore.
  // ------------------------------------------------------------------------

  auto SymbolOrderBook::sendResponse(const EngineOrderResponse &resp) noexcept -> void {
    LOG_INFO(*logger_, "response %s", resp.toString().c_str());
    engine_->publishResponse(resp);
  }

  auto SymbolOrderBook::sendBookUpdate(MarketUpdateType type, Common::OrderId order_id, Common::Side side,
                                        Common::Price price, Common::Qty qty) noexcept -> void {
    BookUpdate update{type, order_id, symbol_, side, price, qty};
    LOG_INFO(*logger_, "book_update %s", update.toString().c_str());
    engine_->publishBookUpdate(update);
  }

  // ------------------------------------------------------------------------
  // Book maintenance: insert / remove resting orders and their price levels.
  // ------------------------------------------------------------------------

  auto SymbolOrderBook::restOrder(Common::ClientId client_id, Common::OrderId order_id, Common::Side side,
                                   Common::Price price, Common::Qty qty, Common::Nanos priority_ts) noexcept -> void {
    auto *order = order_pool_.allocate(order_id, client_id, side, price, qty, priority_ts);
    PriceLevel *level = nullptr;

    if (side == Common::Side::BUY) {
      auto it = bids_.find(price);
      if (it == bids_.end()) {
        level = level_pool_.allocate(price);
        bids_[price] = level;
      } else {
        level = it->second;
      }
    } else {
      auto it = asks_.find(price);
      if (it == asks_.end()) {
        level = level_pool_.allocate(price);
        asks_[price] = level;
      } else {
        level = it->second;
      }
    }

    // append at tail -> newest / lowest time-priority within this price level.
    order->prev_ = level->tail_;
    order->next_ = nullptr;
    if (level->tail_) {
      level->tail_->next_ = order;
    } else {
      level->head_ = order;
    }
    level->tail_ = order;
    ++level->order_count_;

    order_index_[order_id] = order;
  }

  auto SymbolOrderBook::removeOrder(BookOrder *order) noexcept -> void {
    const auto side = order->side_;
    const auto price = order->price_;

    if (side == Common::Side::BUY) {
      auto it = bids_.find(price);
      if (it == bids_.end()) return; // defensive - should not happen if order_index_ is consistent.
      auto *level = it->second;

      if (order->prev_) order->prev_->next_ = order->next_; else level->head_ = order->next_;
      if (order->next_) order->next_->prev_ = order->prev_; else level->tail_ = order->prev_;
      --level->order_count_;

      if (level->order_count_ == 0) {
        bids_.erase(it);
        level_pool_.deallocate(level);
      }
    } else {
      auto it = asks_.find(price);
      if (it == asks_.end()) return;
      auto *level = it->second;

      if (order->prev_) order->prev_->next_ = order->next_; else level->head_ = order->next_;
      if (order->next_) order->next_->prev_ = order->prev_; else level->tail_ = order->prev_;
      --level->order_count_;

      if (level->order_count_ == 0) {
        asks_.erase(it);
        level_pool_.deallocate(level);
      }
    }

    order_index_.erase(order->order_id_);
    order_pool_.deallocate(order);
  }

  // ------------------------------------------------------------------------
  // Matching.
  // ------------------------------------------------------------------------

  auto SymbolOrderBook::matchIncoming(Common::ClientId client_id, Common::OrderId order_id, Common::Side side,
                                       Common::Price limit_price, bool has_limit_price, Common::Qty &leaves_qty) noexcept -> void {
    if (side == Common::Side::BUY) {
      auto it = asks_.begin();
      while (leaves_qty > 0 && it != asks_.end()) {
        const auto level_price = it->first;
        if (has_limit_price && limit_price < level_price) break; // best ask too expensive.

        auto *level = it->second;
        auto *node = level->head_;
        while (node != nullptr) {
          auto *next_node = node->next_;
          if (node->client_id_ != client_id && leaves_qty > 0) {
            const auto fill_qty = std::min(leaves_qty, node->qty_);
            leaves_qty -= fill_qty;
            node->qty_ -= fill_qty;

            sendResponse({ResponseStatus::FILLED, client_id, symbol_, order_id, order_id, side, level_price, fill_qty, leaves_qty});
            sendResponse({ResponseStatus::FILLED, node->client_id_, symbol_, node->order_id_, node->order_id_, node->side_, level_price, fill_qty, node->qty_});
            sendBookUpdate(MarketUpdateType::TRADE, node->order_id_, node->side_, level_price, fill_qty);

            if (node->qty_ == 0) {
              removeOrder(node); // fully filled resting order leaves the book.
            } else {
              sendBookUpdate(MarketUpdateType::MODIFY, node->order_id_, node->side_, level_price, node->qty_);
            }
          }
          node = next_node;
        }
        it = asks_.upper_bound(level_price); // safe even if 'level' was erased above.
      }
    } else {
      auto it = bids_.begin();
      while (leaves_qty > 0 && it != bids_.end()) {
        const auto level_price = it->first;
        if (has_limit_price && limit_price > level_price) break; // best bid too cheap.

        auto *level = it->second;
        auto *node = level->head_;
        while (node != nullptr) {
          auto *next_node = node->next_;
          if (node->client_id_ != client_id && leaves_qty > 0) {
            const auto fill_qty = std::min(leaves_qty, node->qty_);
            leaves_qty -= fill_qty;
            node->qty_ -= fill_qty;

            sendResponse({ResponseStatus::FILLED, client_id, symbol_, order_id, order_id, side, level_price, fill_qty, leaves_qty});
            sendResponse({ResponseStatus::FILLED, node->client_id_, symbol_, node->order_id_, node->order_id_, node->side_, level_price, fill_qty, node->qty_});
            sendBookUpdate(MarketUpdateType::TRADE, node->order_id_, node->side_, level_price, fill_qty);

            if (node->qty_ == 0) {
              removeOrder(node);
            } else {
              sendBookUpdate(MarketUpdateType::MODIFY, node->order_id_, node->side_, level_price, node->qty_);
            }
          }
          node = next_node;
        }
        it = bids_.upper_bound(level_price);
      }
    }
  }

  // ------------------------------------------------------------------------
  // Public order-entry API.
  // ------------------------------------------------------------------------

  auto SymbolOrderBook::addLimitOrder(Common::ClientId client_id, Common::OrderId order_id, Common::Side side,
                                       Common::Price price, Common::Qty qty) noexcept -> void {
    if (order_index_.count(order_id)) {
      sendResponse({ResponseStatus::REJECTED, client_id, symbol_, order_id, Common::Invalid_OrderId, side, price, 0, qty});
      return;
    }

    sendResponse({ResponseStatus::ACCEPTED, client_id, symbol_, order_id, order_id, side, price, 0, qty});

    auto leaves = qty;
    matchIncoming(client_id, order_id, side, price, /*has_limit_price=*/true, leaves);

    if (leaves > 0) {
      restOrder(client_id, order_id, side, price, leaves, Common::getCurrentNanos());
      sendBookUpdate(MarketUpdateType::ADD, order_id, side, price, leaves);
    }
  }

  auto SymbolOrderBook::addMarketOrder(Common::ClientId client_id, Common::OrderId order_id, Common::Side side,
                                        Common::Qty qty) noexcept -> void {
    if (order_index_.count(order_id)) {
      sendResponse({ResponseStatus::REJECTED, client_id, symbol_, order_id, Common::Invalid_OrderId, side, Common::Invalid_Price, 0, qty});
      return;
    }

    sendResponse({ResponseStatus::ACCEPTED, client_id, symbol_, order_id, order_id, side, Common::Invalid_Price, 0, qty});

    auto leaves = qty;
    matchIncoming(client_id, order_id, side, Common::Invalid_Price, /*has_limit_price=*/false, leaves);

    if (leaves > 0) { // liquidity exhausted - cancel the remainder, never rests.
      sendResponse({ResponseStatus::CANCELED, client_id, symbol_, order_id, order_id, side, Common::Invalid_Price, qty - leaves, leaves});
    }
  }

  auto SymbolOrderBook::addIOCOrder(Common::ClientId client_id, Common::OrderId order_id, Common::Side side,
                                     Common::Price price, Common::Qty qty) noexcept -> void {
    if (order_index_.count(order_id)) {
      sendResponse({ResponseStatus::REJECTED, client_id, symbol_, order_id, Common::Invalid_OrderId, side, price, 0, qty});
      return;
    }

    sendResponse({ResponseStatus::ACCEPTED, client_id, symbol_, order_id, order_id, side, price, 0, qty});

    auto leaves = qty;
    matchIncoming(client_id, order_id, side, price, /*has_limit_price=*/true, leaves);

    if (leaves > 0) { // IOC never rests - cancel whatever didn't fill immediately.
      sendResponse({ResponseStatus::CANCELED, client_id, symbol_, order_id, order_id, side, price, qty - leaves, leaves});
    }
  }

  auto SymbolOrderBook::cancelOrder(Common::ClientId client_id, Common::OrderId order_id) noexcept -> void {
    auto it = order_index_.find(order_id);
    if (it == order_index_.end() || it->second->client_id_ != client_id) {
      // Unknown or already-completed order: ignored gracefully, not a crash.
      sendResponse({ResponseStatus::CANCEL_REJECTED, client_id, symbol_, order_id, Common::Invalid_OrderId,
                     Common::Side::INVALID, Common::Invalid_Price, Common::Invalid_Qty, Common::Invalid_Qty});
      return;
    }

    auto *order = it->second;
    const auto side = order->side_;
    const auto price = order->price_;
    const auto qty = order->qty_;

    removeOrder(order);

    sendResponse({ResponseStatus::CANCELED, client_id, symbol_, order_id, order_id, side, price, 0, 0});
    sendBookUpdate(MarketUpdateType::CANCEL, order_id, side, price, qty);
  }

  auto SymbolOrderBook::modifyOrder(Common::ClientId client_id, Common::OrderId order_id,
                                     Common::Price new_price, Common::Qty new_qty) noexcept -> void {
    auto it = order_index_.find(order_id);
    if (it == order_index_.end() || it->second->client_id_ != client_id) {
      sendResponse({ResponseStatus::MODIFY_REJECTED, client_id, symbol_, order_id, Common::Invalid_OrderId,
                     Common::Side::INVALID, Common::Invalid_Price, Common::Invalid_Qty, Common::Invalid_Qty});
      return;
    }
    if (new_qty == 0 || new_qty == Common::Invalid_Qty || new_price <= 0) {
      sendResponse({ResponseStatus::MODIFY_REJECTED, client_id, symbol_, order_id, Common::Invalid_OrderId,
                     Common::Side::INVALID, Common::Invalid_Price, Common::Invalid_Qty, Common::Invalid_Qty});
      return;
    }

    auto *order = it->second;
    const auto side = order->side_;
    const auto old_price = order->price_;
    const auto old_qty = order->qty_;

    if (new_price != old_price) {
      // Price change resets time priority, and may newly cross the book, so treat it
      // like a fresh limit order at the new price.
      removeOrder(order);
      sendResponse({ResponseStatus::MODIFIED, client_id, symbol_, order_id, order_id, side, new_price, 0, new_qty});

      auto leaves = new_qty;
      matchIncoming(client_id, order_id, side, new_price, /*has_limit_price=*/true, leaves);
      if (leaves > 0) {
        restOrder(client_id, order_id, side, new_price, leaves, Common::getCurrentNanos());
        sendBookUpdate(MarketUpdateType::ADD, order_id, side, new_price, leaves);
      }
    } else if (new_qty < old_qty) {
      // Quantity decrease at the same price retains time priority - update in place.
      order->qty_ = new_qty;
      sendResponse({ResponseStatus::MODIFIED, client_id, symbol_, order_id, order_id, side, old_price, 0, new_qty});
      sendBookUpdate(MarketUpdateType::MODIFY, order_id, side, old_price, new_qty);
    } else if (new_qty > old_qty) {
      // Quantity increase resets time priority - remove and re-insert at the back of
      // this price level's FIFO queue.
      removeOrder(order);
      restOrder(client_id, order_id, side, old_price, new_qty, Common::getCurrentNanos());
      sendResponse({ResponseStatus::MODIFIED, client_id, symbol_, order_id, order_id, side, old_price, 0, new_qty});
      sendBookUpdate(MarketUpdateType::MODIFY, order_id, side, old_price, new_qty);
    } else {
      // No actual change.
      sendResponse({ResponseStatus::MODIFIED, client_id, symbol_, order_id, order_id, side, old_price, 0, old_qty});
    }
  }
}