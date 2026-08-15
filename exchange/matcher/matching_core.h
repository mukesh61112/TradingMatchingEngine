#pragma once

#include <thread>
#include <atomic>
#include <vector>
#include <memory>

#include "Common/macros.h"
#include "Common/threads.h"
#include "Common/logging.h"

#include "exchange/order_server/order_request.h"
#include "exchange/order_server/order_response.h"
#include "exchange/market_data/market_update.h"
#include order_book.h"

namespace Exchange {
  /// Owns one SymbolOrderBook per tradeable symbol and runs the single matching thread
  /// that consumes sequenced requests from the gateway and dispatches each to the right
  /// book. Serializing all symbols' requests through one thread keeps matching
  /// deterministic; splitting to one thread per symbol (see README) is a drop-in
  /// extension of this same dispatch loop, just partitioned by symbol.
  class MatchingCore final {
  public:
    MatchingCore(std::vector<Common::Symbol> symbols, OrderRequestQueue *incoming_requests,
                 OrderResponseQueue *outgoing_responses, MarketUpdateQueue *outgoing_updates)
        : incoming_requests_(incoming_requests), outgoing_responses_(outgoing_responses),
          outgoing_updates_(outgoing_updates), logger_("exchange_matching_core.log") {
      for (auto &symbol : symbols) {
        books_.emplace(symbol, std::make_unique<SymbolOrderBook>(symbol, &logger_, this));
      }
    }

    ~MatchingCore() {
      stop();
    }

    /// Called by SymbolOrderBook to hand a response back to the gateway.
    auto publishResponse(const EngineOrderResponse &resp) noexcept -> void {
      *(outgoing_responses_->getNextToWriteTo()) = resp;
      outgoing_responses_->updateWriteIndex();
    }

    /// Called by SymbolOrderBook to hand a market data delta to downstream consumers.
    auto publishBookUpdate(const BookUpdate &update) noexcept -> void {
      *(outgoing_updates_->getNextToWriteTo()) = update;
      outgoing_updates_->updateWriteIndex();
    }

    auto start() -> void {
      running_ = true;
      engine_thread_ = Common::createAndStartThread(-1, "Exchange/MatchingCore", [this]() { run(); });
      ASSERT(engine_thread_ != nullptr, "Failed to start MatchingCore thread.");
    }

    auto stop() -> void {
      running_ = false;
    }

    /// Main loop: pull one sequenced request at a time and dispatch it.
    auto run() noexcept -> void {
      LOG_INFO(logger_, "MatchingCore run loop starting");
      while (running_) {
        auto request = incoming_requests_->getNextToRead();
        if (LIKELY(request != nullptr)) {
          processRequest(*request);
          incoming_requests_->updateReadIndex();
        }
      }
    }

    /// Deleted default, copy & move constructors and assignment-operators.
    MatchingCore() = delete;
    MatchingCore(const MatchingCore &) = delete;
    MatchingCore(const MatchingCore &&) = delete;
    MatchingCore &operator=(const MatchingCore &) = delete;
    MatchingCore &operator=(const MatchingCore &&) = delete;

  private:
    /// Validate, route to the right symbol's book, and dispatch by order type.
    /// Anything that fails validation is rejected with a response - never crashes.
    auto processRequest(const EngineOrderRequest &req) noexcept -> void {
      auto it = books_.find(req.symbol_);
      if (UNLIKELY(it == books_.end())) {
        publishResponse({ResponseStatus::REJECTED, req.client_id_, req.symbol_, req.order_id_,
                          Common::Invalid_OrderId, req.side_, req.price_, 0, req.qty_});
        return;
      }
      auto *book = it->second.get();

      const bool needs_price = (req.order_type_ == Common::OrderType::LIMIT ||
                                 req.order_type_ == Common::OrderType::IOC ||
                                 req.order_type_ == Common::OrderType::MODIFY);
      const bool needs_qty = (req.order_type_ == Common::OrderType::LIMIT ||
                               req.order_type_ == Common::OrderType::MARKET ||
                               req.order_type_ == Common::OrderType::IOC ||
                               req.order_type_ == Common::OrderType::MODIFY);

      if (UNLIKELY(needs_qty && (req.qty_ == 0 || req.qty_ == Common::Invalid_Qty))) {
        publishResponse({ResponseStatus::REJECTED, req.client_id_, req.symbol_, req.order_id_,
                          Common::Invalid_OrderId, req.side_, req.price_, 0, req.qty_});
        return;
      }
      if (UNLIKELY(needs_price && req.price_ <= 0)) {
        publishResponse({ResponseStatus::REJECTED, req.client_id_, req.symbol_, req.order_id_,
                          Common::Invalid_OrderId, req.side_, req.price_, 0, req.qty_});
        return;
      }

      switch (req.order_type_) {
        case Common::OrderType::LIMIT:
          book->addLimitOrder(req.client_id_, req.order_id_, req.side_, req.price_, req.qty_);
          break;
        case Common::OrderType::MARKET:
          book->addMarketOrder(req.client_id_, req.order_id_, req.side_, req.qty_);
          break;
        case Common::OrderType::IOC:
          book->addIOCOrder(req.client_id_, req.order_id_, req.side_, req.price_, req.qty_);
          break;
        case Common::OrderType::CANCEL:
          book->cancelOrder(req.client_id_, req.order_id_);
          break;
        case Common::OrderType::MODIFY:
          book->modifyOrder(req.client_id_, req.order_id_, req.price_, req.qty_);
          break;
        default:
          publishResponse({ResponseStatus::REJECTED, req.client_id_, req.symbol_, req.order_id_,
                            Common::Invalid_OrderId, req.side_, req.price_, 0, req.qty_});
          break;
      }
    }

    OrderBookMap books_;

    OrderRequestQueue  *incoming_requests_  = nullptr;
    OrderResponseQueue *outgoing_responses_ = nullptr;
    MarketUpdateQueue  *outgoing_updates_   = nullptr;

    std::atomic<bool> running_ = {false};
    Common::Logger logger_;
    std::thread *engine_thread_ = nullptr;
  };
}