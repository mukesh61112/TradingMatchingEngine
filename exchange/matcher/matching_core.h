#pragma once

#include <thread>
#include <atomic>
#include <vector>
#include <memory>
#include <unordered_map>

#include "Common/macros.h"
#include "Common/threads.h"
#include "Common/logging.h"

#include "exchange/order_server/order_request.h"
#include "exchange/order_server/order_response.h"
#include "exchange/matcher/market_update.h"
#include "order_book.h"

namespace Exchange {
  /// Owns one SymbolOrderBook per tradeable symbol, and runs one dedicated matching
  /// thread per symbol - not one shared thread across all symbols. Each symbol gets its
  /// own inbound request queue and its own outbound response/update queues, so every
  /// queue in the system stays genuinely single-producer/single-consumer: only
  /// routeRequest() (called from the gateway's single thread) ever writes to a symbol's
  /// request queue, and only that symbol's own matching thread ever writes to its
  /// response/update queues.
  ///
  /// Matching stays deterministic because the gateway still assigns every request one
  /// global receive-time before it gets here; routeRequest() only splits that already
  /// time-ordered stream out by symbol, so each symbol's queue preserves the same
  /// relative receive-time order it always would have.
  class MatchingCore final {
  public:
    static constexpr size_t SYMBOL_QUEUE_SIZE = 1 << 16;

    explicit MatchingCore(const std::vector<Common::Symbol> &symbols) : logger_("exchange_matching_core.log") {
      for (const auto &symbol : symbols) {
        auto ctx = std::make_unique<SymbolContext>();
        ctx->request_queue  = std::make_unique<OrderRequestQueue>(SYMBOL_QUEUE_SIZE);
        ctx->response_queue = std::make_unique<OrderResponseQueue>(SYMBOL_QUEUE_SIZE);
        ctx->update_queue   = std::make_unique<MarketUpdateQueue>(SYMBOL_QUEUE_SIZE);
        ctx->book = std::make_unique<SymbolOrderBook>(symbol, &logger_, ctx->response_queue.get(),
                                                        ctx->update_queue.get());

        response_queues_.push_back(ctx->response_queue.get());
        update_queues_.push_back(ctx->update_queue.get());

        contexts_.emplace(symbol, std::move(ctx));
      }

      // A request naming a symbol we don't have a book for can't be routed to any
      // per-symbol thread, so it's rejected straight away onto this dedicated queue.
      unknown_symbol_responses_ = std::make_unique<OrderResponseQueue>(SYMBOL_QUEUE_SIZE);
      response_queues_.push_back(unknown_symbol_responses_.get());
    }

    ~MatchingCore() {
      stop();
    }

    /// Called by the order gateway, in receive-time order, once per sequenced request.
    /// Looks up the right symbol's queue and enqueues it there. The gateway is the only
    /// caller, so each symbol's request queue only ever has this one writer.
    auto routeRequest(const EngineOrderRequest &req) noexcept -> void {
      auto it = contexts_.find(req.symbol_);
      if (UNLIKELY(it == contexts_.end())) {
        publishReject(*unknown_symbol_responses_, req);
        return;
      }
      auto *queue = it->second->request_queue.get();
      *(queue->getNextToWriteTo()) = req;
      queue->updateWriteIndex();
    }

    /// One response queue per symbol, plus one for requests naming an unknown symbol.
    /// The gateway drains all of these to forward responses back to clients.
    auto responseQueues() const noexcept -> const std::vector<OrderResponseQueue *> & {
      return response_queues_;
    }

    /// One update queue per symbol. The market data feed drains all of these.
    auto updateQueues() const noexcept -> const std::vector<MarketUpdateQueue *> & {
      return update_queues_;
    }

    /// Starts one matching thread per symbol.
    auto start() -> void {
      running_ = true;
      for (auto &entry : contexts_) {
        auto *ctx = entry.second.get();
        const auto thread_name = "Exchange/MatchingCore/" + entry.first;
        ctx->thread = Common::createAndStartThread(-1, thread_name, [this, ctx]() { run(ctx); });
        ASSERT(ctx->thread != nullptr, "Failed to start matching thread for symbol " + entry.first);
      }
    }

    auto stop() -> void {
      running_ = false;
    }

    /// Deleted default, copy & move constructors and assignment-operators.
    MatchingCore() = delete;
    MatchingCore(const MatchingCore &) = delete;
    MatchingCore(const MatchingCore &&) = delete;
    MatchingCore &operator=(const MatchingCore &) = delete;
    MatchingCore &operator=(const MatchingCore &&) = delete;

  private:
    /// Everything one symbol's dedicated matching thread needs, owned in one place.
    struct SymbolContext {
      std::unique_ptr<OrderRequestQueue>  request_queue;
      std::unique_ptr<OrderResponseQueue> response_queue;
      std::unique_ptr<MarketUpdateQueue>  update_queue;
      std::unique_ptr<SymbolOrderBook>    book;
      std::thread *thread = nullptr;
    };

    static auto publishReject(OrderResponseQueue &queue, const EngineOrderRequest &req) noexcept -> void {
      EngineOrderResponse resp{ResponseStatus::REJECTED, req.client_id_, req.symbol_, req.order_id_,
                                Common::Invalid_OrderId, req.side_, req.price_, 0, req.qty_};
      *(queue.getNextToWriteTo()) = resp;
      queue.updateWriteIndex();
    }

    /// One symbol's dedicated matching loop: pulls sequenced requests from that
    /// symbol's own queue only (nothing else writes to it), validates, and dispatches
    /// to that symbol's book. Never touches any other symbol's state.
    auto run(SymbolContext *ctx) noexcept -> void {
      LOG_INFO(logger_, "matching thread starting");
      while (running_) {
        auto request = ctx->request_queue->getNextToRead();
        if (LIKELY(request != nullptr)) {
          processRequest(*ctx, *request);
          ctx->request_queue->updateReadIndex();
        }
      }
    }

    /// Validate, then dispatch by order type. Anything that fails validation is
    /// rejected with a response - never crashes.
    auto processRequest(SymbolContext &ctx, const EngineOrderRequest &req) noexcept -> void {
      const bool needs_price = (req.order_type_ == Common::OrderType::LIMIT ||
                                 req.order_type_ == Common::OrderType::IOC ||
                                 req.order_type_ == Common::OrderType::MODIFY);
      const bool needs_qty = (req.order_type_ == Common::OrderType::LIMIT ||
                               req.order_type_ == Common::OrderType::MARKET ||
                               req.order_type_ == Common::OrderType::IOC ||
                               req.order_type_ == Common::OrderType::MODIFY);

      if (UNLIKELY(needs_qty && (req.qty_ == 0 || req.qty_ == Common::Invalid_Qty))) {
        publishReject(*ctx.response_queue, req);
        return;
      }
      if (UNLIKELY(needs_price && req.price_ <= 0)) {
        publishReject(*ctx.response_queue, req);
        return;
      }

      auto *book = ctx.book.get();
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
          publishReject(*ctx.response_queue, req);
          break;
      }
    }

    std::unordered_map<Common::Symbol, std::unique_ptr<SymbolContext>> contexts_;
    std::vector<OrderResponseQueue *> response_queues_;
    std::vector<MarketUpdateQueue *>  update_queues_;
    std::unique_ptr<OrderResponseQueue> unknown_symbol_responses_;

    std::atomic<bool> running_ = {false};
    Common::Logger logger_;
  };
}