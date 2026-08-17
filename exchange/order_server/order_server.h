#pragma once

#include <mutex>
#include <vector>
#include <thread>
#include <atomic>
#include <functional>

#include "Common/macros.h"
#include "Common/threads.h"
#include "Common/time.h"
#include "Common/logging.h"

#include "exchange/order_server/order_request.h"
#include "exchange/order_server/order_response.h"
#include "exchange/order_server/request_sequencer.h"

namespace Exchange {
  /// Sits between (potentially many) order-producing threads and the matching engine.
  /// Accepts requests via submit(), which any producer thread can call concurrently,
  /// buffers them, and periodically hands the whole batch to a RequestSequencer which
  /// orders them by receive time and routes each one onward (e.g. into the matching
  /// engine's per-symbol queues) before draining every response queue the matching
  /// engine hands back.
  ///
  /// NOTE: unlike the fully lock-free stages elsewhere in this engine, ingestion here
  /// uses a small mutex-protected inbox. Producer submission is not on the matching hot
  /// path, so the trade-off is acceptable and documented in the README; it avoids
  /// building a general MPSC lock-free queue for a single, low-frequency entry point.
  class OrderGatewayServer final {
  public:
    OrderGatewayServer(RequestSequencer::Router router, std::vector<OrderResponseQueue *> incoming_responses)
        : sequencer_(std::move(router), &logger_), incoming_responses_(std::move(incoming_responses)),
          logger_("exchange_order_gateway.log") {
    }

    ~OrderGatewayServer() {
      stop();
    }

    /// Register a callback to receive every response coming back from the matching
    /// engine, in the order drainResponses() reads them.
    auto onResponse(std::function<void(const EngineOrderResponse &)> callback) -> void {
      response_callback_ = std::move(callback);
    }

    /// Called by any producer thread to submit a new request. Thread-safe.
    auto submit(const EngineOrderRequest &request) noexcept -> void {
      const auto rx_time = Common::getCurrentNanos();
      std::lock_guard<std::mutex> lock(inbox_mutex_);
      inbox_.push_back({rx_time, request});
    }

    /// Start the background thread that periodically sequences and routes requests,
    /// and drains matching-engine responses back out.
    auto start() -> void {
      running_ = true;
      gateway_thread_ = Common::createAndStartThread(-1, "Exchange/OrderGatewayServer", [this]() { run(); });
      ASSERT(gateway_thread_ != nullptr, "Failed to start OrderGatewayServer thread.");
    }

    auto stop() -> void {
      running_ = false;
    }

    /// Main loop: drain the inbox into the sequencer, flush it, then drain any pending
    /// responses from the matching engine.
    auto run() noexcept -> void {
      LOG_INFO(logger_, "OrderGatewayServer run loop starting");
      while (running_) {
        drainInbox();
        sequencer_.flush();
        drainResponses();

        using namespace std::literals::chrono_literals;
        std::this_thread::sleep_for(1ms);
      }
    }

    /// Deleted default, copy & move constructors and assignment-operators.
    OrderGatewayServer() = delete;
    OrderGatewayServer(const OrderGatewayServer &) = delete;
    OrderGatewayServer(const OrderGatewayServer &&) = delete;
    OrderGatewayServer &operator=(const OrderGatewayServer &) = delete;
    OrderGatewayServer &operator=(const OrderGatewayServer &&) = delete;

  private:
    struct TimedRequest {
      Common::Nanos rx_time_;
      EngineOrderRequest request_;
    };

    /// Move everything producer threads have submitted since the last drain into the
    /// sequencer under a short-lived lock, then release it before sorting/routing.
    auto drainInbox() noexcept -> void {
      std::vector<TimedRequest> batch;
      {
        std::lock_guard<std::mutex> lock(inbox_mutex_);
        if (inbox_.empty()) {
          return;
        }
        batch.swap(inbox_);
      }
      for (const auto &item : batch) {
        sequencer_.enqueue(item.rx_time_, item.request_);
      }
    }

    /// Drains every response queue the matching engine gave us (one per symbol, plus
    /// one for unroutable requests), forwarding each response via the registered
    /// callback, if any.
    auto drainResponses() noexcept -> void {
      for (auto *queue : incoming_responses_) {
        for (auto resp = queue->getNextToRead(); queue->size() && resp; resp = queue->getNextToRead()) {
          LOG_INFO(logger_, "response %s", resp->toString().c_str());
          if (response_callback_) {
            response_callback_(*resp);
          }
          queue->updateReadIndex();
        }
      }
    }

    RequestSequencer sequencer_;
    std::vector<OrderResponseQueue *> incoming_responses_;
    std::function<void(const EngineOrderResponse &)> response_callback_;

    Common::Logger logger_;

    std::mutex inbox_mutex_;
    std::vector<TimedRequest> inbox_;

    std::atomic<bool> running_ = {false};
    std::thread *gateway_thread_ = nullptr;
  };
}