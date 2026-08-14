#pragma once

#include <mutex>
#include <vector>
#include <thread>
#include <atomic>

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
  /// orders them by receive time before publishing to the matching engine's queue.
  ///
  /// NOTE: unlike the fully lock-free stages elsewhere in this engine, ingestion here
  /// uses a small mutex-protected inbox. Producer submission is not on the matching hot
  /// path, so the trade-off is acceptable and documented in the README; it avoids
  /// building a general MPSC lock-free queue for a single, low-frequency entry point.
  class OrderGatewayServer final {
  public:
    OrderGatewayServer(OrderRequestQueue *outgoing_requests, OrderResponseQueue *incoming_responses)
        : sequencer_(outgoing_requests, &logger_), incoming_responses_(incoming_responses),
          logger_("exchange_order_gateway.log") {
    }

    ~OrderGatewayServer() {
      stop();
    }

    /// Called by any producer thread to submit a new request. Thread-safe.
    auto submit(const EngineOrderRequest &request) noexcept -> void {
      const auto rx_time = Common::getCurrentNanos();
      std::lock_guard<std::mutex> lock(inbox_mutex_);
      inbox_.push_back({rx_time, request});
    }

    /// Start the background thread that periodically sequences and publishes requests,
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
    /// sequencer under a short-lived lock, then release it before sorting/publishing.
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

    /// Forward matching-engine responses onward (e.g. to per-client callbacks / sockets).
    /// Left as a hook here since this assignment's scope stops at the in-process API.
    auto drainResponses() noexcept -> void {
      for (auto resp = incoming_responses_->getNextToRead(); incoming_responses_->size() && resp; resp = incoming_responses_->getNextToRead()) {
        LOG_INFO(logger_, "response %s", resp->toString().c_str());
        incoming_responses_->updateReadIndex();
      }
    }

    RequestSequencer sequencer_;
    OrderResponseQueue *incoming_responses_ = nullptr;

    Common::Logger logger_;

    std::mutex inbox_mutex_;
    std::vector<TimedRequest> inbox_;

    std::atomic<bool> running_ = {false};
    std::thread *gateway_thread_ = nullptr;
  };
}