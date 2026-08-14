#pragma once

#include <array>
#include <algorithm>

#include "Common/macros.h"
#include "Common/logging.h"
#include "exchange/order_server/order_request.h"

namespace Exchange {
  /// Upper bound on requests buffered between two flush() calls, across all producer threads.
  constexpr size_t MAX_PENDING_GATEWAY_REQUESTS = 8192;

  /// Buffers requests as they arrive (potentially out of order, from multiple producer
  /// threads) and publishes them to the matching engine's queue in strict receive-time
  /// order. This is what lets a single-threaded-per-symbol matching engine stay
  /// deterministic even though many producers are submitting concurrently.
  class RequestSequencer final {
  public:
    RequestSequencer(OrderRequestQueue *out_queue, Common::Logger *logger)
        : out_queue_(out_queue), logger_(logger) {
    }

    /// Buffer one request tagged with its receive time. Not visible to the matching
    /// engine until flush() is called.
    auto enqueue(Common::Nanos rx_time, const EngineOrderRequest &request) noexcept -> void {
      ASSERT(pending_size_ < pending_.size(), "RequestSequencer overflow - too many pending requests.");
      auto tagged = request;
      tagged.recv_time_ = rx_time;
      pending_[pending_size_++] = tagged;
    }

    /// Sort everything buffered since the last flush by receive time, then push each
    /// request onto the matching engine's lock free queue in that order.
    auto flush() noexcept -> void {
      if (UNLIKELY(pending_size_ == 0)) {
        return;
      }

      std::sort(pending_.begin(), pending_.begin() + pending_size_,
                [](const EngineOrderRequest &a, const EngineOrderRequest &b) noexcept {
                  return a.recv_time_ < b.recv_time_;
                });

      for (size_t i = 0; i < pending_size_; ++i) {
        LOG_INFO(*logger_, "sequencing %s", pending_[i].toString().c_str());
        *(out_queue_->getNextToWriteTo()) = pending_[i];
        out_queue_->updateWriteIndex();
      }
      pending_size_ = 0;
    }

    /// Deleted default, copy & move constructors and assignment-operators.
    RequestSequencer() = delete;
    RequestSequencer(const RequestSequencer &) = delete;
    RequestSequencer(const RequestSequencer &&) = delete;
    RequestSequencer &operator=(const RequestSequencer &) = delete;
    RequestSequencer &operator=(const RequestSequencer &&) = delete;

  private:
    /// Destination queue read by the matching engine.
    OrderRequestQueue *out_queue_ = nullptr;
    Common::Logger *logger_ = nullptr;

    /// Unsorted scratch buffer of requests accumulated since the last flush().
    std::array<EngineOrderRequest, MAX_PENDING_GATEWAY_REQUESTS> pending_;
    size_t pending_size_ = 0;
  };
}