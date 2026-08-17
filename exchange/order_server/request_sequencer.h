#pragma once

#include <array>
#include <algorithm>
#include <functional>

#include "Common/macros.h"
#include "Common/logging.h"
#include "exchange/order_server/order_request.h"

namespace Exchange {
  /// Upper bound on requests buffered between two flush() calls, across all producer threads.
  constexpr size_t MAX_PENDING_GATEWAY_REQUESTS = 8192;

  /// Buffers requests as they arrive (potentially out of order, from multiple producer
  /// threads) and publishes them in strict receive-time order via a router callback,
  /// which decides where each sequenced request actually goes (e.g. MatchingCore
  /// splitting the stream out to the right per-symbol queue). This is what lets a
  /// matching engine with one thread per symbol stay deterministic even though many
  /// producers are submitting concurrently.
  class RequestSequencer final {
  public:
    /// Called once per request, in receive-time order, by flush().
    using Router = std::function<void(const EngineOrderRequest &)>;

    RequestSequencer(Router router, Common::Logger *logger)
        : router_(std::move(router)), logger_(logger) {
    }

    /// Buffer one request tagged with its receive time. Not visible to the router until
    /// flush() is called. If the scratch buffer is already full, flush what's pending
    /// first rather than overflowing - a large backlog is a valid (if undesirable)
    /// runtime condition, not something that should crash the process.
    auto enqueue(Common::Nanos rx_time, const EngineOrderRequest &request) noexcept -> void {
      if (UNLIKELY(pending_size_ >= pending_.size())) {
        flush();
      }
      auto tagged = request;
      tagged.recv_time_ = rx_time;
      pending_[pending_size_++] = tagged;
    }

    /// True once the scratch buffer can't accept another enqueue() without flushing.
    auto isFull() const noexcept -> bool {
      return pending_size_ >= pending_.size();
    }

    /// Sort everything buffered since the last flush by receive time, then hand each
    /// request to the router in that order.
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
        router_(pending_[i]);
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
    Router router_;
    Common::Logger *logger_ = nullptr;

    /// Unsorted scratch buffer of requests accumulated since the last flush().
    std::array<EngineOrderRequest, MAX_PENDING_GATEWAY_REQUESTS> pending_;
    size_t pending_size_ = 0;
  };
}