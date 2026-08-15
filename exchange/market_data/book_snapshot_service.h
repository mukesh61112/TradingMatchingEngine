#pragma once

#include <unordered_map>
#include <vector>
#include <atomic>
#include <thread>

#include "Common/types.h"
#include "Common/time.h"
#include "Common/logging.h"
#include "Common/threads.h"
#include "Common/macros.h"
#include "../matcher/market_update.h"

namespace Exchange {
  /// How often (in nanoseconds) a full snapshot is regenerated from the accumulated deltas.
  constexpr Common::Nanos SNAPSHOT_INTERVAL_NANOS = 60 * Common::NANOS_TO_SECS;

  /// One resting order as it appears in a published snapshot. Deliberately has no
  /// ClientId field - market data is anonymized, same as on a real exchange feed.
  struct SnapshotEntry {
    Common::OrderId order_id_ = Common::Invalid_OrderId;
    Common::Side    side_     = Common::Side::INVALID;
    Common::Price   price_    = Common::Invalid_Price;
    Common::Qty     qty_      = Common::Invalid_Qty;
  };

  /// Mirrors the live order book for every symbol purely from the incremental
  /// BookUpdate stream (ADD/MODIFY/CANCEL) and periodically materializes a complete,
  /// point-in-time snapshot of every resting order. A consumer that missed some
  /// incremental updates - e.g. a newly-connecting client, or one recovering after a
  /// drop - can catch up by loading the latest snapshot and then replaying only the
  /// deltas that arrived after it, instead of replaying the entire history.
  class BookSnapshotService final {
  public:
    explicit BookSnapshotService(MarketUpdateQueue *incoming_updates)
        : incoming_updates_(incoming_updates), logger_("exchange_book_snapshot_service.log") {
    }

    ~BookSnapshotService() {
      stop();
    }

    auto start() -> void {
      running_ = true;
      service_thread_ = Common::createAndStartThread(-1, "Exchange/BookSnapshotService", [this]() { run(); });
      ASSERT(service_thread_ != nullptr, "Failed to start BookSnapshotService thread.");
    }

    auto stop() -> void {
      running_ = false;
    }

    /// Returns a full point-in-time copy of every resting order for one symbol - this is
    /// what a recovering/late-joining consumer would load before applying further deltas.
    auto snapshotFor(const Common::Symbol &symbol) const -> std::vector<SnapshotEntry> {
      std::vector<SnapshotEntry> out;
      auto it = mirror_.find(symbol);
      if (it == mirror_.end()) {
        return out;
      }
      out.reserve(it->second.size());
      for (const auto &entry_pair : it->second) {
        out.push_back(entry_pair.second);
      }
      return out;
    }

    /// Main loop: apply every incoming delta to the mirror, then periodically dump a
    /// full snapshot.
    auto run() noexcept -> void {
      LOG_INFO(logger_, "BookSnapshotService run loop starting");
      while (running_) {
        for (auto update = incoming_updates_->getNextToRead(); incoming_updates_->size() && update; update = incoming_updates_->getNextToRead()) {
          applyUpdate(*update);
          incoming_updates_->updateReadIndex();
        }

        if (Common::getCurrentNanos() - last_snapshot_time_ > SNAPSHOT_INTERVAL_NANOS) {
          last_snapshot_time_ = Common::getCurrentNanos();
          publishSnapshot();
        }
      }
    }

    /// Deleted default, copy & move constructors and assignment-operators.
    BookSnapshotService() = delete;
    BookSnapshotService(const BookSnapshotService &) = delete;
    BookSnapshotService(const BookSnapshotService &&) = delete;
    BookSnapshotService &operator=(const BookSnapshotService &) = delete;
    BookSnapshotService &operator=(const BookSnapshotService &&) = delete;

  private:
    /// Apply one incremental delta to the in-memory mirror. TRADE events are logged for
    /// visibility only - they don't change what needs to happen to the mirror, because a
    /// fully-filled resting order always also generates an explicit CANCEL, which is
    /// what actually removes it here.
    auto applyUpdate(const BookUpdate &update) noexcept -> void {
      auto &book = mirror_[update.symbol_];

      switch (update.type_) {
        case MarketUpdateType::ADD:
          book[update.order_id_] = SnapshotEntry{update.order_id_, update.side_, update.price_, update.qty_};
          break;
        case MarketUpdateType::MODIFY: {
          auto it = book.find(update.order_id_);
          if (it != book.end()) {
            it->second.qty_ = update.qty_;
          }
          break;
        }
        case MarketUpdateType::CANCEL:
          book.erase(update.order_id_);
          break;
        case MarketUpdateType::TRADE:
        case MarketUpdateType::INVALID:
        default:
          break;
      }
    }

    /// Materialize a full snapshot of every symbol's current book state and log it. In a
    /// networked deployment this is what would be broadcast on a dedicated snapshot
    /// channel for recovery; here it's exposed directly via snapshotFor().
    auto publishSnapshot() noexcept -> void {
      for (const auto &symbol_book : mirror_) {
        LOG_INFO(logger_, "snapshot symbol=%s orders=%d", symbol_book.first.c_str(), (int) symbol_book.second.size());
      }
    }

    MarketUpdateQueue *incoming_updates_ = nullptr;
    Common::Logger logger_;

    /// Symbol -> OrderId -> resting order snapshot entry.
    std::unordered_map<Common::Symbol, std::unordered_map<Common::OrderId, SnapshotEntry>> mirror_;

    Common::Nanos last_snapshot_time_ = 0;
    std::atomic<bool> running_ = {false};
    std::thread *service_thread_ = nullptr;
  };
}