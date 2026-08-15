#pragma once

#include <sstream>

#include "Common/types.h"
#include "Common/lock_free_queue.h"

namespace Exchange {
  /// Kind of book-changing event being broadcast.
  enum class MarketUpdateType : uint8_t {
    INVALID = 0,
    ADD     = 1,   // new order added to the book
    MODIFY  = 2,   // resting order's quantity changed (partial fill or modify)
    CANCEL  = 3,   // resting order removed from the book
    TRADE   = 4    // a match occurred at this price/qty
  };

  inline const char *marketUpdateTypeToString(MarketUpdateType type) noexcept {
    switch (type) {
      case MarketUpdateType::ADD:    return "ADD";
      case MarketUpdateType::MODIFY: return "MODIFY";
      case MarketUpdateType::CANCEL: return "CANCEL";
      case MarketUpdateType::TRADE:  return "TRADE";
      default:                      return "INVALID";
    }
  }

  /// One book-changing event, emitted by the matching engine after every add / match /
  /// cancel / modify so market data consumers (publisher, snapshot synthesizer) can
  /// keep their view of the book in sync.
  struct BookUpdate {
    MarketUpdateType type_   = MarketUpdateType::INVALID;
    Common::OrderId  order_id_ = Common::Invalid_OrderId;
    Common::Symbol   symbol_;
    Common::Side     side_   = Common::Side::INVALID;
    Common::Price    price_  = Common::Invalid_Price;
    Common::Qty      qty_    = Common::Invalid_Qty;

    auto toString() const -> std::string {
      std::stringstream ss;
      ss << "BookUpdate ["
         << "type:"   << marketUpdateTypeToString(type_)
         << " oid:"   << order_id_
         << " symbol:" << symbol_
         << " side:"  << Common::sideToString(side_)
         << " qty:"   << qty_
         << " price:" << price_
         << "]";
      return ss.str();
    }
  };

  /// Lock free queue carrying book updates from the matching engine to market data consumers.
  using MarketUpdateQueue = Common::LFQueue<BookUpdate>;
}