#pragma once

#include <sstream>
#include "Common/types.h"

namespace Exchange {
  /// A single resting order inside one symbol's order book. Doubles as a node in the
  /// intrusive doubly-linked FIFO list of orders sitting at its price level, so once an
  /// order is located (via the book's OrderId -> BookOrder* index) cancel/modify/remove
  /// is O(1) - no scanning required.
  struct BookOrder {
    Common::OrderId   order_id_     = Common::Invalid_OrderId;
    Common::ClientId  client_id_    = Common::Invalid_ClientId;
    Common::Symbol    symbol_;
    Common::OrderType order_type_   = Common::OrderType::INVALID;
    Common::Side      side_         = Common::Side::INVALID;
    Common::Price     price_        = Common::Invalid_Price;
    Common::Qty       original_qty_ = Common::Invalid_Qty;   // quantity as (re)submitted, unchanged by fills
    Common::Qty       qty_          = Common::Invalid_Qty;   // remaining (unfilled) quantity
    Common::Nanos     priority_ts_  = 0;                     // time priority within its price level

    BookOrder *prev_ = nullptr;
    BookOrder *next_ = nullptr;

    BookOrder() = default;
    BookOrder(Common::OrderId order_id, Common::ClientId client_id, Common::Symbol symbol,
              Common::OrderType order_type, Common::Side side, Common::Price price,
              Common::Qty original_qty, Common::Qty qty, Common::Nanos priority_ts) noexcept
        : order_id_(order_id), client_id_(client_id), symbol_(std::move(symbol)), order_type_(order_type),
          side_(side), price_(price), original_qty_(original_qty), qty_(qty), priority_ts_(priority_ts) {
    }

    auto toString() const -> std::string {
      std::stringstream ss;
      ss << "BookOrder ["
         << "oid:"      << order_id_
         << " client:"   << client_id_
         << " symbol:"   << symbol_
         << " type:"     << Common::orderTypeToString(order_type_)
         << " side:"     << Common::sideToString(side_)
         << " price:"    << price_
         << " orig_qty:" << original_qty_
         << " qty:"      << qty_
         << " ts:"       << priority_ts_
         << "]";
      return ss.str();
    }
  };

  /// One price level: a FIFO (time-priority) list of BookOrder nodes all resting at the
  /// same price. head_ is matched first, tail_ is matched last.
  struct PriceLevel {
    Common::Price price_ = Common::Invalid_Price;
    BookOrder *head_ = nullptr;
    BookOrder *tail_ = nullptr;
    size_t order_count_ = 0;

    PriceLevel() = default;
    explicit PriceLevel(Common::Price price) noexcept : price_(price) {
    }
  };
}