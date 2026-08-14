#pragma once

#include <sstream>

#include "Common/types.h"
#include "Common/lock_free_queue.h"

namespace Exchange {
  /// Request structure consumed internally by the matching engine. The action to take
  /// (place a limit/market/IOC order, cancel, or modify an existing one) is carried
  /// directly by Common::OrderType, so no separate "request type" enum is needed.
  struct EngineOrderRequest {
    Common::OrderType order_type_ = Common::OrderType::INVALID;
    Common::ClientId  client_id_  = Common::Invalid_ClientId;
    Common::Symbol    symbol_;
    Common::OrderId   order_id_   = Common::Invalid_OrderId;
    Common::Side      side_       = Common::Side::INVALID;
    Common::Price     price_      = Common::Invalid_Price;
    Common::Qty       qty_        = Common::Invalid_Qty;

    /// Stamped by the gateway's RequestSequencer, not by the caller. Drives price-time
    /// priority ordering when multiple producer threads submit requests concurrently.
    Common::Nanos recv_time_ = 0;

    auto toString() const -> std::string {
      std::stringstream ss;
      ss << "EngineOrderRequest ["
         << "type:"   << Common::orderTypeToString(order_type_)
         << " client:" << client_id_
         << " symbol:" << symbol_
         << " oid:"    << order_id_
         << " side:"   << Common::sideToString(side_)
         << " qty:"    << qty_
         << " price:"  << price_
         << " rx:"     << recv_time_
         << "]";
      return ss.str();
    }
  };

  /// Lock free queue carrying sequenced order requests from the gateway to the matching engine.
  using OrderRequestQueue = Common::LFQueue<EngineOrderRequest>;
}