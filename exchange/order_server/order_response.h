#pragma once

#include <sstream>


#include "Common/types.h"
#include "Common/lock_free_queue.h"
namespace Exchange {
  /// Outcome of an EngineOrderRequest, as reported back by the matching engine.
  enum class ResponseStatus : uint8_t {
    INVALID          = 0,
    ACCEPTED         = 1,   // resting order booked
    REJECTED         = 2,   // failed validation (bad symbol/price/qty/dup id, etc.)
    FILLED           = 3,   // partially or fully matched
    CANCELED         = 4,
    CANCEL_REJECTED  = 5,   // cancel on unknown/already-completed order ignored gracefully
    MODIFIED         = 6,
    MODIFY_REJECTED  = 7
  };

  inline const char *responseStatusToString(ResponseStatus status) noexcept {
    switch (status) {
      case ResponseStatus::ACCEPTED:        return "ACCEPTED";
      case ResponseStatus::REJECTED:        return "REJECTED";
      case ResponseStatus::FILLED:          return "FILLED";
      case ResponseStatus::CANCELED:        return "CANCELED";
      case ResponseStatus::CANCEL_REJECTED: return "CANCEL_REJECTED";
      case ResponseStatus::MODIFIED:        return "MODIFIED";
      case ResponseStatus::MODIFY_REJECTED: return "MODIFY_REJECTED";
      default:                              return "INVALID";
    }
  }

  /// Response structure produced internally by the matching engine and consumed by the
  /// order gateway, which forwards it back to the originating client.
  struct EngineOrderResponse {
    ResponseStatus   status_          = ResponseStatus::INVALID;
    Common::ClientId client_id_       = Common::Invalid_ClientId;
    Common::Symbol   symbol_;
    Common::OrderId  client_order_id_ = Common::Invalid_OrderId;   // id as submitted by the client
    Common::OrderId  market_order_id_ = Common::Invalid_OrderId;   // id assigned by the book
    Common::Side     side_            = Common::Side::INVALID;
    Common::Price    price_           = Common::Invalid_Price;
    Common::Qty      exec_qty_        = Common::Invalid_Qty;       // quantity filled in this event
    Common::Qty      leaves_qty_      = Common::Invalid_Qty;       // quantity still remaining/resting

    auto toString() const -> std::string {
      std::stringstream ss;
      ss << "EngineOrderResponse ["
         << "status:"  << responseStatusToString(status_)
         << " client:" << client_id_
         << " symbol:" << symbol_
         << " coid:"   << client_order_id_
         << " moid:"   << market_order_id_
         << " side:"   << Common::sideToString(side_)
         << " exec:"   << exec_qty_
         << " leaves:" << leaves_qty_
         << " price:"  << price_
         << "]";
      return ss.str();
    }
  };

  /// Lock free queue carrying order responses from the matching engine back to the gateway.
  using OrderResponseQueue = Common::LFQueue<EngineOrderResponse>;
}