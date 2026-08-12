#pragma once

#include <cstdint>
#include <limits>
#include <string>

namespace Common {
using OrderId  = uint64_t;   // globally unique order identifier
using ClientId = uint64_t;
using Price    = int64_t;    // fixed-point price (e.g. price * tick_scale)
using Qty      = uint64_t;
using Symbol   = std::string;
using Nanos    = uint64_t;   // timestamp in nanoseconds

constexpr OrderId  Invalid_OrderId  = std::numeric_limits<OrderId>::max();
constexpr ClientId Invalid_ClientId = std::numeric_limits<ClientId>::max();
constexpr Price    Invalid_Price    = std::numeric_limits<Price>::min();
constexpr Qty      Invalid_Qty      = std::numeric_limits<Qty>::max();
constexpr Nanos    Invalid_Nanos    = std::numeric_limits<Nanos>::max();

enum class Side : uint8_t {
    INVALID = 0,
    BUY     = 1,
    SELL    = 2
};

inline const char* sideToString(Side side) noexcept {
    switch (side) {
        case Side::BUY:  return "BUY";
        case Side::SELL: return "SELL";
        default:         return "INVALID";
    }
}


enum class OrderType : uint8_t {
    INVALID = 0,
    LIMIT   = 1,
    MARKET  = 2,
    IOC     = 3,   // Immediate-Or-Cancel
    CANCEL  = 4,
    MODIFY  = 5
};

inline const char* orderTypeToString(OrderType type) noexcept {
    switch (type) {
        case OrderType::LIMIT:  return "LIMIT";
        case OrderType::MARKET: return "MARKET";
        case OrderType::IOC:    return "IOC";
        case OrderType::CANCEL: return "CANCEL";
        case OrderType::MODIFY: return "MODIFY";
        default:                return "INVALID";
    }
}

} // namespace Common