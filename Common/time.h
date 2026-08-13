#pragma once

#include <chrono>
#include <ctime>
#include <string>

namespace Common {
  using Nanos = int64_t;

  constexpr Nanos NANOS_TO_MICROS = 1000;
  constexpr Nanos MICROS_TO_MILLIS = 1000;
  constexpr Nanos MILLIS_TO_SECS = 1000;
  constexpr Nanos NANOS_TO_MILLIS = NANOS_TO_MICROS * MICROS_TO_MILLIS;
  constexpr Nanos NANOS_TO_SECS = NANOS_TO_MILLIS * MILLIS_TO_SECS;

  /// Current time in nanoseconds since epoch. Used for order timestamps.
  inline auto getCurrentNanos() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
  }

  /// Human-readable "YYYY-MM-DD HH:MM:SS" string of current time, written into time_str.
  inline auto &getCurrentTimeStr(std::string *time_str) {
    const auto time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&time));
    *time_str = buf;
    return *time_str;
  }
}