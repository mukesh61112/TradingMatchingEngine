// benchmark_main.cpp
//
// Fires a bunch of orders through the gateway -> matching engine pipeline and
// reports throughput plus round-trip latency percentiles (time from submit() to
// seeing that order's first response come back).
//
// usage: benchmark_main [num_orders]   (default 200000)

#include <iostream>
#include <vector>
#include <random>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <thread>

#include "../exchange/order_server/order_server.h"
#include "../exchange/matcher/matching_core.h"

using namespace Exchange;
using namespace Common;

int main(int argc, char **argv) {
  size_t num_orders = 200000;
  if (argc > 1) {
    num_orders = std::stoull(argv[1]);
  }

  const std::vector<Symbol> symbols = {"AAPL", "MSFT", "GOOG", "AMZN", "TSLA"};

  OrderRequestQueue request_queue(1 << 20);
  OrderResponseQueue response_queue(1 << 20);
  MarketUpdateQueue update_queue(1 << 20);

  MatchingCore matcher(symbols, &request_queue, &response_queue, &update_queue);
  OrderGatewayServer gateway(&request_queue, &response_queue);

  matcher.start();
  gateway.start();

  std::mt19937 rng(42); // fixed seed so results are reproducible across runs
  std::uniform_int_distribution<int> price_dist(95, 105);
  std::uniform_int_distribution<int> qty_dist(1, 50);
  std::uniform_int_distribution<int> side_dist(0, 1);
  std::uniform_int_distribution<int> symbol_dist(0, static_cast<int>(symbols.size()) - 1);

  // order_id -> time it was submitted, cleared once we've seen its first response
  std::vector<Nanos> submitted_at(num_orders + 1, 0);
  std::vector<Nanos> latencies_ns;
  latencies_ns.reserve(num_orders);
  std::atomic<size_t> acked_count{0};

  gateway.onResponse([&](const EngineOrderResponse &resp) {
    const auto oid = resp.client_order_id_;
    if (oid >= 1 && oid <= num_orders && submitted_at[oid] != 0) {
      latencies_ns.push_back(getCurrentNanos() - submitted_at[oid]);
      submitted_at[oid] = 0; // first response only - later fills for the same order don't count again
      acked_count.fetch_add(1, std::memory_order_release);
    }
  });

  std::cout << "submitting " << num_orders << " orders...\n";
  const auto start = std::chrono::steady_clock::now();

  for (size_t i = 1; i <= num_orders; ++i) {
    EngineOrderRequest req;
    req.order_type_ = OrderType::LIMIT;
    req.client_id_  = i % 500;
    req.symbol_     = symbols[symbol_dist(rng)];
    req.order_id_   = i;
    req.side_       = side_dist(rng) == 0 ? Side::BUY : Side::SELL;
    req.price_      = price_dist(rng);
    req.qty_        = qty_dist(rng);

    submitted_at[i] = getCurrentNanos();
    gateway.submit(req);
  }

  // wait for responses to catch up, bail out after a few idle seconds either way
  auto last_progress = std::chrono::steady_clock::now();
  size_t last_count = 0;
  while (acked_count.load(std::memory_order_acquire) < num_orders) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    const auto current = acked_count.load(std::memory_order_acquire);
    if (current != last_count) {
      last_count = current;
      last_progress = std::chrono::steady_clock::now();
    } else if (std::chrono::steady_clock::now() - last_progress > std::chrono::seconds(5)) {
      break;
    }
  }

  const auto end = std::chrono::steady_clock::now();
  const double elapsed_sec = std::chrono::duration<double>(end - start).count();

  gateway.stop();
  matcher.stop();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  std::sort(latencies_ns.begin(), latencies_ns.end());

  auto percentile = [&](double p) -> Nanos {
    if (latencies_ns.empty()) return 0;
    size_t idx = static_cast<size_t>(p * static_cast<double>(latencies_ns.size() - 1));
    return latencies_ns[idx];
  };

  Nanos total_ns = 0;
  for (auto l : latencies_ns) total_ns += l;
  const double mean_us = latencies_ns.empty() ? 0.0 : (static_cast<double>(total_ns) / latencies_ns.size()) / 1000.0;

  std::cout << "\n--- results ---\n";
  std::cout << "orders submitted    : " << num_orders << "\n";
  std::cout << "orders acknowledged : " << latencies_ns.size() << "\n";
  std::cout << "elapsed             : " << elapsed_sec << " s\n";
  std::cout << "throughput          : " << static_cast<size_t>(num_orders / elapsed_sec) << " orders/sec\n";
  std::cout << "mean latency        : " << mean_us << " us\n";
  std::cout << "p50 latency         : " << percentile(0.50) / 1000.0 << " us\n";
  std::cout << "p75 latency         : " << percentile(0.75) / 1000.0 << " us\n";
  std::cout << "p90 latency         : " << percentile(0.90) / 1000.0 << " us\n";
  std::cout << "p99 latency         : " << percentile(0.99) / 1000.0 << " us\n";
  std::cout << "max latency         : " << (latencies_ns.empty() ? 0 : latencies_ns.back()) / 1000.0 << " us\n";

  return 0;
}