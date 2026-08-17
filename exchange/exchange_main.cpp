// exchange_main.cpp
//
// Reads a plain CSV file of orders, feeds them through the gateway -> matching engine
// pipeline, and writes out what happened (accepts, fills, rejects, cancels...).
//
// usage: exchange_main <orders.csv> [output.txt]
//
// Line format (7 comma separated fields, no header row needed):
//   TYPE,client_id,symbol,order_id,side,price,qty
//
// TYPE is one of LIMIT / MARKET / IOC / CANCEL / MODIFY.
// side is BUY / SELL, or "-" when it doesn't apply (CANCEL).
// price/qty are plain integers. Use 0 where a field doesn't apply.
//
// Examples:
//   LIMIT,1,AAPL,1001,BUY,150,100
//   MARKET,2,AAPL,2001,SELL,0,50
//   CANCEL,1,AAPL,1001,-,0,0
//   MODIFY,1,AAPL,1001,-,151,40

#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <algorithm>
#include <chrono>
#include <thread>

#include "exchange/order_server/order_server.h"
#include "exchange/matcher/matching_core.h"
#include "exchange/market_data/market_data_feed.h"

using namespace Exchange;
using namespace Common;

static OrderType parseType(const std::string &s) {
  if (s == "LIMIT") return OrderType::LIMIT;
  if (s == "MARKET") return OrderType::MARKET;
  if (s == "IOC") return OrderType::IOC;
  if (s == "CANCEL") return OrderType::CANCEL;
  if (s == "MODIFY") return OrderType::MODIFY;
  return OrderType::INVALID;
}

static Side parseSide(const std::string &s) {
  if (s == "BUY") return Side::BUY;
  if (s == "SELL") return Side::SELL;
  return Side::INVALID;
}

static std::vector<std::string> splitFields(const std::string &line) {
  std::vector<std::string> fields;
  std::stringstream ss(line);
  std::string field;
  while (std::getline(ss, field, ',')) {
    fields.push_back(field);
  }
  return fields;
}

static bool parseLine(const std::string &line, EngineOrderRequest &req) {
  if (line.empty() || line[0] == '#') return false;

  auto fields = splitFields(line);
  if (fields.size() != 7) return false;

  req.order_type_ = parseType(fields[0]);
  if (req.order_type_ == OrderType::INVALID) return false;

  req.client_id_ = std::stoull(fields[1]);
  req.symbol_    = fields[2];
  req.order_id_  = std::stoull(fields[3]);
  req.side_      = parseSide(fields[4]);
  req.price_     = std::stoll(fields[5]);
  req.qty_       = std::stoull(fields[6]);
  return true;
}

int main(int argc, char **argv) {
  if (argc < 2) {
    std::cerr << "usage: " << argv[0] << " <orders.csv> [output.txt]\n";
    return 1;
  }

  std::ifstream input_file(argv[1]);
  if (!input_file.is_open()) {
    std::cerr << "could not open " << argv[1] << "\n";
    return 1;
  }

  std::vector<EngineOrderRequest> orders;
  std::string line;
  while (std::getline(input_file, line)) {
    EngineOrderRequest req;
    if (parseLine(line, req)) {
      orders.push_back(req);
    } else if (!line.empty() && line[0] != '#') {
      std::cerr << "skipping malformed line: " << line << "\n";
    }
  }
  input_file.close();

  if (orders.empty()) {
    std::cerr << "no valid orders found in " << argv[1] << "\n";
    return 1;
  }

  // figure out which symbols we need books for, in first-seen order
  std::vector<Symbol> symbols;
  for (const auto &o : orders) {
    if (std::find(symbols.begin(), symbols.end(), o.symbol_) == symbols.end()) {
      symbols.push_back(o.symbol_);
    }
  }

  // MatchingCore owns one request/response/update queue per symbol internally and
  // runs one matching thread per symbol - it must be constructed first so the gateway
  // and feed can be wired to its per-symbol queues.
  MatchingCore matcher(symbols);
  OrderGatewayServer gateway([&matcher](const EngineOrderRequest &req) { matcher.routeRequest(req); },
                              matcher.responseQueues());
  MarketDataFeed feed(matcher.updateQueues());

  std::vector<std::string> response_lines;
  gateway.onResponse([&](const EngineOrderResponse &r) {
    response_lines.push_back(r.toString());
  });

  std::vector<std::string> update_lines;
  feed.subscribe([&](const BookUpdate &u) {
    update_lines.push_back(u.toString());
  });

  matcher.start();
  gateway.start();
  feed.start();

  for (const auto &req : orders) {
    gateway.submit(req);
  }

  // crude but simple: wait until responses stop coming in for a bit, then call it done.
  size_t last_seen = 0;
  int idle_polls = 0;
  while (idle_polls < 15) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    if (response_lines.size() == last_seen) {
      ++idle_polls;
    } else {
      idle_polls = 0;
      last_seen = response_lines.size();
    }
  }

  feed.stop();
  gateway.stop();
  matcher.stop();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  std::ostream *out = &std::cout;
  std::ofstream output_file;
  if (argc >= 3) {
    output_file.open(argv[2]);
    if (!output_file.is_open()) {
      std::cerr << "could not open output file " << argv[2] << ", writing to stdout instead\n";
    } else {
      out = &output_file;
    }
  }

  *out << "orders read: " << orders.size() << ", symbols: ";
  for (size_t i = 0; i < symbols.size(); ++i) {
    *out << symbols[i] << (i + 1 < symbols.size() ? "," : "");
  }
  *out << "\n\n--- responses ---\n";
  for (const auto &r : response_lines) {
    *out << r << "\n";
  }
  *out << "\n--- book updates ---\n";
  for (const auto &u : update_lines) {
    *out << u << "\n";
  }

  return 0;
}