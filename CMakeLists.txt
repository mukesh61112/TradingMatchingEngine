cmake_minimum_required(VERSION 3.20)
project(matching_engine CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")

find_package(Threads REQUIRED)

include_directories(${CMAKE_SOURCE_DIR})
message(STATUS "CMAKE_SOURCE_DIR = ${CMAKE_SOURCE_DIR}")
message(STATUS "Include directory = ${CMAKE_SOURCE_DIR}")

# the only translation unit in the whole project - everything else is header-only

set(ENGINE_SOURCES
    exchange/matcher/order_book.cpp
)

add_executable(exchange_main exchange/exchange_main.cpp ${ENGINE_SOURCES})
target_link_libraries(exchange_main PRIVATE Threads::Threads)

enable_testing()
add_executable(test_order_book tests/test_order_book.cpp ${ENGINE_SOURCES})
target_link_libraries(test_order_book PRIVATE Threads::Threads)
add_test(NAME OrderBookTests COMMAND test_order_book)

add_executable(benchmark_main benchmarks/benchmark_main.cpp ${ENGINE_SOURCES})
target_link_libraries(benchmark_main PRIVATE Threads::Threads)