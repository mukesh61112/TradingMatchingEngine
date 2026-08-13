#pragma once

#include <string>
#include <fstream>
#include <cstdio>
#include <cstring>

#include "macros.h"
#include "lock_free_queue.h"
#include "threads.h"
#include "time.h"

namespace Common {
  /// Maximum size of the lock free queue of log lines to be written.
  constexpr size_t LOG_QUEUE_SIZE = 8 * 1024 * 1024 / 256;

  /// Maximum length of a single, fully-formatted log line.
  constexpr size_t LOG_LINE_MAX_LEN = 256;

  /// A single log entry: one fully-formatted line (timestamp, file, line, function,
  /// message already baked in by the caller). No per-field type tagging needed -
  /// the producer builds the string once, the background thread just writes it.
  struct LogElement {
    char buffer_[LOG_LINE_MAX_LEN] = {};
  };

  class Logger final {
  public:
    explicit Logger(const std::string &file_name)
        : file_name_(file_name), queue_(LOG_QUEUE_SIZE) {
      file_.open(file_name);
      ASSERT(file_.is_open(), "Could not open log file:" + file_name);
      logger_thread_ = createAndStartThread(-1, "Common/Logger " + file_name_, [this]() { flushQueue(); });
      ASSERT(logger_thread_ != nullptr, "Failed to start Logger thread.");
    }

    ~Logger() {
      std::string time_str;
      std::cerr << Common::getCurrentTimeStr(&time_str) << " Flushing and closing Logger for " << file_name_ << std::endl;

      while (queue_.size()) {
        using namespace std::literals::chrono_literals;
        std::this_thread::sleep_for(1s);
      }
      running_ = false;
      logger_thread_->join();

      file_.close();
      std::cerr << Common::getCurrentTimeStr(&time_str) << " Logger for " << file_name_ << " exiting." << std::endl;
    }

    /// Background thread: consumes fully-formatted line buffers from the lock free
    /// queue and writes them straight to the output log file.
    auto flushQueue() noexcept -> void {
      while (running_) {
        for (auto next = queue_.getNextToRead(); queue_.size() && next; next = queue_.getNextToRead()) {
          file_ << next->buffer_;
          queue_.updateReadIndex();
        }
        file_.flush();

        using namespace std::literals::chrono_literals;
        std::this_thread::sleep_for(10ms);
      }
    }

    /// Called by producer threads. Copies one already-formatted line into the next
    /// free slot in the lock free queue. This is the only entry point into the queue -
    /// there is no per-type overload set, the caller is responsible for formatting
    /// (see LOG_INFO / LOG_WARN / LOG_ERROR macros below).
    auto pushLine(const char *line) noexcept {
      auto elem = queue_.getNextToWriteTo();
      std::snprintf(elem->buffer_, LOG_LINE_MAX_LEN, "%s", line);
      queue_.updateWriteIndex();
    }

    /// Deleted default, copy & move constructors and assignment-operators.
    Logger() = delete;
    Logger(const Logger &) = delete;
    Logger(const Logger &&) = delete;
    Logger &operator=(const Logger &) = delete;
    Logger &operator=(const Logger &&) = delete;

  private:
    /// File to which the log entries will be written.
    const std::string file_name_;
    std::ofstream file_;

    /// Lock free queue of pre-formatted line buffers from producer threads to the
    /// background disk writer thread.
    LFQueue<LogElement> queue_;
    std::atomic<bool> running_ = {true};

    /// Background logging thread.
    std::thread *logger_thread_ = nullptr;
  };
}

/// Builds one formatted log line - "[date time] [LEVEL] [file:line function] message" -
/// on the caller's stack, then hands the finished buffer to the logger in a single call.
#define LOG_LINE(logger, level, fmt, ...)                                                                    \
  do {                                                                                                       \
    std::string time_str_;                                                                                   \
    char line_buf_[Common::LOG_LINE_MAX_LEN];                                                                 \
    std::snprintf(line_buf_, sizeof(line_buf_), "%s [%s] [%s:%d %s] " fmt "\n",                                \
                  Common::getCurrentTimeStr(&time_str_).c_str(), level, __FILE__, __LINE__, __func__, ##__VA_ARGS__); \
    (logger).pushLine(line_buf_);                                                                             \
  } while (0)

#define LOG_INFO(logger, fmt, ...)  LOG_LINE(logger, "INFO",  fmt, ##__VA_ARGS__)
#define LOG_WARN(logger, fmt, ...)  LOG_LINE(logger, "WARN",  fmt, ##__VA_ARGS__)
#define LOG_ERROR(logger, fmt, ...) LOG_LINE(logger, "ERROR", fmt, ##__VA_ARGS__)