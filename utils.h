/*
 * Copyright 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef VERBSMARKS_UTILS_H_
#define VERBSMARKS_UTILS_H_

#include <pthread.h>

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "connection_coordinator.pb.h"
#include "folly/stats/TDigest.h"
#include "google/protobuf/duration.pb.h"
#include "google/protobuf/repeated_ptr_field.h"
#include "google/protobuf/timestamp.pb.h"
#include "grpcpp/security/credentials.h"
#include "grpcpp/security/server_credentials.h"
#include "verbsmarks.pb.h"

namespace verbsmarks {
namespace utils {

// The number of nanoseconds in a second.
constexpr int64_t kSecondInNanoseconds = 1e9;

// Short deadline for timely requests (e.g., leader health check)
constexpr std::chrono::seconds kGrpcRequestTimeout(30);

// Longer deadline for experiment start/finish RPCs, which may be necessary for
// large-scale experiments.
constexpr std::chrono::seconds kGrpcLongRequestTimeout(300);

// After the experiment time is over, terminate after this time if it failes to
// drain the completion queue.
constexpr absl::Duration kHardCutoffAfterExperiment = absl::Seconds(30);

// ----------------------------------------------------------------------
// HostPortString()
//    Given a host and port, returns a string of the form "host:port" or
//    "[ho:st]:port", depending on whether host contains colons like
//    an IPv6 literal.  If the host is already bracketed, then additional
//    brackets will not be added.
// ----------------------------------------------------------------------
std::string HostPortString(absl::string_view host, int port);

// Saves contents to file. If it fails to open file, it dumps the content to
// stdout.
absl::Status SaveToFile(absl::string_view filename, absl::string_view content,
                        bool append = false);

google::protobuf::Timestamp TimeToProto(absl::Time d);

void DurationToProto(absl::Duration d, google::protobuf::Duration& proto);

absl::Duration ProtoToDuration(const google::protobuf::Duration& proto);

// Returns a human readable string representation of `op_type`.
std::string_view OpTypeToString(const proto::RdmaOp op_type);

proto::ThroughputResult MakeThroughputResult(double_t ops_per_second,
                                             double_t bytes_per_second,
                                             int32_t seconds_from_start = 0);

// Increments each of the fields in `result` by the value of the field in
// `append`.
void AppendThroughputResult(proto::ThroughputResult& result,
                            const proto::ThroughputResult& append);

// Sets the fields of the `LatencyResult` proto from a tdigest instance.
proto::LatencyResult LatencyTdigestToProto(const folly::TDigest& tdigest);

// Reads the leader config from proto text.
absl::StatusOr<proto::LeaderConfig> ReadVerbsmarksLeaderConfig();

// Return the desired creds based on the configuration.
std::shared_ptr<grpc::ServerCredentials> GetGrpcServerCredentials();
std::shared_ptr<grpc::ChannelCredentials> GetGrpcChannelCredentials();

// Keeps track of the average of a series of integer values.
class RunningAverage {
 public:
  RunningAverage() : average_(0), count_(0) {}

  // Incorporates `new_value` into the average.
  void Add(int64_t new_value) {
    ++count_;
    // Convert the integer types to floating point before performing
    // computations.
    double_t count_double = static_cast<double_t>(count_);
    // Multiply `average_` by a fraction, rather than multiplying `average_`
    // then dividing it, to avoid integer overflow.
    average_ = (((count_double - 1) / count_double) * average_) +
               static_cast<double_t>(new_value) / count_double;
  }

  double_t Get() const { return average_; }

 private:
  double_t average_;
  int64_t count_;
};

// Keeps track of multiple average latencies, according to their string
// description, and can dump them into a proto. Useful for debugging.
class LatencyTracker {
 public:
  // Incorporates `latency` into the average of latencies added for
  // `description`.
  void Add(absl::string_view description, uint64_t latency) {
    if (!latencies_.contains(description)) {
      latencies_[description] = RunningAverage();
    }
    latencies_[description].Add(latency);
  }

  // Creates a `proto::Metric` for the average latencies for all descriptions
  // that have been added.
  google::protobuf::RepeatedPtrField<proto::Metric> Dump() const {
    google::protobuf::RepeatedPtrField<proto::Metric> metrics;
    for (const auto& elem : latencies_) {
      proto::Metric* metric = metrics.Add();
      metric->set_description(elem.first);
      metric->set_double_value(elem.second.Get());
    }
    return metrics;
  }

 private:
  absl::flat_hash_map<std::string, RunningAverage> latencies_;
};

// If `desired_cpu` is bigger than the number of CPUs on the system, apply
// modulo and returns the value.
int AssignableCpu(int desired_cpu);

// Tries to pin `thread` to `desired_cpu`. If error happens, logs the error.
void SetThreadAffinity(int desired_cpu, pthread_t thread);

// Tries to find which cpu `thread` is running on.
absl::StatusOr<int> GetThreadAffinity(pthread_t thread);

// Tries to pin `thread` to `desired_cpu`. Returns true if pinning worked. Logs
// error, if any.
bool SetThreadAffinityAndLog(int desired_cpu, pthread_t thread);

// Busy polling for a certain duration.
inline void BusyPolling(int64_t duration) {
  auto until = absl::GetCurrentTimeNanos() + duration;
  while (absl::GetCurrentTimeNanos() < until) {
  }
}

// Thread pool creates a fixed number of threads and run tasks in the queue. The
// workers busy-poll the tasks queue until all tasks are done. This is suitable
// for burst of tasks that need to be executed in parallel, not for long running
// workers handling sporadic tasks.
class ThreadPool {
 public:
  explicit ThreadPool(int workers) : tasks_in_progress_(0), stop_(false) {
    for (int i = 0; i < workers; ++i) {
      threads_.push_back(std::thread([this]() { this->Run(); }));
    }
  }

  ~ThreadPool() {
    {
      absl::MutexLock lock(&mutex_);
      stop_ = true;
    }
    for (auto& thread : threads_) {
      thread.join();
    }
  }

  // The main loop of the thread. Executes tasks in the queue. Runs until all
  // tasks are done.
  void Run() {
    while (true) {
      std::function<void()> task;
      {
        absl::MutexLock lock(&mutex_);
        if (stop_) {
          break;
        }
        if (tasks_.empty()) {
          continue;
        }
        task = tasks_.front();
        tasks_.pop();
      }
      task();
      absl::MutexLock lock(&mutex_);
      --tasks_in_progress_;
    }
  }

  // Waits until all the tasks are done. Users should call after all tasks are
  // added. Once all the tasks are done, it will set the `stop_` flag to true.
  void Wait() {
    while (true) {
      absl::MutexLock lock(&mutex_);
      if (tasks_in_progress_ == 0) {
        stop_ = true;
        return;
      }
    }
  }

  // Adds a task to the queue.
  template <typename Func>
  void Add(Func f) {
    absl::MutexLock lock(&mutex_);
    tasks_.push(f);
    ++tasks_in_progress_;
  }

 private:
  std::vector<std::thread> threads_;
  absl::Mutex mutex_;
  std::queue<std::function<void()>> tasks_ ABSL_GUARDED_BY(mutex_);
  int tasks_in_progress_ ABSL_GUARDED_BY(mutex_);
  bool stop_ ABSL_GUARDED_BY(mutex_);
};
}  // namespace utils
}  // namespace verbsmarks

#endif  // VERBSMARKS_UTILS_H_
