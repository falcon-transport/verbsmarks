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

#ifndef VERBSMARKS_LOAD_GENERATOR_H_
#define VERBSMARKS_LOAD_GENERATOR_H_

#include <functional>
#include <memory>
#include <optional>
#include <thread>

#include "absl/base/thread_annotations.h"
#include "absl/log/log.h"
#include "absl/random/distributions.h"
#include "absl/random/random.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "absl/types/optional.h"
#include "verbsmarks.pb.h"

namespace verbsmarks {

// By default, load calculations directly use absl::Now for timing via
// `default_clock`. Providing another clock impl to CreateLoadGenerator enables
// testing and added functionality.
class LoadGeneratorClock {
 public:
  virtual absl::Time TimeNow() { return absl::Now(); }
  virtual ~LoadGeneratorClock() = default;
};

extern LoadGeneratorClock default_clock;

// The LoadGenerator is responsible for modulating the rate at which operations
// are posted by determining when operations should be posted.
class LoadGenerator {
 public:
  virtual ~LoadGenerator() = default;

  // Indicates to the LoadGenerator that the experiment has begun. It returns
  // the start time for testing.
  virtual absl::Time Start() = 0;

  // Returns the number of operations if it is time to post the next operation.
  // Returns 0 if it shouldn't post any operation.
  virtual int ShouldPost() = 0;

  // Indicates to the LoadGenerator that `num_posted` new operations have been
  // issued.
  virtual void Advance(int32_t num_posted) = 0;

  // Indicates to the LoadGenerator that `num_completed` operations have
  // successfully completed.
  virtual void Complete(int32_t num_completed) = 0;
};

// Determines when operations should be posted according to a fixed time
// interval in an open loop. This class is thread-compatible. Use
// FixedDelayLoadGeneratorThreadSafe for safe multithreading.
class FixedDelayLoadGenerator : public LoadGenerator {
 public:
  // Creates a FixedDelayLoadGenerator that will determinate operations should
  // be posted at the interval `delay_between_posts`. The first operation will
  // be able to be posted at the time `Start()` is called + `initial_delay`. The
  // second at `Start()` +  `initial_delay` + `delay_between_posts`, the third
  // at `Start()` + `initial_delay` + 2 * `delay_between_posts`, and so on.
  explicit FixedDelayLoadGenerator(const absl::Duration delay_between_posts,
                                   const absl::Duration initial_delay,
                                   int num_to_post,
                                   LoadGeneratorClock *clock = &default_clock)
      : delay_between_posts_(delay_between_posts),
        initial_delay_(initial_delay),
        num_to_post_(num_to_post),
        clock_(clock) {
    if (delay_between_posts <= absl::ZeroDuration()) {
      LOG(FATAL) << "Cannot create FixedDelayLoadGenerator with non-positive "
                    "delay between posts. Delay requested: "
                 << delay_between_posts;
    }
  }

  // Sets the time of the first post operation to the current time, as the first
  // operation can be posted at the start of the experiment + `initial_delay` if
  // given.
  absl::Time Start() override;

  // Returns `num_to_post_` if `Start` has been called and the next post time
  // has passed, 0 if `Start` hasn't been called or the next post time is in
  // the future.
  int ShouldPost() override;

  // Increments the time at which the next operation can be posted by
  // `delay_between_posts`, for each operation posted. Logs a fatal error if the
  // experiment has not been started.
  void Advance(const int32_t num_posted) override;

  // No-op, open loop does not take into account the completion rate of
  // operations.
  void Complete(const int32_t num_completed) override {}

 protected:
  // The interval between operations.
  const absl::Duration delay_between_posts_;
  // The initial delay before starting to post.
  const absl::Duration initial_delay_;
  // The number of operations to post at once.
  const int num_to_post_;
  LoadGeneratorClock *clock_;

  // The timestamp that should pass before the next operation is posted. nullptr
  // if `Start` has not yet been called.
  std::optional<absl::Time> next_post_time_;
};

// Determines when operations should be posted according to a poisson arrival
// with a given mean interval in an open loop. This class is thread-compatible.
// Use PoissonArrivalLoadGeneratorThreadSafe for safe multi-threading.
class PoissonArrivalLoadGenerator : public LoadGenerator {
 public:
  // Creates a PoissonArrivalLoadGenerator that will determinate operations
  // should be posted following poisson arrival.
  explicit PoissonArrivalLoadGenerator(
      const absl::Duration mean_interval,
      LoadGeneratorClock *clock = &default_clock);

  // Sets the time of the first post operation to the current time, as the first
  // operation can be posted at the start of the experiment.
  absl::Time Start() override;

  // Returns 1 if `Start` has been called and the next post time has passed,
  // 0 otherwise.
  int ShouldPost() override;

  // Increments the time at which the next operation can be posted following
  // poisson arrival.
  void Advance(const int32_t num_posted) override;

  // No-op, open loop does not take into account the completion rate of
  // operations.
  void Complete(const int32_t num_completed) override {}

 protected:
  // The mean interval between operations.
  const absl::Duration mean_interval_;
  absl::InsecureBitGen gen_;
  // The rate parameter. Will be passed to absl::Exponential. Calculate in the
  // constructor by 1 / mean_interval.
  const double lambda_;
  LoadGeneratorClock *clock_;

  // The timestamp that should pass before the next operation is posted.
  absl::Time next_post_time_;
};

enum class BarrierStatus {
  kNotRequested,
  kRequested,   // Barrier requested by ShouldPost. BarrierRequester will update
                // to kInProgress.
  kInProgress,  // BarrierRequester is executing the barrier function.
  kGranted,     // The barrier function returned with grant.
  kDenied,      // The barrier function returned with deny.
  kFinished     // Shutdown BarrierRequester.
};

// BarrieredBurstLoadGenerator generates high-burst traffic during the burst
// cycle and does not generate traffic during rest cycle. This class creates a
// new thread which will wait for the barrier.
class BarrieredBurstLoadGenerator : public LoadGenerator {
 public:
  // `barrier_request_func`: requests a barrier and blocks until the barrier is
  // cleared.
  explicit BarrieredBurstLoadGenerator(
      const absl::Duration rest_length,
      std::function<void(verbsmarks::proto::BarrierRequest,
                         verbsmarks::proto::BarrierResponse *)>
          *barrier_request_func,
      LoadGeneratorClock *clock = &default_clock);

  ~BarrieredBurstLoadGenerator() override {
    {
      absl::MutexLock lock(&mutex_);
      barrier_status_ = BarrierStatus::kFinished;
    }
    barrier_requester_.join();
  }

  // Sets the time of the first post operation to the current time, as the first
  // operation can be posted at the start of the experiment.
  absl::Time Start() override ABSL_LOCKS_EXCLUDED(mutex_);

  // Returns 1 if the next post time has passed and the barrier is cleared.
  int ShouldPost() override ABSL_LOCKS_EXCLUDED(mutex_);

  // No-op. advancing time happens in ShouldPost as updating the barrier status.
  void Advance(const int32_t num_posted) override ABSL_LOCKS_EXCLUDED(mutex_) {}

  // No-op, does not take into account the completion rate of operations.
  void Complete(const int32_t num_completed) override {}

 private:
  absl::Mutex mutex_;
  void BarrierRequester(
      std::function<void(verbsmarks::proto::BarrierRequest,
                         verbsmarks::proto::BarrierResponse *)> *);

  // The length of the rest period. Rest can be zero.
  const absl::Duration rest_length_;
  LoadGeneratorClock *clock_;

  // The timestamp that should pass before the next operation is posted.
  // Infinite future if `Start` has not yet been called.
  absl::Time next_post_time_ ABSL_GUARDED_BY(mutex_);
  BarrierStatus barrier_status_ ABSL_GUARDED_BY(mutex_);
  std::thread barrier_requester_;
};

// Determines when operations should be posted based on a fixed upper bound on
// the number of concurrent outstanding operations. This class is
// thread-compatible. Use ClosedLoopLoadGeneratorThreadSafe for safe
// multi-threading.
class ClosedLoopLoadGenerator : public LoadGenerator {
 public:
  explicit ClosedLoopLoadGenerator(const int32_t max_outstanding_ops,
                                   const int32_t batch_size);

  // No-op for closed loop because time does not impact whether or not to post.
  absl::Time Start() override { return absl::Now(); }

  // Returns 1 if the number of operations inflight allows a batch of
  // operations to be posted without exceeding the maximum number, 0
  // otherwise.
  int ShouldPost() override;

  // Increments the current number of operations inflight by `num_posted`. Logs
  // a fatal error if `num_posted` is negative.
  void Advance(const int32_t num_posted) override;

  // Decrements the current number of operations inflight by `num_completed`. If
  // `num_completed` is greater than the number of outstanding ops, logs a fatal
  // error.
  void Complete(const int32_t num_completed) override;

 protected:
  // The maximum number of operations that can be outstanding concurrently.
  const int32_t max_outstanding_ops_;
  // The number of operations that will be posted at once. `ShouldPost` will
  // only be true when `batch_size_` operations are able to be posted.
  const int32_t batch_size_;
  // The current number of operations that have been posted but not yet
  // completed.
  int32_t curr_outstanding_ops_;
};

absl::StatusOr<std::unique_ptr<LoadGenerator>> CreateLoadGenerator(
    const proto::PerFollowerTrafficPattern &pattern, int min_wqe_cap,
    LoadGeneratorClock *clock = &default_clock);

}  // namespace verbsmarks

#endif  // VERBSMARKS_LOAD_GENERATOR_H_
