// Copyright 2024 Google LLC

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "load_generator.h"

#include <sys/types.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <thread>

#include "absl/log/log.h"
#include "absl/random/distributions.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "utils.h"
#include "verbsmarks.pb.h"

namespace verbsmarks {

LoadGeneratorClock default_clock;

using LoadParametersCase = proto::PerFollowerTrafficPattern::LoadParametersCase;

absl::StatusOr<std::unique_ptr<LoadGenerator>> CreateLoadGenerator(
    const proto::PerFollowerTrafficPattern &pattern, int min_wqe_cap,
    LoadGeneratorClock *clock) {
  if (pattern.traffic_type() == proto::TRAFFIC_TYPE_PINGPONG) {
    // In pingpong, we can post closed_loop_max_outstanding receives, but only
    // one OP in flight.
    return std::make_unique<ClosedLoopLoadGenerator>(
        /* closed_loop_max_outstanding = */ 1,
        pattern.traffic_characteristics().batch_size());
  }
  switch (pattern.load_parameters_case()) {
    case LoadParametersCase::kOpenLoopParameters: {
      auto &open_loop_params = pattern.open_loop_parameters();
      if (utils::ProtoToDuration(open_loop_params.average_interval()) <=
          absl::ZeroDuration()) {
        return absl::InvalidArgumentError(
            absl::StrCat("open loop average interval needs to be positive: ",
                         pattern.open_loop_parameters()));
      }
      // Multiply the interval by batch size because the average_interval is
      // based on posting a single operation at a time, and we wait the
      // appropriate amount of time when posting `batch_size` operations at
      // once.
      absl::Duration average_interval =
          utils::ProtoToDuration(open_loop_params.average_interval()) *
          pattern.traffic_characteristics().batch_size();
      int to_post = pattern.traffic_characteristics().batch_size();
      if (pattern.number_to_post_at_once() > 1) {
        // Incast will use this instead of batch size.
        to_post = pattern.number_to_post_at_once();
      }
      switch (open_loop_params.arrival_time_distribution_type()) {
        case proto::ARRIVAL_TIME_DISTRIBUTION_FIXED:
          return std::make_unique<FixedDelayLoadGenerator>(
              average_interval,
              utils::ProtoToDuration(pattern.initial_delay_before_posting()),
              to_post, clock);
        case proto::ARRIVAL_TIME_DISTRIBUTION_POISSON:
          return std::make_unique<PoissonArrivalLoadGenerator>(average_interval,
                                                               clock);
        default:
          return absl::UnimplementedError(
              absl::StrCat("open loop distribution type ",
                           open_loop_params.arrival_time_distribution_type(),
                           " not supported."));
      }
    } break;
    case LoadParametersCase::kClosedLoopMaxOutstanding: {
      int closed_loop_max_outstanding = pattern.closed_loop_max_outstanding();
      if (closed_loop_max_outstanding == 0) {
        closed_loop_max_outstanding = min_wqe_cap;
      }
      if (closed_loop_max_outstanding <= 0) {
        return absl::InvalidArgumentError(absl::StrCat(
            "closed_loop_max_outstanding must be positive. Value provided: ",
            pattern.closed_loop_max_outstanding()));
      }
      return std::make_unique<ClosedLoopLoadGenerator>(
          closed_loop_max_outstanding,
          pattern.traffic_characteristics().batch_size());
    } break;
    case proto::PerFollowerTrafficPattern::LOAD_PARAMETERS_NOT_SET:
      return absl::InvalidArgumentError(
          "required field load_parameters is missing.");
  }
}

absl::Time FixedDelayLoadGenerator::Start() {
  next_post_time_ = clock_->TimeNow() + initial_delay_;
  return next_post_time_.value();
}

int FixedDelayLoadGenerator::ShouldPost() {
  if (next_post_time_.has_value() &&
      clock_->TimeNow() >= next_post_time_.value()) {
    return num_to_post_;
  }
  return 0;
}

void FixedDelayLoadGenerator::Advance(const int32_t num_posted) {
  // By contract, fixed delay load generator always advance by num_to_post_.
  if (next_post_time_.has_value()) {
    *next_post_time_ += delay_between_posts_;
  } else {
    LOG(FATAL) << "FixedDelayLoadGenerator cannot Advance before `Start` has "
                  "been called.";
  }
}

PoissonArrivalLoadGenerator::PoissonArrivalLoadGenerator(
    const absl::Duration mean_interval, LoadGeneratorClock *clock)
    : mean_interval_(mean_interval),
      lambda_(1.0 / absl::ToDoubleSeconds(mean_interval)),
      clock_(clock) {
  if (mean_interval <= absl::ZeroDuration()) {
    LOG(FATAL) << "Cannot create PoissonArrivalLoadGenerator with non-positive "
                  "delay between posts. Delay requested: "
               << mean_interval;
  }
  next_post_time_ = absl::InfiniteFuture();
}

absl::Time PoissonArrivalLoadGenerator::Start() {
  next_post_time_ = clock_->TimeNow();
  return next_post_time_;
}

int PoissonArrivalLoadGenerator::ShouldPost() {
  return (clock_->TimeNow() >= next_post_time_) ? 1 : 0;
}

void PoissonArrivalLoadGenerator::Advance(const int32_t num_posted) {
  next_post_time_ += absl::Seconds(absl::Exponential(gen_, lambda_));
}

ClosedLoopLoadGenerator::ClosedLoopLoadGenerator(
    const int32_t max_outstanding_ops, const int32_t batch_size)
    : max_outstanding_ops_(max_outstanding_ops),
      batch_size_(batch_size),
      curr_outstanding_ops_(0) {
  if (max_outstanding_ops <= 0) {
    LOG(FATAL) << "Cannot create ClosedLoopLoadGenerator with non-positive "
                  "max outstanding ops. Requested: "
               << max_outstanding_ops;
  }
}

int ClosedLoopLoadGenerator::ShouldPost() {
  if (curr_outstanding_ops_ + batch_size_ <= max_outstanding_ops_) {
    return 1;
  }
  return 0;
}

void ClosedLoopLoadGenerator::Advance(const int32_t num_posted) {
  if (num_posted > 0) {
    curr_outstanding_ops_ += num_posted;
  } else {
    LOG(FATAL) << "ClosedLoopLoadGenerator cannot issue a non-positive "
                  "number of operations. Requested: "
               << num_posted << ".";
  }
}

void ClosedLoopLoadGenerator::Complete(const int32_t num_completed) {
  if (num_completed <= curr_outstanding_ops_) {
    curr_outstanding_ops_ -= num_completed;
  } else {
    LOG(FATAL)
        << "ClosedLoopLoadGenerator cannot complete more operations than "
           "have been posted. Current number of outstanding operations: "
        << curr_outstanding_ops_
        << ", requested number of operations to complete: " << num_completed
        << ".";
  }
}

BarrieredBurstLoadGenerator::BarrieredBurstLoadGenerator(
    const absl::Duration rest_length,
    std::function<void(verbsmarks::proto::BarrierRequest,
                       verbsmarks::proto::BarrierResponse *)>
        *barrier_request_func,
    LoadGeneratorClock *clock)
    : rest_length_(rest_length),
      clock_(clock),
      next_post_time_(absl::InfiniteFuture()),
      barrier_status_(BarrierStatus::kNotRequested),
      barrier_requester_(
          std::thread(&BarrieredBurstLoadGenerator::BarrierRequester, this,
                      barrier_request_func)) {
  if (barrier_request_func == nullptr) {
    LOG(FATAL)
        << "Barrier function is not provided for BarrieredBurstLoadGenerator";
  }
}

absl::Time BarrieredBurstLoadGenerator::Start() {
  absl::MutexLock lock(&mutex_);
  next_post_time_ = clock_->TimeNow();
  return next_post_time_;
}

void BarrieredBurstLoadGenerator::BarrierRequester(
    std::function<void(verbsmarks::proto::BarrierRequest,
                       verbsmarks::proto::BarrierResponse *)>
        *barrier_request_func) {
  while (true) {
    mutex_.Lock();
    if (barrier_status_ == BarrierStatus::kFinished) {
      mutex_.Unlock();
      return;
    }
    if (barrier_status_ == BarrierStatus::kRequested) {
      barrier_status_ = BarrierStatus::kInProgress;
      // Execute the barrier function after releasing the lock as the
      // barrier_request_func will block until the barrier is cleared.
      mutex_.Unlock();
      verbsmarks::proto::BarrierRequest request;
      verbsmarks::proto::BarrierResponse response;
      (*barrier_request_func)(request, &response);
      absl::MutexLock lock(&mutex_);
      if (response.post_granted()) {
        barrier_status_ = BarrierStatus::kGranted;
      } else {
        barrier_status_ = BarrierStatus::kDenied;
      }
      continue;
    }
    mutex_.Unlock();
  }
}

int BarrieredBurstLoadGenerator::ShouldPost() {
  absl::MutexLock lock(&mutex_);
  absl::Time now = clock_->TimeNow();
  if (now >= next_post_time_) {
    if (barrier_status_ == BarrierStatus::kNotRequested) {
      // We just passed the time and have not requested barrier.
      barrier_status_ = BarrierStatus::kRequested;
      return 0;
    }
    if (barrier_status_ == BarrierStatus::kGranted ||
        barrier_status_ == BarrierStatus::kDenied) {
      next_post_time_ = clock_->TimeNow() + rest_length_;
      int should_post = 0;
      if (barrier_status_ == BarrierStatus::kGranted) {
        should_post = 1;
      }
      barrier_status_ = BarrierStatus::kNotRequested;
      return should_post;
    }
  }
  return 0;
}

}  // namespace verbsmarks
