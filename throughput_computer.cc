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

#include "throughput_computer.h"

#include <cstdint>
#include <vector>

#include "absl/log/log.h"
#include "utils.h"
#include "verbsmarks.pb.h"

namespace verbsmarks {

std::vector<proto::ThroughputResult> ThroughputComputer::AddMeasurement(
    int64_t timestamp, int64_t bytes_completed) {
  std::vector<proto::ThroughputResult> result_in_last_seconds;

  if (beginning_of_current_second_ == kFinishedTimestamp) {
    LOG(ERROR) << "ThroughputComputer cannot add new measurements after "
                  "FinishMeasurements has been called.";
    return result_in_last_seconds;
  }
  end_of_measurement_ = timestamp;
  if (beginning_of_current_second_ == kNotStartedTimestamp) {
    // If this is the first measurement added to this ThroughputCounter, use
    // `timestamp` as the start time of the first second.
    beginning_of_current_second_ = timestamp;
    beginning_of_measurement_ = timestamp;
  } else if (timestamp >=
             beginning_of_current_second_ + utils::kSecondInNanoseconds) {
    // If this new measurement takes us into a new second, set the return value
    // to the throughput of the past second(s) and compute average throughput.
    do {
      int seconds_from_start =
          (beginning_of_current_second_ - beginning_of_measurement_) /
          utils::kSecondInNanoseconds;
      result_in_last_seconds.push_back(utils::MakeThroughputResult(
          ops_in_current_second_, bytes_in_current_second_,
          seconds_from_start));
      AddCurrentSecondToAverage();

      beginning_of_current_second_ += utils::kSecondInNanoseconds;
      ops_in_current_second_ = 0;
      bytes_in_current_second_ = 0;
    } while (timestamp >=
             beginning_of_current_second_ + utils::kSecondInNanoseconds);
  }

  ++ops_in_current_second_;
  bytes_in_current_second_ += bytes_completed;

  return result_in_last_seconds;
}

std::vector<proto::ThroughputResult> ThroughputComputer::FinishMeasurements(
    int64_t timestamp) {
  // If end timestamp is not provided, end_of_measurement_ will be the time of
  // the last sample.
  if (timestamp != 0) {
    end_of_measurement_ = timestamp;
  }

  std::vector<proto::ThroughputResult> result_in_last_seconds;
  if (beginning_of_current_second_ != kFinishedTimestamp) {
    // Record stats for all seconds up to the finishing time.
    do {
      int seconds_from_start =
          (beginning_of_current_second_ - beginning_of_measurement_) /
          utils::kSecondInNanoseconds;
      result_in_last_seconds.push_back(utils::MakeThroughputResult(
          ops_in_current_second_, bytes_in_current_second_,
          seconds_from_start));
      AddCurrentSecondToAverage();

      beginning_of_current_second_ += utils::kSecondInNanoseconds;
      ops_in_current_second_ = 0;
      bytes_in_current_second_ = 0;
    } while (timestamp >= beginning_of_current_second_);

    beginning_of_current_second_ = kFinishedTimestamp;
  }
  return result_in_last_seconds;
}

void ThroughputComputer::AddCurrentSecondToAverage() {
  total_ops_ += ops_in_current_second_;
  total_bytes_ += bytes_in_current_second_;
}

proto::ThroughputResult ThroughputComputer::GetAverageResult() const {
  proto::ThroughputResult result;
  double duration_nanos = (end_of_measurement_ - beginning_of_measurement_);
  double duration_seconds = duration_nanos / utils::kSecondInNanoseconds;
  if (duration_seconds == 0) {
    return result;
  }

  double ops_per_second = total_ops_ / duration_seconds;
  double bytes_per_second = total_bytes_ / duration_seconds;
  double gigabits_per_second = bytes_per_second * 8 / 1e9;

  result.set_ops_per_second(ops_per_second);
  result.set_bytes_per_second(bytes_per_second);
  result.set_gbps(gigabits_per_second);
  result.set_seconds_from_start(0);
  return result;
}

}  // namespace verbsmarks
