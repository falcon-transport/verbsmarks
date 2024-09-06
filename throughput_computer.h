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

#ifndef VERBSMARKS_THROUGHPUT_COMPUTER_H_
#define VERBSMARKS_THROUGHPUT_COMPUTER_H_

#include <complex.h>

#include <cstdint>
#include <vector>

#include "utils.h"
#include "verbsmarks.pb.h"

namespace verbsmarks {

// This class keeps tracks of overall and per-second throughput, by getting the
// timestamps at which operations complete, as well as the size of the
// operations that complete, and determining second boundaries. This class is
// not safe for concurrent access.
class ThroughputComputer {
 public:
  ThroughputComputer()
      : beginning_of_measurement_(kNotStartedTimestamp),
        beginning_of_current_second_(kNotStartedTimestamp),
        ops_in_current_second_(0),
        bytes_in_current_second_(0) {}

  // Increments the tally of operations and bytes that have completed in the
  // current second. `timestamp` is the completion time of the measurement being
  // added, in nanoseconds. `bytes_completed` is the size of the operation that
  // completed. In the first call to `AddMeasurement`, `timestamp` will be used
  // as the start time of the first second. If `timestamp` is more than one
  // second after the start time of the current second, we start a new second
  // and return the throughput of the recently completed seconds. Otherwise this
  // method returns an empty vector. This is a no-op if FinishMeasurements() has
  // already been called.
  std::vector<proto::ThroughputResult> AddMeasurement(int64_t timestamp,
                                                      int64_t bytes_completed);

  // Indicates that there will be no more measurements added. The current second
  // is completed (or until given timestamp if specified) and the tally of
  // measurements in the remaining seconds is included in the average. Returns
  // the throughput of the remaining seconds.
  std::vector<proto::ThroughputResult> FinishMeasurements(
      int64_t timestamp = 0);

  // Calculates average ops per second and bytes per second based on total
  // counts and beginning/end timestamps.
  proto::ThroughputResult GetAverageResult() const;

 private:
  // A sentinel timestamp to indicate that we have not yet received any
  // measurements.
  static constexpr int64_t kNotStartedTimestamp = -1;
  // A sentinel timestamp to indicate that we have added all measurements to the
  // average.
  static constexpr int64_t kFinishedTimestamp = -10;

  // The current average throughput of the seconds that have completed.
  uint64_t total_ops_ = 0;
  // The current average goodput of the seconds that have completed.
  uint64_t total_bytes_ = 0;
  // The timestamp in nanoseconds of the time we started the measurement.
  uint64_t beginning_of_measurement_ = 0;
  // Measurement end time in nanoseconds.
  uint64_t end_of_measurement_ = 0;
  // The timestamp in nanoseconds of the current second for which we are keeping
  // a running tally.
  int64_t beginning_of_current_second_ = 0;
  // The number of operations that have completed since, and within one second
  // of, `beginning_of_current_second_`.
  int64_t ops_in_current_second_ = 0;
  // The number of operations that have completed since, and within one second
  // of, `beginning_of_current_second_`.
  int64_t bytes_in_current_second_ = 0;

  // Incorporates the number of operations and bytes that completed in the
  // current second into the overall averages stored in
  // `overall_ops_per_second_` and `overall_bytes_per_second_`.
  void AddCurrentSecondToAverage();

  // The seconds that throughput_compter has finalized results. If reaching
  // finished time, return 0.
  int32_t GetCompletedSecond() const {
    if (beginning_of_current_second_ == kFinishedTimestamp) {
      return 0;
    }
    return (beginning_of_current_second_ - beginning_of_measurement_) /
           utils::kSecondInNanoseconds;
  }
};

}  // namespace verbsmarks

#endif  // VERBSMARKS_THROUGHPUT_COMPUTER_H_
