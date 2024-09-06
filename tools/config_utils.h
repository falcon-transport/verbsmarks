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

#ifndef VERBSMARKS_TOOLS_CONFIG_UTILS_H_
#define VERBSMARKS_TOOLS_CONFIG_UTILS_H_

#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "verbsmarks.pb.h"

namespace verbsmarks {
namespace config_utils {

// Returns a basic leader config with 1 traffic pattern, 1 queue pair, and in
// which all operations are 32B reads.
proto::LeaderConfig GetThroughputConfig();

// Returns the config in `GetThroughputConfig`, with the size of all operations
// changed to `op_bytes`.
proto::LeaderConfig GetThroughputConfigWithSize(double op_bytes);

// Modify `config` so that all its traffic patterns will have the given
// `inline_threshold`.
void ModifyInlineThreshold(int inline_threshold, proto::LeaderConfig& config);

// Returns a modified version of `config` that has `num_threads` traffic
// patterns. If `num_threads` is more than the current number of traffic
// patterns, truncates the traffic patterns so that there are `num_threads` of
// them. If `num_threads` is more than the current number of traffic patterns,
// the last traffic pattern is copied until there are `num_threads` traffic
// patterns. Returns unmodified `config` if `num_threads` is not positive or the
// config currently has no traffic patterns.
proto::LeaderConfig GetConfigWithNumThreads(int num_threads,
                                            proto::LeaderConfig config);

// Takes a TrafficCharacteristics and changes OP ratio accordingly.
void ApplyOpRatio(double write_ratio, double read_ratio, double sendrecv_ratio,
                  double write_imm_ratio, double sendrecv_imm_ratio,
                  proto::TrafficCharacteristics* characteristics);

// Takes an Explicit Traffic and changes flow as needed.
void ApplyQpNum(proto::ExplicitTraffic* explicit_traffic, int qp_num);

// Apply repeat to all flows in the explicit traffic.
void ApplyRepeat(proto::ExplicitTraffic* explicit_traffic, int repeat);

absl::flat_hash_map<std::string, proto::LeaderConfig> GetConfigMap(
    const std::vector<int>& message_sizes,
    const std::vector<int>& qp_nums_to_try, int traffic_pattern_cnt,
    double write_ratio, double read_ratio, double sendrecv_ratio,
    double write_imm_ratio, double sendrecv_imm_ratio,
    double open_loop_load_gbps, int closed_loop_outstanding, bool bidirectional,
    int inline_threshold, bool multiple_op);
}  // namespace config_utils
}  // namespace verbsmarks

#endif  // VERBSMARKS_TOOLS_CONFIG_UTILS_H_
