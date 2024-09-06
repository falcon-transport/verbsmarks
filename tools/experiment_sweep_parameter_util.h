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

#ifndef VERBSMARKS_TOOLS_EXPERIMENT_SWEEP_PARAMETER_UTIL_H_
#define VERBSMARKS_TOOLS_EXPERIMENT_SWEEP_PARAMETER_UTIL_H_

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "tools/experiment_config.pb.h"
#include "verbsmarks.pb.h"

namespace verbsmarks {

using SweepParameters = std::vector<std::vector<std::string>>;

// Given the verbsmarks sweep parameter configs, convert them into string.
// For example, for the sweep parameter configs as below:
//   number_of_threads: 1
//   number_of_threads: 10
//   number_of_qps: 2
//   number_of_qps: 20
// We store the sweep parameters in a list of list of strings:
// { {"number_of_threads-1", "number_of_threads-10"},
// {"number_of_qps-2","number_of_qps-20"} }
SweepParameters GetVerbsmarksSweepParameters(
    const proto::VerbsmarksSweepParameterConfig& sweep_parameter_config,
    uint32_t experiment_iterations);

// Given the sweep parameters extracted from config, generates the corresponding
// cartesian product.
// For example, for the sweep parameters below:
// { {"number_of_threads-1", "number_of_threads-10"},
// {"number_of_qps-2","number_of_qps-20"} }
// The corresponding cartesian product is:
// { {"number_of_threads-1", "number_of_qps-2"},
// {"number_of_threads-1", "number_of_qps-20"},
// {"number_of_threads-10", "number_of_qps-2"},
// {"number_of_threads-10", "number_of_qps-20"}}
SweepParameters GenerateCartesianProductOfSweepParameters(
    const SweepParameters& sweep_parameter_space);

// Converts a given OpRatio config string to the valid string for experiment ID.
// For example,
// "op_ratio_configs { op_ratio { op_code: RDMA_OP_READ ratio: 1 } }" -->
// "read_1"
std::string ConstructOpRatioExperimentId(std::string parameter);

// Converts a given sweep parameter combination to the experiment ID
// ({experiment_name, iteration}). For example:
// {"number_of_threads-1", "number_of_qps-2", "experiment_iteration-1"} -->
// {"number_of_threads-1/number_of_qps-2", "1"}
std::pair<std::string, std::string> ConstructExperimentId(
    const std::vector<std::string>& sweep_parameter_combination);

// Generates new config file by sweeping the parameters in the config template.
proto::LeaderConfig GenerateConfigFromSweepParameters(
    const proto::LeaderConfig& config_template,
    absl::Span<const std::string> sweep_parameters);

// Create the experiment description (includes brief experiment description and
// sweep parameter values).
absl::Status CreateExperimentDescription(
    absl::string_view path, absl::string_view experiment_description,
    const std::vector<std::string>& sweep_combination);

// Set the loopback flag.
void SetSweepLoopback(bool is_loopback);

}  // namespace verbsmarks

#endif  // VERBSMARKS_TOOLS_EXPERIMENT_SWEEP_PARAMETER_UTIL_H_
