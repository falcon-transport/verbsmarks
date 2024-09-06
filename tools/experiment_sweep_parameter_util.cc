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

#include "tools/experiment_sweep_parameter_util.h"

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <string>
#include <utility>
#include <vector>

#include "absl/flags/flag.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/types/span.h"
#include "google/protobuf/duration.pb.h"
#include "google/protobuf/message.h"
#include "google/protobuf/text_format.h"
#include "tools/config_utils.h"
#include "tools/experiment_config.pb.h"
#include "verbsmarks.pb.h"

//
ABSL_FLAG(bool, affinity_loopback, false,
          "If true, thread affinity is set loopback friendly when thread cnt "
          "is applied.");

ABSL_FLAG(uint32_t, min_core_id, 1,
          "Minimal core_id that verbsmarks pins to. "
          "The value must be less or equal than FLAGS_max_cpus. The flag is "
          "used to avoid running traffic on cores that might have kernel "
          "interrupts to potentially reach better performance.");

//
ABSL_FLAG(uint32_t, max_cpus, 256,
          "Max number of cores the host supports."
          "Find the value from the target host.");

namespace verbsmarks {

SweepParameters GetVerbsmarksSweepParameters(
    const proto::VerbsmarksSweepParameterConfig& sweep_parameter_config,
    uint32_t experiment_iterations) {
  if (sweep_parameter_config.offered_loads_size() &&
      sweep_parameter_config.max_outstanding_ops_size()) {
    LOG(FATAL) << "Can not sweep offered load and max outstanding ops at the "
                  "same time. Wrong configuration.";
  }

  SweepParameters sweep_parameter_space;
  if (sweep_parameter_config.number_of_threads_size()) {
    std::vector<std::string> number_of_threads_vec;
    for (const auto& it : sweep_parameter_config.number_of_threads()) {
      number_of_threads_vec.push_back(absl::StrCat("number_of_threads-", it));
    }
    sweep_parameter_space.push_back(number_of_threads_vec);
  }

  if (sweep_parameter_config.number_of_qps_size()) {
    std::vector<std::string> number_of_qps_vec;
    for (const auto& it : sweep_parameter_config.number_of_qps()) {
      number_of_qps_vec.push_back(absl::StrCat("number_of_qps-", it));
    }
    sweep_parameter_space.push_back(number_of_qps_vec);
  }

  if (sweep_parameter_config.number_of_repeats_size()) {
    std::vector<std::string> number_of_repeats_vec;
    for (const auto& it : sweep_parameter_config.number_of_repeats()) {
      number_of_repeats_vec.push_back(absl::StrCat("number_of_repeats-", it));
    }
    sweep_parameter_space.push_back(number_of_repeats_vec);
  }

  if (sweep_parameter_config.message_sizes_size()) {
    std::vector<std::string> message_sizes_vec;
    for (const auto& it : sweep_parameter_config.message_sizes()) {
      message_sizes_vec.push_back(absl::StrCat("message_sizes-", it));
    }
    sweep_parameter_space.push_back(message_sizes_vec);
  }

  if (sweep_parameter_config.op_ratio_configs_size()) {
    std::vector<std::string> op_ratio_configs_vec;
    for (const auto& it : sweep_parameter_config.op_ratio_configs()) {
      std::string op_ratio_config;
      google::protobuf::TextFormat::PrintToString(it, &op_ratio_config);
      absl::StrReplaceAll({{"\t", ""}, {"\n", " "}}, &op_ratio_config);
      LOG(INFO) << "*** op_ratio_config: \"" << op_ratio_config << "\"";
      op_ratio_configs_vec.push_back(
          absl::StrCat("op_ratio_configs-", op_ratio_config));
    }
    sweep_parameter_space.push_back(op_ratio_configs_vec);
  }

  if (sweep_parameter_config.offered_loads_size()) {
    std::vector<std::string> offered_loads_vec;
    for (const auto& it : sweep_parameter_config.offered_loads()) {
      offered_loads_vec.push_back(
          absl::StrCat("offered_loads-", absl::StrFormat("%.10f", it)));
    }
    sweep_parameter_space.push_back(offered_loads_vec);
  }

  if (sweep_parameter_config.arrival_time_distribution_type_size()) {
    std::vector<std::string> arrival_time_distribution_type_vec;
    for (const auto& it :
         sweep_parameter_config.arrival_time_distribution_type()) {
      arrival_time_distribution_type_vec.push_back(
          absl::StrCat("arrival_time_distribution_type-",
                       proto::ArrivalTimeDistributionType_Name(it)));
    }
    sweep_parameter_space.push_back(arrival_time_distribution_type_vec);
  }

  if (sweep_parameter_config.max_outstanding_ops_size()) {
    std::vector<std::string> max_outstanding_ops_vec;
    for (const auto& it : sweep_parameter_config.max_outstanding_ops()) {
      max_outstanding_ops_vec.push_back(
          absl::StrCat("max_outstanding_ops-", it));
    }
    sweep_parameter_space.push_back(max_outstanding_ops_vec);
  }

  if (sweep_parameter_config.inline_threshold_size()) {
    std::vector<std::string> inline_threshold_vec;
    for (const auto& it : sweep_parameter_config.inline_threshold()) {
      inline_threshold_vec.push_back(absl::StrCat("inline_threshold-", it));
    }
    sweep_parameter_space.push_back(inline_threshold_vec);
  }

  if (sweep_parameter_config.batch_size_size()) {
    std::vector<std::string> batch_size_vec;
    for (const auto& it : sweep_parameter_config.batch_size()) {
      batch_size_vec.push_back(absl::StrCat("batch_size-", it));
    }
    sweep_parameter_space.push_back(batch_size_vec);
  }

  if (sweep_parameter_config.experiment_time_seconds_size() > 0) {
    std::vector<std::string> experiment_time_vec;
    for (const auto& it : sweep_parameter_config.experiment_time_seconds()) {
      experiment_time_vec.push_back(
          absl::StrCat("experiment_time_seconds-", it));
    }
    sweep_parameter_space.push_back(experiment_time_vec);
  }

  if (sweep_parameter_config.traffic_class_size() > 0) {
    std::vector<std::string> traffic_class_vec;
    for (const auto& it : sweep_parameter_config.traffic_class()) {
      traffic_class_vec.push_back(absl::StrCat("traffic_class-", it));
    }
    sweep_parameter_space.push_back(traffic_class_vec);
  }

  // This is not experiment iteration but `iterations` in the special traffic,
  // such as pingpong and bandwidth.
  if (sweep_parameter_config.iterations_size() > 0) {
    std::vector<std::string> iteration_vec;
    for (const auto& it : sweep_parameter_config.iterations()) {
      iteration_vec.push_back(absl::StrCat("iterations-", it));
    }
    sweep_parameter_space.push_back(iteration_vec);
  }

  // MUST put this at the end. Add any new sweep parameters to handle above!
  // The repeated experiment iteration index starting from 1.
  std::vector<std::string> experiment_iteration_vec;
  // If the experiment_iterations is not configured in the config file. We run
  // the experiment only once by default.
  if (experiment_iterations == 0) experiment_iterations = 1;
  for (int i = 1; i <= experiment_iterations; ++i) {
    experiment_iteration_vec.push_back(
        absl::StrCat("experiment_iteration-", i));
  }
  sweep_parameter_space.push_back(experiment_iteration_vec);

  return sweep_parameter_space;
}

SweepParameters GenerateCartesianProductOfSweepParameters(
    const SweepParameters& sweep_parameter_space) {
  int cartesian_product_size = 1;
  for (const auto& it : sweep_parameter_space) {
    cartesian_product_size *= it.size();
  }
  SweepParameters cartesian_product(cartesian_product_size);

  const int sweep_parameter_space_dimension = sweep_parameter_space.size();
  std::vector<int> combination_indexes(sweep_parameter_space_dimension, 0);

  for (int i = 0; i < cartesian_product_size; ++i) {
    // Generates current combination from indexes.
    std::vector<std::string> combination(sweep_parameter_space_dimension);
    for (int j = 0; j < sweep_parameter_space_dimension; ++j) {
      combination[j] = sweep_parameter_space[j][combination_indexes[j]];
    }
    // Stores the combination.
    cartesian_product[i] = combination;

    // Calculates indexes for next combination.
    for (int idx = sweep_parameter_space_dimension - 1; idx >= 0; --idx) {
      const int current_index = combination_indexes[idx] + 1;
      if (current_index < sweep_parameter_space[idx].size()) {
        combination_indexes[idx] = current_index;
        break;
      } else {
        combination_indexes[idx] = 0;
      }
    }
  }

  return cartesian_product;
}

std::string ConstructOpRatioExperimentId(std::string parameter) {
  std::vector<std::string> config_value_pair = absl::StrSplit(parameter, '-');
  proto::OpRatioConfig op_ratio_config;
  std::string experiment_id = "";
  CHECK(google::protobuf::TextFormat::ParseFromString(config_value_pair[1],
                                                      &op_ratio_config));
  for (const auto& op_ratio : op_ratio_config.op_ratio()) {
    if (!experiment_id.empty()) absl::StrAppend(&experiment_id, "_");
    switch (op_ratio.op_code()) {
      case proto::RDMA_OP_READ:
        absl::StrAppend(&experiment_id, "read-", op_ratio.ratio());
        break;
      case proto::RDMA_OP_WRITE:
        absl::StrAppend(&experiment_id, "write-", op_ratio.ratio());
        break;
      case proto::RDMA_OP_SEND_RECEIVE:
        absl::StrAppend(&experiment_id, "send-", op_ratio.ratio());
        break;
      case proto::RDMA_OP_WRITE_IMMEDIATE:
        absl::StrAppend(&experiment_id, "writeimm-", op_ratio.ratio());
        break;
      case proto::RDMA_OP_SEND_WITH_IMMEDIATE_RECEIVE:
        absl::StrAppend(&experiment_id, "sendimm-", op_ratio.ratio());
        break;
      default:
        LOG(FATAL) << "Unknown op code.";
    }
  }
  return experiment_id;
}

std::pair<std::string, std::string> ConstructExperimentId(
    const std::vector<std::string>& sweep_parameter_combination) {
  std::string experiment_name = "";
  for (int i = 0; i < sweep_parameter_combination.size() - 1; ++i) {
    std::string parameter = sweep_parameter_combination[i];
    if (absl::StrContains(parameter, "op_ratio_configs")) {
      absl::StrAppend(&experiment_name,
                      ConstructOpRatioExperimentId(parameter));
    } else {
      // Remove any '_' that's already in the parameter name.
      parameter.erase(std::remove(parameter.begin(), parameter.end(), '_'),
                      parameter.end());
      absl::StrAppend(&experiment_name, parameter);
    }
    if (i < sweep_parameter_combination.size() - 2) {
      absl::StrAppend(&experiment_name, "_");
    }
  }
  std::vector<std::string> repeat_id =
      absl::StrSplit(sweep_parameter_combination.back(), '-');

  return std::make_pair(experiment_name, repeat_id.at(1));
}

void SetTrafficPatternCnt(proto::LeaderConfig& config, std::string parameter) {
  std::vector<std::string> config_value_pair = absl::StrSplit(parameter, '-');
  int traffic_pattern_cnt;
  CHECK(absl::SimpleAtoi(config_value_pair[1], &traffic_pattern_cnt));

  config = config_utils::GetConfigWithNumThreads(traffic_pattern_cnt, config);
  // Make thread affinity param (this is for loopback).
  config.clear_explicit_thread_affinity_param();
  config.set_thread_affinity_type(proto::THREAD_AFFINITY_EXPLICIT);
  uint32_t core = absl::GetFlag(FLAGS_min_core_id);
  if (absl::GetFlag(FLAGS_affinity_loopback)) {
    for (int follower_id = 0; follower_id < config.group_size();
         ++follower_id) {
      for (auto& traffic : config.traffic_patterns()) {
        auto* added = config.mutable_explicit_thread_affinity_param()
                          ->add_explicit_thread_affinity_configs();
        added->set_follower_id(follower_id);
        added->set_traffic_pattern_id(traffic.global_traffic_id());
        added->set_cpu_core_id(core);
        ++core;
        if (core >= absl::GetFlag(FLAGS_max_cpus)) {
          core = absl::GetFlag(FLAGS_min_core_id);
        }
      }
    }
  } else {
    for (int follower_id = 0; follower_id < config.group_size();
         ++follower_id) {
      core = 1;
      for (auto& traffic : config.traffic_patterns()) {
        auto* added = config.mutable_explicit_thread_affinity_param()
                          ->add_explicit_thread_affinity_configs();
        added->set_follower_id(follower_id);
        added->set_traffic_pattern_id(traffic.global_traffic_id());
        added->set_cpu_core_id(core);
        ++core;
        if (core >= absl::GetFlag(FLAGS_max_cpus)) {
          core = absl::GetFlag(FLAGS_min_core_id);
        }
      }
    }
  }
}

void SetQpNumbers(proto::LeaderConfig& config, std::string parameter) {
  std::vector<std::string> config_value_pair = absl::StrSplit(parameter, '-');
  int qp_number;
  CHECK(absl::SimpleAtoi(config_value_pair[1], &qp_number));

  for (auto& traffic_pattern : *config.mutable_traffic_patterns()) {
    config_utils::ApplyQpNum(traffic_pattern.mutable_explicit_traffic(),
                             qp_number);
  }
}

void ApplyRepeat(proto::LeaderConfig& config, std::string parameter) {
  std::vector<std::string> config_value_pair = absl::StrSplit(parameter, '-');
  int repeat;
  CHECK(absl::SimpleAtoi(config_value_pair[1], &repeat));

  for (auto& traffic_pattern : *config.mutable_traffic_patterns()) {
    config_utils::ApplyRepeat(traffic_pattern.mutable_explicit_traffic(),
                              repeat);
  }
}

void SetMessageSize(proto::LeaderConfig& config, std::string parameter) {
  std::vector<std::string> config_value_pair = absl::StrSplit(parameter, '-');
  int message_size;
  CHECK(absl::SimpleAtoi(config_value_pair[1], &message_size));

  for (auto& traffic_pattern : *config.mutable_traffic_patterns()) {
    traffic_pattern.mutable_traffic_characteristics()
        ->mutable_size_distribution_params_in_bytes()
        ->set_mean(message_size);
    traffic_pattern.mutable_traffic_characteristics()
        ->mutable_size_distribution_params_in_bytes()
        ->set_min(message_size);
    traffic_pattern.mutable_traffic_characteristics()
        ->mutable_size_distribution_params_in_bytes()
        ->set_max(message_size);
  }
}

void SetOpRatio(proto::LeaderConfig& config, std::string parameter) {
  std::vector<std::string> config_value_pair = absl::StrSplit(parameter, '-');
  proto::OpRatioConfig op_ratio_config;
  CHECK(google::protobuf::TextFormat::ParseFromString(config_value_pair[1],
                                                      &op_ratio_config));
  double write_ratio = 0, read_ratio = 0, sendrecv_ratio = 0,
         write_imm_ratio = 0, sendrecv_imm_ratio = 0;
  for (const auto& op_ratio : op_ratio_config.op_ratio()) {
    switch (op_ratio.op_code()) {
      case proto::RDMA_OP_READ:
        read_ratio = op_ratio.ratio();
        break;
      case proto::RDMA_OP_WRITE:
        write_ratio = op_ratio.ratio();
        break;
      case proto::RDMA_OP_SEND_RECEIVE:
        sendrecv_ratio = op_ratio.ratio();
        break;
      case proto::RDMA_OP_WRITE_IMMEDIATE:
        write_imm_ratio = op_ratio.ratio();
        break;
      case proto::RDMA_OP_SEND_WITH_IMMEDIATE_RECEIVE:
        sendrecv_imm_ratio = op_ratio.ratio();
        break;
      default:
        LOG(FATAL) << "Unknown op code.";
    }
  }

  for (auto& traffic_pattern : *config.mutable_traffic_patterns()) {
    auto* characteristics = traffic_pattern.mutable_traffic_characteristics();
    config_utils::ApplyOpRatio(write_ratio, read_ratio, sendrecv_ratio,
                               write_imm_ratio, sendrecv_imm_ratio,
                               characteristics);
  }
}

void SetOfferedLoad(proto::LeaderConfig& config, std::string parameter) {
  std::vector<std::string> config_value_pair = absl::StrSplit(parameter, '-');
  double offered_load;
  CHECK(absl::SimpleAtod(config_value_pair[1], &offered_load));

  for (auto& traffic_pattern : *config.mutable_traffic_patterns()) {
    traffic_pattern.mutable_open_loop_parameters()->set_offered_load_gbps(
        offered_load);
    traffic_pattern.clear_closed_loop_max_outstanding();
  }
}

void SetTimeDistributionType(proto::LeaderConfig& config,
                             std::string parameter) {
  std::vector<std::string> config_value_pair = absl::StrSplit(parameter, '-');
  proto::ArrivalTimeDistributionType distribution_type;
  if (!proto::ArrivalTimeDistributionType_Parse(config_value_pair[1],
                                                &distribution_type)) {
    LOG(ERROR) << "Unknown distribution type: " << config_value_pair[1];
    return;
  }

  for (auto& traffic_pattern : *config.mutable_traffic_patterns()) {
    traffic_pattern.mutable_open_loop_parameters()
        ->set_arrival_time_distribution_type(distribution_type);
  }
}

void SetMaxOutstandingOps(proto::LeaderConfig& config, std::string parameter) {
  std::vector<std::string> config_value_pair = absl::StrSplit(parameter, '-');
  int outstanding_ops;
  CHECK(absl::SimpleAtoi(config_value_pair[1], &outstanding_ops));

  for (auto& traffic_pattern : *config.mutable_traffic_patterns()) {
    traffic_pattern.set_closed_loop_max_outstanding(outstanding_ops);
  }
}

void SetInlindThreshold(proto::LeaderConfig& config, std::string parameter) {
  std::vector<std::string> config_value_pair = absl::StrSplit(parameter, '-');
  int inline_threshold;
  CHECK(absl::SimpleAtoi(config_value_pair[1], &inline_threshold));

  for (auto& traffic_pattern : *config.mutable_traffic_patterns()) {
    traffic_pattern.mutable_traffic_characteristics()->set_inline_threshold(
        inline_threshold);
  }
}

void SetBatchSize(proto::LeaderConfig& config, std::string parameter) {
  std::vector<std::string> config_value_pair = absl::StrSplit(parameter, '-');
  int batch_size;
  CHECK(absl::SimpleAtoi(config_value_pair[1], &batch_size));

  for (auto& traffic_pattern : *config.mutable_traffic_patterns()) {
    traffic_pattern.mutable_traffic_characteristics()->set_batch_size(
        batch_size);
  }
}

void SetTrafficClass(proto::LeaderConfig& config, std::string parameter) {
  std::vector<std::string> config_value_pair = absl::StrSplit(parameter, '-');
  int traffic_class;
  CHECK(absl::SimpleAtoi(config_value_pair[1], &traffic_class));  // Crash OK

  for (auto& traffic_pattern : *config.mutable_traffic_patterns()) {
    traffic_pattern.mutable_traffic_characteristics()->set_traffic_class(
        traffic_class);
  }
}

void SetExperimentTime(proto::LeaderConfig& config, std::string parameter) {
  std::vector<std::string> config_value_pair = absl::StrSplit(parameter, '-');
  int32_t experiment_time_seconds;

  CHECK(absl::SimpleAtoi(config_value_pair[1],  // Crash OK
                         &experiment_time_seconds));

  for (auto& traffic_pattern : *config.mutable_traffic_patterns()) {
    google::protobuf::Duration* experiment_time =
        traffic_pattern.mutable_traffic_characteristics()
            ->mutable_experiment_time();
    experiment_time->set_seconds(experiment_time_seconds);
  }
}

void SetIterations(proto::LeaderConfig& config, std::string parameter) {
  std::vector<std::string> config_value_pair = absl::StrSplit(parameter, '-');
  int64_t iterations;
  CHECK(absl::SimpleAtoi(config_value_pair[1], &iterations));  // Crash OK
  LOG(INFO) << "Setting Iterations: " << iterations;

  for (auto& traffic_pattern : *config.mutable_traffic_patterns()) {
    // Bandwidth traffic and pingpong traffic accepts iterations.
    if (traffic_pattern.has_bandwidth_traffic()) {
      traffic_pattern.mutable_bandwidth_traffic()->set_iterations(iterations);
    } else if (traffic_pattern.has_pingpong_traffic()) {
      traffic_pattern.mutable_pingpong_traffic()->set_iterations(iterations);
    }
  }
}

absl::Status CreateExperimentDescription(
    absl::string_view path, absl::string_view experiment_description,
    const std::vector<std::string>& sweep_combination) {
  std::ofstream outfile(absl::StrCat(path, "/DESCRIPTION"), std::ios::out);
  if (!outfile) {
    return absl::InternalError(
        absl::StrCat("Failed to open DESCRIPTION file at path ", path));
  }
  outfile << "Experiment Description\n" << experiment_description << "\n";
  outfile << "Sweep Parameters\n";
  for (const auto& sweep_parameter : sweep_combination) {
    std::vector<std::string> parameter_value =
        absl::StrSplit(sweep_parameter, '-');
    outfile << absl::Substitute("$0: $1\n", parameter_value.at(0),
                                parameter_value.at(1));
  }
  outfile.close();
  return absl::OkStatus();
}

void SetSweepLoopback(bool is_loopback) {
  absl::SetFlag(&FLAGS_affinity_loopback, is_loopback);
}

proto::LeaderConfig GenerateConfigFromSweepParameters(
    const proto::LeaderConfig& config_template,
    absl::Span<const std::string> sweep_parameters) {
  proto::LeaderConfig generated_config = config_template;
  // Note that the configs except number_of_threads are per traffic pattern.
  for (const auto& parameter : sweep_parameters) {
    if (absl::StrContains(parameter, "number_of_threads")) {
      SetTrafficPatternCnt(generated_config, parameter);
      continue;
    }
    if (absl::StrContains(parameter, "number_of_qps")) {
      SetQpNumbers(generated_config, parameter);
      continue;
    }
    if (absl::StrContains(parameter, "number_of_repeats")) {
      ApplyRepeat(generated_config, parameter);
      continue;
    }
    if (absl::StrContains(parameter, "message_sizes")) {
      SetMessageSize(generated_config, parameter);
      continue;
    }
    if (absl::StrContains(parameter, "op_ratio_configs")) {
      SetOpRatio(generated_config, parameter);
      continue;
    }
    if (absl::StrContains(parameter, "offered_loads")) {
      SetOfferedLoad(generated_config, parameter);
      continue;
    }
    if (absl::StrContains(parameter, "arrival_time_distribution_type")) {
      SetTimeDistributionType(generated_config, parameter);
      continue;
    }
    if (absl::StrContains(parameter, "max_outstanding_ops")) {
      SetMaxOutstandingOps(generated_config, parameter);
      continue;
    }
    if (absl::StrContains(parameter, "inline_threshold")) {
      SetInlindThreshold(generated_config, parameter);
      continue;
    }
    if (absl::StrContains(parameter, "batch_size")) {
      SetBatchSize(generated_config, parameter);
      continue;
    }
    if (absl::StrContains(parameter, "experiment_time_seconds")) {
      SetExperimentTime(generated_config, parameter);
      continue;
    }
    if (absl::StrContains(parameter, "iterations")) {
      SetIterations(generated_config, parameter);
      continue;
    }
    if (absl::StrContains(parameter, "traffic_class")) {
      SetTrafficClass(generated_config, parameter);
      continue;
    }
  }

  return generated_config;
}

}  // namespace verbsmarks
