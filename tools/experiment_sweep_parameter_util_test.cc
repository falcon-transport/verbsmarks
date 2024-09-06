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
#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_replace.h"
#include "gmock/gmock.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"
#include "net/proto2/contrib/parse_proto/testing.h"
#include "tools/config_utils.h"
#include "tools/experiment_config.pb.h"
#include "verbsmarks.pb.h"

namespace verbsmarks {
namespace {

proto::VerbsmarksSweepParameterConfig SweepParameterConfig() {
  return google::protobuf::contrib::parse_proto::ParseTestProto(R"pb(
    number_of_qps: 3
    message_sizes: 8
    message_sizes: 1024
    traffic_class: 7
    experiment_time_seconds: 5
    experiment_time_seconds: 100
    op_ratio_configs { op_ratio { op_code: RDMA_OP_WRITE ratio: 1 } }
    op_ratio_configs { op_ratio { op_code: RDMA_OP_READ ratio: 1 } }
    op_ratio_configs {
      op_ratio { op_code: RDMA_OP_READ ratio: 1 }
      op_ratio { op_code: RDMA_OP_WRITE ratio: 1 }
    }
  )pb");
}

proto::LeaderConfig IncastConfig() {
  return google::protobuf::contrib::parse_proto::ParseTestProto(R"pb(
    group_size: 6
    quorum_time_out_sec: 300

    traffic_patterns: {
      global_traffic_id: 1
      participants: { all: true }
      connection_type: CONNECTION_TYPE_RC
      closed_loop_max_outstanding: 2
      traffic_characteristics {
        op_ratio: { op_code: RDMA_OP_WRITE ratio: 1 }
        message_size_distribution_type: MESSAGE_SIZE_DISTRIBUTION_FIXED
        size_distribution_params_in_bytes: { mean: 1024000 }
        delay_time: { seconds: 0 }
        buffer_time: { seconds: 0 }
        experiment_time: { seconds: 60 }
        batch_size: 1
        signal_only_last_op_in_batch: true
        inline_threshold: 128
      }
      explicit_traffic: {
        flows: { initiator: 1 target: 0 repeat: 1 }
        flows: { initiator: 2 target: 0 repeat: 1 }
        flows: { initiator: 3 target: 0 repeat: 1 }
        flows: { initiator: 4 target: 0 repeat: 1 }
        flows: { initiator: 5 target: 0 repeat: 1 }
        bidirectional: false
      }
    }
  )pb");
}

TEST(ExperimentSweepParameterTest, ConvertSweepParameterConfigToString) {
  auto sweep_parameters = SweepParameterConfig();
  SweepParameters configs = GetVerbsmarksSweepParameters(sweep_parameters, 4);

  EXPECT_EQ(configs.size(), 6);
  for (int i = 0; i < configs.size(); ++i) {
    if (i == 0) {
      const auto& config = configs.at(i);
      EXPECT_EQ(config.size(), 1);
      EXPECT_EQ(config.at(0), absl::StrCat("number_of_qps-",
                                           sweep_parameters.number_of_qps(0)));
    } else if (i == 1) {
      const auto& config = configs.at(i);
      EXPECT_EQ(config.size(), 2);
      for (int j = 0; j < config.size(); ++j) {
        EXPECT_EQ(
            config.at(j),
            absl::StrCat("message_sizes-", sweep_parameters.message_sizes(j)));
      }
    } else if (i == 2) {
      const auto& config = configs.at(i);
      EXPECT_EQ(config.size(), 3);
      for (int j = 0; j < config.size(); ++j) {
        std::string op_ratio_config;
        google::protobuf::TextFormat::PrintToString(
            sweep_parameters.op_ratio_configs(j), &op_ratio_config);
        absl::StrReplaceAll({{"\t", ""}, {"\n", " "}}, &op_ratio_config);
        EXPECT_EQ(config.at(j),
                  absl::StrCat("op_ratio_configs-", op_ratio_config));
      }
    } else if (i == 3) {
      const auto& config = configs.at(i);
      EXPECT_EQ(config.size(), 2);
      for (int j = 0; j < config.size(); ++j) {
        EXPECT_EQ(config.at(j),
                  absl::StrCat("experiment_time_seconds-",
                               sweep_parameters.experiment_time_seconds(j)));
      }
    } else if (i == 4) {
      const auto& config = configs.at(i);
      EXPECT_EQ(config.size(), 1);
      EXPECT_EQ(config.at(0), absl::StrCat("traffic_class-",
                                           sweep_parameters.traffic_class(0)));
    } else if (i == 5) {
      const auto& config = configs.at(i);
      EXPECT_EQ(config.size(), 4);
      for (int j = 0; j < config.size(); ++j) {
        EXPECT_EQ(config.at(j), absl::StrCat("experiment_iteration-", j + 1));
      }
    }
  }
}

TEST(ExperimentSweepParameterTest, GenerateCartesianProduct) {
  auto sweep_parameters = SweepParameterConfig();
  SweepParameters configs = GetVerbsmarksSweepParameters(sweep_parameters, 0);
  auto sweep_parameter_combinations =
      GenerateCartesianProductOfSweepParameters(configs);

  int expr_time_5_cnt = 0;
  int expr_time_100_cnt = 0;
  absl::flat_hash_map<int, int> message_size_cnt;
  absl::flat_hash_map<std::string, int> op_ratio_cnt;
  EXPECT_EQ(sweep_parameter_combinations.size(), 12);
  for (int i = 0; i < sweep_parameter_combinations.size(); ++i) {
    const auto& combination = sweep_parameter_combinations.at(i);
    EXPECT_EQ(combination.size(), 6);
    EXPECT_EQ(combination.front(), "number_of_qps-3");
    EXPECT_EQ(combination.back(), "experiment_iteration-1");
    if (std::find(combination.begin(), combination.end(), "message_sizes-8") !=
        combination.end()) {
      message_size_cnt[8]++;
    }
    if (std::find(combination.begin(), combination.end(),
                  "message_sizes-1024") != combination.end()) {
      message_size_cnt[1024]++;
    }
    if (std::find(combination.begin(), combination.end(),
                  "experiment_time_seconds-5") != combination.end()) {
      expr_time_5_cnt++;
    }
    if (std::find(combination.begin(), combination.end(),
                  "experiment_time_seconds-100") != combination.end()) {
      expr_time_100_cnt++;
    }
    for (int j = 0; j < combination.size(); ++j) {
      if (absl::StrContains(combination.at(j), "op_ratio_configs")) {
        op_ratio_cnt[combination.at(j)]++;
      }
    }
  }
  // # of occurrences is accurate.
  EXPECT_EQ(expr_time_5_cnt, 6);
  EXPECT_EQ(expr_time_100_cnt, 6);
  EXPECT_EQ(message_size_cnt[8], 6);
  EXPECT_EQ(message_size_cnt[1024], 6);
  EXPECT_EQ(op_ratio_cnt.size(), 3);
  EXPECT_TRUE(op_ratio_cnt.contains(
      "op_ratio_configs-op_ratio {   op_code: RDMA_OP_READ   ratio: 1 } "));
  EXPECT_TRUE(op_ratio_cnt.contains(
      "op_ratio_configs-op_ratio {   op_code: RDMA_OP_WRITE   ratio: 1 } "));
  EXPECT_TRUE(op_ratio_cnt.contains(
      "op_ratio_configs-op_ratio {   op_code: RDMA_OP_READ   ratio: 1 } "
      "op_ratio {   op_code: RDMA_OP_WRITE   ratio: 1 } "));
  for (const auto& kv : op_ratio_cnt) {
    EXPECT_EQ(kv.second, 4);
  }
}

TEST(ExperimentSweepParameterTest, SweepParameterInTemplateNonLoopback) {
  SetSweepLoopback(false);
  auto config_template = config_utils::GetThroughputConfig();
  auto sweep_parameters = SweepParameterConfig();
  sweep_parameters.add_number_of_threads(2);
  auto sweep_parameter_combinations = GenerateCartesianProductOfSweepParameters(
      GetVerbsmarksSweepParameters(sweep_parameters, 0));

  absl::flat_hash_map<int, int> message_size_cnt;
  absl::flat_hash_map<std::string, int> op_ratio_cnt;
  for (int i = 0; i < sweep_parameter_combinations.size(); ++i) {
    auto generated_config = GenerateConfigFromSweepParameters(
        config_template, sweep_parameter_combinations.at(i));
    EXPECT_EQ(generated_config.traffic_patterns_size(), 2);
    auto traffic_pattern_0 = generated_config.mutable_traffic_patterns(0);
    auto traffic_pattern_1 = generated_config.mutable_traffic_patterns(1);
    traffic_pattern_0->clear_global_traffic_id();
    traffic_pattern_1->clear_global_traffic_id();
    EXPECT_THAT(*traffic_pattern_0, ::testing::EqualsProto(*traffic_pattern_1));
    EXPECT_EQ(
        generated_config.traffic_patterns(0).explicit_traffic().flows_size(),
        3);
    // Check message size.
    auto mean = generated_config.traffic_patterns(0)
                    .traffic_characteristics()
                    .size_distribution_params_in_bytes()
                    .mean();
    message_size_cnt[mean]++;
    std::string op_ratio_string = "";
    for (auto op_ratio : generated_config.traffic_patterns(0)
                             .traffic_characteristics()
                             .op_ratio()) {
      std::string append_string;
      google::protobuf::TextFormat::PrintToString(op_ratio, &append_string);
      op_ratio_string += append_string;
    }
    op_ratio_cnt[op_ratio_string]++;
    // Check affinity - non loopback affinity should start at 1 and unique per
    // follower.
    absl::flat_hash_map<int, absl::flat_hash_set<int>>
        affinity_set_per_follower;
    absl::flat_hash_set<int> followers;
    for (int follower : generated_config.traffic_patterns(0)
                            .participants()
                            .specific_followers()
                            .participant()) {
      followers.insert(follower);
    }
    for (const int follower : followers) {
      int core = 1;
      for (const auto traffic : generated_config.traffic_patterns()) {
        affinity_set_per_follower[follower].insert(core);
        ++core;
      }
    }
    for (auto explicit_thread_affinity_config :
         generated_config.explicit_thread_affinity_param()
             .explicit_thread_affinity_configs()) {
      auto& affinity_set =
          affinity_set_per_follower[explicit_thread_affinity_config
                                        .follower_id()];
      EXPECT_TRUE(
          affinity_set.contains(explicit_thread_affinity_config.cpu_core_id()));
      affinity_set.erase(explicit_thread_affinity_config.cpu_core_id());
    }
    for (const auto& kv : affinity_set_per_follower) {
      EXPECT_TRUE(kv.second.empty());
    }
  }
  EXPECT_EQ(message_size_cnt.size(), 2);
  for (const auto& kv : message_size_cnt) {
    EXPECT_EQ(kv.second, 12 / message_size_cnt.size());
  }
  EXPECT_EQ(op_ratio_cnt.size(), 3);
  for (const auto& kv : op_ratio_cnt) {
    EXPECT_EQ(kv.second, 4);
  }
}

TEST(ExperimentSweepParameterTest, SweepParameterInTemplateLoopback) {
  SetSweepLoopback(true);
  auto config_template = config_utils::GetThroughputConfig();
  auto sweep_parameters = SweepParameterConfig();
  sweep_parameters.add_number_of_threads(2);
  auto sweep_parameter_combinations = GenerateCartesianProductOfSweepParameters(
      GetVerbsmarksSweepParameters(sweep_parameters, 0));

  absl::flat_hash_map<int, int> message_size_cnt;
  absl::flat_hash_map<std::string, int> op_ratio_cnt;
  for (int i = 0; i < sweep_parameter_combinations.size(); ++i) {
    auto generated_config = GenerateConfigFromSweepParameters(
        config_template, sweep_parameter_combinations.at(i));
    EXPECT_EQ(generated_config.traffic_patterns_size(), 2);
    auto traffic_pattern_0 = generated_config.mutable_traffic_patterns(0);
    auto traffic_pattern_1 = generated_config.mutable_traffic_patterns(1);
    traffic_pattern_0->clear_global_traffic_id();
    traffic_pattern_1->clear_global_traffic_id();
    EXPECT_THAT(*traffic_pattern_0, ::testing::EqualsProto(*traffic_pattern_1));
    EXPECT_EQ(traffic_pattern_0->traffic_characteristics().traffic_class(), 7);
    EXPECT_EQ(
        generated_config.traffic_patterns(0).explicit_traffic().flows_size(),
        3);
    // Check message size.
    auto mean = generated_config.traffic_patterns(0)
                    .traffic_characteristics()
                    .size_distribution_params_in_bytes()
                    .mean();
    message_size_cnt[mean]++;
    std::string op_ratio_string = "";
    for (auto op_ratio : generated_config.traffic_patterns(0)
                             .traffic_characteristics()
                             .op_ratio()) {
      std::string append_string;
      google::protobuf::TextFormat::PrintToString(op_ratio, &append_string);
      op_ratio_string += append_string;
    }
    op_ratio_cnt[op_ratio_string]++;
    // Check affinity - loopback affinity should start at 1 and unique.
    absl::flat_hash_set<int> affinity_set;
    absl::flat_hash_set<int> followers;
    for (int follower : generated_config.traffic_patterns(0)
                            .participants()
                            .specific_followers()
                            .participant()) {
      followers.insert(follower);
    }
    int core = 1;
    for (int i = 0; i < followers.size(); ++i) {
      for (const auto traffic : generated_config.traffic_patterns()) {
        affinity_set.insert(core);
        ++core;
      }
    }
    for (auto explicit_thread_affinity_config :
         generated_config.explicit_thread_affinity_param()
             .explicit_thread_affinity_configs()) {
      EXPECT_TRUE(
          affinity_set.contains(explicit_thread_affinity_config.cpu_core_id()));
      affinity_set.erase(explicit_thread_affinity_config.cpu_core_id());
    }
    EXPECT_TRUE(affinity_set.empty());
  }
  EXPECT_EQ(message_size_cnt.size(), 2);
  for (const auto& kv : message_size_cnt) {
    EXPECT_EQ(kv.second, 12 / message_size_cnt.size());
  }
  EXPECT_EQ(op_ratio_cnt.size(), 3);
  for (const auto& kv : op_ratio_cnt) {
    EXPECT_EQ(kv.second, 4);
  }
}

TEST(ExperimentSweepParameterTest, GenerateCorrectExperimentId) {
  auto sweep_parameters = SweepParameterConfig();
  SweepParameters configs = GetVerbsmarksSweepParameters(sweep_parameters, 1);
  // Generate all the combinations from the sweep config.
  SweepParameters sweep_parameter_combinations =
      GenerateCartesianProductOfSweepParameters(configs);
  EXPECT_EQ(sweep_parameter_combinations.size(), 12);
  // Check the name for the first sweep config combination.
  EXPECT_EQ(ConstructExperimentId(sweep_parameter_combinations.front()).first,
            "numberofqps-3_messagesizes-8_write-1_experimenttimeseconds-5_"
            "trafficclass-7");
  // Check the name for the last sweep config combination.
  EXPECT_EQ(ConstructExperimentId(sweep_parameter_combinations.back()).first,
            "numberofqps-3_messagesizes-1024_read-1_write-1_"
            "experimenttimeseconds-100_trafficclass-7");
}

TEST(ExperimentSweepParameterTest, IncastWorks) {
  auto config_template = IncastConfig();
  proto::VerbsmarksSweepParameterConfig sweep_parameters;
  absl::flat_hash_set<int> repeat_set = {2, 5};
  for (const int repeat : repeat_set) {
    sweep_parameters.add_number_of_repeats(repeat);
  }
  auto sweep_parameter_combinations = GenerateCartesianProductOfSweepParameters(
      GetVerbsmarksSweepParameters(sweep_parameters, 0));
  absl::flat_hash_map<std::string, int> op_ratio_cnt;
  for (int i = 0; i < sweep_parameter_combinations.size(); ++i) {
    auto generated_config = GenerateConfigFromSweepParameters(
        config_template, sweep_parameter_combinations.at(i));
    EXPECT_EQ(generated_config.traffic_patterns_size(), 1);
    int repeat = generated_config.traffic_patterns(0)
                     .explicit_traffic()
                     .flows(0)
                     .repeat();
    EXPECT_TRUE(repeat_set.contains(repeat));
    for (const auto flow :
         generated_config.traffic_patterns(0).explicit_traffic().flows()) {
      EXPECT_EQ(flow.repeat(), repeat);
    }
    if (repeat_set.contains(repeat)) {
      repeat_set.erase(repeat);
    }
  }
  EXPECT_TRUE(repeat_set.empty());
}

}  // namespace
}  // namespace verbsmarks
