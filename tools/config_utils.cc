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

#include "tools/config_utils.h"

#include <stdint.h>

#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/log/log.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/substitute.h"
#include "google/protobuf/text_format.h"
#include "verbsmarks.pb.h"

namespace verbsmarks {
namespace config_utils {

proto::LeaderConfig GetThroughputConfig() {
  proto::LeaderConfig leader_config;

  std::string base_config_textproto = R"pb(
    group_size: 2
    quorum_time_out_sec: 30
    traffic_patterns: {
      global_traffic_id: 1
      participants: { specific_followers { participant: 0 participant: 1 } }
      connection_type: CONNECTION_TYPE_RC
      open_loop_parameters {
        offered_load_gbps: 0.0003
        arrival_time_distribution_type: ARRIVAL_TIME_DISTRIBUTION_FIXED
      }
      traffic_characteristics {
        op_ratio: { op_code: RDMA_OP_READ ratio: 1 }
        message_size_distribution_type: MESSAGE_SIZE_DISTRIBUTION_FIXED
        size_distribution_params_in_bytes: { mean: 8 }
        delay_time: { seconds: 0 }
        buffer_time: { seconds: 0 }
        experiment_time: { seconds: 10 }
        batch_size: 1
      }
      explicit_traffic: {
        flows: { initiator: 0 target: 1 }
        bidirectional: false
      }
    }
    thread_affinity_type: THREAD_AFFINITY_PER_TRAFFIC_PATTERN
  )pb";

  if (!google::protobuf::TextFormat::ParseFromString(base_config_textproto,
                                                     &leader_config)) {
    LOG(FATAL) << "Failed to LeaderConfig textproto";
  }
  return leader_config;
}

proto::LeaderConfig GetThroughputConfigWithSize(double op_bytes) {
  proto::LeaderConfig leader_config = GetThroughputConfig();
  for (int i = 0; i < leader_config.traffic_patterns_size(); ++i) {
    leader_config.mutable_traffic_patterns(i)
        ->mutable_traffic_characteristics()
        ->mutable_size_distribution_params_in_bytes()
        ->set_mean(op_bytes);
    leader_config.mutable_traffic_patterns(i)
        ->mutable_traffic_characteristics()
        ->mutable_size_distribution_params_in_bytes()
        ->set_min(op_bytes);
    leader_config.mutable_traffic_patterns(i)
        ->mutable_traffic_characteristics()
        ->mutable_size_distribution_params_in_bytes()
        ->set_max(op_bytes);
  }
  return leader_config;
}

void ModifyInlineThreshold(int inline_threshold, proto::LeaderConfig& config) {
  for (int i = 0; i < config.traffic_patterns_size(); ++i) {
    config.mutable_traffic_patterns(i)
        ->mutable_traffic_characteristics()
        ->set_inline_threshold(inline_threshold);
  }
}

proto::LeaderConfig GetConfigWithNumThreads(const int num_threads,
                                            proto::LeaderConfig config) {
  int num_existing_threads = config.traffic_patterns_size();
  if (num_threads <= 0 || num_existing_threads == 0) {
    return config;
  }

  if (num_existing_threads > num_threads) {
    config.mutable_traffic_patterns()->DeleteSubrange(
        num_threads, num_existing_threads - num_threads);
  } else {
    while (num_existing_threads < num_threads) {
      const int32_t id =
          config.traffic_patterns(config.traffic_patterns_size() - 1)
              .global_traffic_id() +
          1;
      verbsmarks::proto::GlobalTrafficPattern* new_traffic_pattern =
          config.add_traffic_patterns();
      *new_traffic_pattern = config.traffic_patterns(0);
      new_traffic_pattern->set_global_traffic_id(id);
      ++num_existing_threads;
    }
  }
  return config;
}

void ApplyOpRatio(double write_ratio, double read_ratio, double sendrecv_ratio,
                  double write_imm_ratio, double sendrecv_imm_ratio,
                  proto::TrafficCharacteristics* characteristics) {
  characteristics->clear_op_ratio();
  auto apply = [&characteristics](auto op_code, double ratio) {
    auto op_ratio = characteristics->add_op_ratio();
    op_ratio->set_op_code(op_code);
    op_ratio->set_ratio(ratio);
  };

  if (write_ratio > 0) {
    apply(::verbsmarks::proto::RDMA_OP_WRITE, write_ratio);
  }
  if (read_ratio > 0) {
    apply(::verbsmarks::proto::RDMA_OP_READ, read_ratio);
  }
  if (sendrecv_ratio > 0) {
    apply(::verbsmarks::proto::RDMA_OP_SEND_RECEIVE, sendrecv_ratio);
  }
  if (write_imm_ratio > 0) {
    apply(::verbsmarks::proto::RDMA_OP_WRITE_IMMEDIATE, write_imm_ratio);
  }
  if (sendrecv_imm_ratio > 0) {
    apply(::verbsmarks::proto::RDMA_OP_SEND_WITH_IMMEDIATE_RECEIVE,
          sendrecv_imm_ratio);
  }
}

void ApplyQpNum(proto::ExplicitTraffic* explicit_traffic, int qp_num) {
  // Apply QP num as needed.
  auto flow = explicit_traffic->flows(0);
  explicit_traffic->clear_flows();
  for (int i = 0; i < qp_num; ++i) {
    auto* new_flow = explicit_traffic->add_flows();
    new_flow->set_initiator(flow.initiator());
    new_flow->set_target(flow.target());
  }
}

void ApplyRepeat(proto::ExplicitTraffic* explicit_traffic, int repeat) {
  for (int i = 0; i < explicit_traffic->flows_size(); ++i) {
    explicit_traffic->mutable_flows(i)->set_repeat(repeat);
  }
}

void MakeConfig(
    proto::LeaderConfig& base_config, int traffic_pattern_cnt, int total_qp_num,
    int message_size, double write_ratio, double read_ratio,
    double sendrecv_ratio, double write_imm_ratio, double sendrecv_imm_ratio,
    double open_loop_load_gbps, int closed_loop_outstanding, bool bidirectional,
    int inline_threshold,
    absl::flat_hash_map<std::string, proto::LeaderConfig>& config_map) {
  std::string load = "closedloop";
  int traffic_pattern_cnt_to_use = traffic_pattern_cnt;
  LOG(INFO) << "traffic_pattern_cnt_to_use: " << traffic_pattern_cnt_to_use
            << " total_qp_num: " << total_qp_num;
  if (total_qp_num < traffic_pattern_cnt_to_use) {
    // No need to make a traffic pattern with empty QP.
    traffic_pattern_cnt_to_use = total_qp_num;
  }
  if (closed_loop_outstanding != 0) {
    base_config.mutable_traffic_patterns(0)->set_closed_loop_max_outstanding(
        closed_loop_outstanding);
  } else {
    double open_loop_load_gbps_per_traffic_pattern =
        open_loop_load_gbps / traffic_pattern_cnt_to_use;
    base_config.mutable_traffic_patterns(0)
        ->mutable_open_loop_parameters()
        ->set_offered_load_gbps(open_loop_load_gbps_per_traffic_pattern);
    load = "open_loop";
  }
  // Change the traffic pattern count as requested.
  proto::LeaderConfig config =
      GetConfigWithNumThreads(traffic_pattern_cnt_to_use, base_config);

  // Distribute the total QPs across traffic patterns evenly.
  int qp_num_per_traffic_pattern = total_qp_num / traffic_pattern_cnt_to_use;
  int qp_left_over =
      total_qp_num - (qp_num_per_traffic_pattern * traffic_pattern_cnt_to_use);
  LOG(INFO) << "qp_num_per_traffic_pattern: " << qp_num_per_traffic_pattern;
  LOG(INFO) << "qp_left_over: " << qp_left_over;

  // Apply OP ratio as needed.
  for (auto& traffic_pattern : *config.mutable_traffic_patterns()) {
    // Apply OP ratio.
    auto* characteristics = traffic_pattern.mutable_traffic_characteristics();
    verbsmarks::config_utils::ApplyOpRatio(write_ratio, read_ratio,
                                           sendrecv_ratio, write_imm_ratio,
                                           sendrecv_imm_ratio, characteristics);

    int additional_qp = 0;
    if (qp_left_over > 0) {
      --qp_left_over;
      additional_qp = 1;
    }
    // Apply QP num as needed.
    verbsmarks::config_utils::ApplyQpNum(
        traffic_pattern.mutable_explicit_traffic(),
        qp_num_per_traffic_pattern + additional_qp);

    // Apply uni/bidirection.
    traffic_pattern.mutable_explicit_traffic()->set_bidirectional(
        bidirectional);
  }
  std::string op_string = "";
  if (write_ratio > 0) {
    absl::StrAppend(&op_string, "_W", write_ratio);
  }
  if (read_ratio > 0) {
    absl::StrAppend(&op_string, "_R", read_ratio);
  }
  if (sendrecv_ratio > 0) {
    absl::StrAppend(&op_string, "_SR", sendrecv_ratio);
  }
  if (write_imm_ratio > 0) {
    absl::StrAppend(&op_string, "_WIMM", write_imm_ratio);
  }
  if (sendrecv_imm_ratio > 0) {
    absl::StrAppend(&op_string, "_SRIMM", sendrecv_imm_ratio);
  }

  auto filename =
      absl::Substitute("config_$0_TPCNT$1_QP$2_MSGSIZE$3_BIDI$4_$5.textpb",
                       op_string, traffic_pattern_cnt_to_use, total_qp_num,
                       message_size, bidirectional, load);
  LOG(INFO) << filename << config.traffic_patterns(0).traffic_characteristics();
  config_map[filename] = std::move(config);
}

absl::flat_hash_map<std::string, proto::LeaderConfig> GetConfigMap(
    const std::vector<int>& message_sizes,
    const std::vector<int>& qp_nums_to_try, int traffic_pattern_cnt,
    double write_ratio, double read_ratio, double sendrecv_ratio,
    double write_imm_ratio, double sendrecv_imm_ratio,
    double open_loop_load_gbps, int closed_loop_outstanding, bool bidirectional,
    int inline_threshold, bool multiple_op) {
  absl::flat_hash_map<std::string, proto::LeaderConfig> config_map;

  // Vary by message size if needed.
  for (auto message_size : message_sizes) {
    proto::LeaderConfig config =
        config_utils::GetThroughputConfigWithSize(message_size);
    config_utils::ModifyInlineThreshold(inline_threshold, config);

    for (auto total_qp_num : qp_nums_to_try) {
      if (multiple_op) {
        MakeConfig(config, traffic_pattern_cnt, total_qp_num, message_size,
                   write_ratio, read_ratio, sendrecv_ratio, write_imm_ratio,
                   sendrecv_imm_ratio, open_loop_load_gbps,
                   closed_loop_outstanding, bidirectional, inline_threshold,
                   config_map);
      } else {
        if (write_ratio > 0) {
          LOG(INFO) << "WRITE";
          MakeConfig(config, traffic_pattern_cnt, total_qp_num, message_size,
                     write_ratio, 0, 0, 0, 0, open_loop_load_gbps,
                     closed_loop_outstanding, bidirectional, inline_threshold,
                     config_map);
        }
        if (read_ratio > 0) {
          LOG(INFO) << "READ";
          MakeConfig(config, traffic_pattern_cnt, total_qp_num, message_size, 0,
                     read_ratio, 0, 0, 0, open_loop_load_gbps,
                     closed_loop_outstanding, bidirectional, inline_threshold,
                     config_map);
        }
        if (sendrecv_ratio > 0) {
          LOG(INFO) << "SR";
          MakeConfig(config, traffic_pattern_cnt, total_qp_num, message_size, 0,
                     0, sendrecv_ratio, 0, 0, open_loop_load_gbps,
                     closed_loop_outstanding, bidirectional, inline_threshold,
                     config_map);
        }
        if (write_imm_ratio > 0) {
          LOG(INFO) << "WRITE IMM";
          MakeConfig(config, traffic_pattern_cnt, total_qp_num, message_size, 0,
                     0, 0, write_imm_ratio, 0, open_loop_load_gbps,
                     closed_loop_outstanding, bidirectional, inline_threshold,
                     config_map);
        }
        if (sendrecv_imm_ratio > 0) {
          LOG(INFO) << "SEND IMM";
          MakeConfig(config, traffic_pattern_cnt, total_qp_num, message_size, 0,
                     0, 0, 0, sendrecv_imm_ratio, open_loop_load_gbps,
                     closed_loop_outstanding, bidirectional, inline_threshold,
                     config_map);
        }
      }
    }
  }
  return config_map;
}

}  // namespace config_utils
}  // namespace verbsmarks
