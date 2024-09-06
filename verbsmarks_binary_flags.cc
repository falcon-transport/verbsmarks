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

#include <cstdint>
#include <string>

#include "absl/flags/flag.h"

ABSL_FLAG(std::string, leader_config_file_path, "",
          "Directory containing LeaderConfig textproto file.");

ABSL_FLAG(std::string, leader_config_file, "",
          "LeaderConfig textproto filename.");

ABSL_FLAG(int32_t, leader_port, 9000,
          "Port used by Verbsmarks leader. Must specify the same port for each "
          "leader and follower instance.");

ABSL_FLAG(int32_t, follower_port, 9001, "Port used by follower instance.");

ABSL_FLAG(std::string, leader_address, "",
          "The ip address/hostname of the leader. Must specify for each leader "
          "and follower instance.");

ABSL_FLAG(std::string, follower_address, "",
          "The ip address/hostname of the follower.");

ABSL_FLAG(std::string, follower_alias, "",
          "The follower alias for debugging purpose.");

ABSL_FLAG(bool, is_leader, false, "Run as leader if set, otherwise follower.");

ABSL_FLAG(std::string, grpc_creds, "GRPC_CRED_INSECURE",
          "The gRPC credentials used for verbsmarks. Must be one of: "
          "GRPC_CRED_SSL, GRPC_CRED_LOCAL, or GRPC_CRED_INSECURE");

ABSL_FLAG(int32_t, qp_memory_space_slots, 1,
          "The total memory space allocated per QP is max_op_size * "
          "qp_memory_space_slots.");

ABSL_FLAG(int32_t, max_dest_rd_atomic, 512,
          "The number of responder resource for reads/atomic at dest (IRD).");

ABSL_FLAG(int32_t, max_rd_atomic, 256,
          "The number outstanding reads/atomics handled at initiator (ORD).");

ABSL_FLAG(std::string, result_file_name, "",
          "Save the results from the followers if specified.");

ABSL_FLAG(std::string, description, "",
          "Information about the experiment. This will be attached to the "
          "saved report.");

ABSL_FLAG(bool, enable_heartbeats, true,
          "Enable follower-leader health checks and stats communication "
          "while experiment is running.");

ABSL_FLAG(std::string, periodic_result_file_name_template, "",
          "Leader periodically writes output files containing follower stats "
          "if specified. Sequence number will be appended to filename or "
          "substituted for $0 if contained in the string. No effect unless "
          "enable_heartbeats is set.");

ABSL_FLAG(bool, ignore_outstanding_ops_at_cutoff, false,
          "Do not error out on any outstanding ops after the hard cutoff time "
          "(potentially benign for openloop)");
