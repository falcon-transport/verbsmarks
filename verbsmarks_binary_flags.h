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

#ifndef VERBSMARKS_VERBSMARKS_BINARY_FLAGS_H_
#define VERBSMARKS_VERBSMARKS_BINARY_FLAGS_H_

#include <cstdint>
#include <string>

#include "absl/flags/declare.h"

ABSL_DECLARE_FLAG(std::string, leader_config_file_path);
ABSL_DECLARE_FLAG(std::string, leader_config_file);
ABSL_DECLARE_FLAG(int32_t, leader_port);
ABSL_DECLARE_FLAG(int32_t, follower_port);
ABSL_DECLARE_FLAG(std::string, leader_address);
ABSL_DECLARE_FLAG(std::string, follower_address);
ABSL_DECLARE_FLAG(std::string, follower_alias);
ABSL_DECLARE_FLAG(bool, is_leader);
ABSL_DECLARE_FLAG(std::string, grpc_creds);
ABSL_DECLARE_FLAG(int32_t, qp_memory_space_slots);
ABSL_DECLARE_FLAG(int32_t, max_dest_rd_atomic);
ABSL_DECLARE_FLAG(int32_t, max_rd_atomic);
ABSL_DECLARE_FLAG(std::string, result_file_name);
ABSL_DECLARE_FLAG(std::string, description);
ABSL_DECLARE_FLAG(bool, enable_heartbeats);
ABSL_DECLARE_FLAG(std::string, periodic_result_file_name_template);
ABSL_DECLARE_FLAG(bool, ignore_outstanding_ops_at_cutoff);

#endif  // VERBSMARKS_VERBSMARKS_BINARY_FLAGS_H_
