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

#include <iostream>
#include <string>
#include <thread>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/log/log.h"
#include "ibverbs_utils.h"
#include "utils.h"
#include "verbsmarks_binary.h"
#include "verbsmarks_binary_flags.h"

ABSL_FLAG(std::string, device_name, "", "RDMA device name");

int Run(verbsmarks::VerbsmarksBinary* const verbsmarks_binary) {
  if (absl::GetFlag(FLAGS_is_leader)) {
    // Runs as a leader.
    auto status = verbsmarks_binary->CreateVerbsmarksLeaderAndStart();
    if (!status.ok()) {
      std::cout << "Verbsmarks leader fails: " << status << '\n';
      LOG(ERROR) << "Verbsmarks leader fails: " << status;
      return -1;
    }
  } else {
    auto device_name = absl::GetFlag(FLAGS_device_name);
    // Create a separate thread to handle AEs.
    std::thread async_event_handler(
        verbsmarks::ibverbs_utils::WaitAndAckAsyncEvents, device_name);
    async_event_handler.detach();

    // Runs as a follower.
    auto status =
        verbsmarks_binary->CreateVerbsmarksFollowerAndRun(device_name);
    if (!status.ok()) {
      std::cout << "Verbsmarks follower fails: " << status << '\n';
      LOG(ERROR) << "verbsmarks follower fails: " << status;
      return -1;
    }
  }
  return 0;
}

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  verbsmarks::VerbsmarksBinary verbsmarks_binary(
      /*leader_creds=*/verbsmarks::utils::GetGrpcServerCredentials(),
      /*follower_creds=*/verbsmarks::utils::GetGrpcServerCredentials(),
      /*follower_client_creds=*/
      verbsmarks::utils::GetGrpcChannelCredentials());
  return Run(&verbsmarks_binary);
}
