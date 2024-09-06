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

#include "verbsmarks_binary.h"

#include <chrono>
#include <memory>
#include <string>
#include <utility>

#include "absl/flags/flag.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "grpcpp/create_channel.h"
#include "utils.h"
#include "verbsmarks.grpc.pb.h"
#include "verbsmarks.pb.h"
#include "verbsmarks_binary_flags.h"
#include "verbsmarks_follower.h"
#include "verbsmarks_leader.h"
#include "verbsmarks_leader_service.h"

namespace verbsmarks {

absl::Status StartVerbsmarksLeaderService(
    VerbsMarksLeaderService* leader_service) {
  leader_service->Start();
  LOG(INFO) << "Verbsmarks leader starts.";
  while (true) {
    absl::SleepFor(absl::Seconds(5));
    auto status = leader_service->IsFinished();
    if (status.first) {
      LOG(INFO) << "Verbsmarks leader finished.";
      leader_service->Shutdown((absl::Now() + absl::Seconds(2)));
      return status.second;
    }
  }
}

absl::Status VerbsmarksBinary::CreateVerbsmarksLeaderAndStart() {
  auto leader_config_or_error = utils::ReadVerbsmarksLeaderConfig();
  if (!leader_config_or_error.ok()) {
    return leader_config_or_error.status();
  }
  auto leader_impl = std::make_unique<VerbsMarksLeaderImpl>(
      leader_config_or_error.value(), absl::GetFlag(FLAGS_enable_heartbeats));

  leader_service_ = std::make_unique<VerbsMarksLeaderService>(
      verbsmarks_leader_service_creds_, absl::GetFlag(FLAGS_leader_port),
      std::move(leader_impl));
  return StartVerbsmarksLeaderService(leader_service_.get());
}

absl::Status VerbsmarksBinary::CreateVerbsmarksFollowerAndRun(
    absl::string_view device_name) {
  // Creates stub to the leader.
  const std::string target = utils::HostPortString(
      absl::GetFlag(FLAGS_leader_address), absl::GetFlag(FLAGS_leader_port));
  auto channel = grpc::CreateChannel(target, verbsmarks_follower_client_creds_);
  auto stub_to_leader = proto::VerbsMarks::NewStub(channel);
  if (!channel->WaitForConnected(std::chrono::system_clock::now() +
                                 std::chrono::seconds(180))) {
    LOG(FATAL) << "Leader channel failed to become ready";
  }

  // Initializes follower.
  auto follower = std::make_unique<VerbsMarksFollower>(
      verbsmarks_follower_service_creds_, absl::GetFlag(FLAGS_follower_port),
      absl::GetFlag(FLAGS_follower_address),
      absl::GetFlag(FLAGS_follower_alias), stub_to_leader.get(), device_name);

  // Runs follower in the scope of valid server creds.
  LOG(INFO) << "Verbsmarks follower starts.";
  return follower->Run();
}

}  // namespace verbsmarks
