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

#ifndef VERBSMARKS_VERBSMARKS_BINARY_H_
#define VERBSMARKS_VERBSMARKS_BINARY_H_

#include <memory>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "grpcpp/security/credentials.h"
#include "grpcpp/security/server_credentials.h"
#include "verbsmarks.pb.h"
#include "verbsmarks_leader_service.h"

namespace verbsmarks {

// Start the leader service.
absl::Status StartVerbsmarksLeaderService(
    VerbsMarksLeaderService* leader_service);

class VerbsmarksBinary {
 public:
  VerbsmarksBinary(
      std::shared_ptr<grpc::ServerCredentials> leader_creds,
      std::shared_ptr<grpc::ServerCredentials> follower_creds,
      std::shared_ptr<grpc::ChannelCredentials> follower_client_creds)
      : verbsmarks_leader_service_creds_(leader_creds),
        verbsmarks_follower_service_creds_(follower_creds),
        verbsmarks_follower_client_creds_(follower_client_creds) {}

  virtual ~VerbsmarksBinary() = default;

  // Create a verbsmarks leader and start its service.
  virtual absl::Status CreateVerbsmarksLeaderAndStart();
  // Create verbsmarks follower and run it.
  virtual absl::Status CreateVerbsmarksFollowerAndRun(
      absl::string_view device_name);

 protected:
  const std::shared_ptr<grpc::ServerCredentials>
      verbsmarks_leader_service_creds_;
  const std::shared_ptr<grpc::ServerCredentials>
      verbsmarks_follower_service_creds_;
  const std::shared_ptr<grpc::ChannelCredentials>
      verbsmarks_follower_client_creds_;
  std::unique_ptr<VerbsMarksLeaderService> leader_service_;
};

}  // namespace verbsmarks

#endif  // VERBSMARKS_VERBSMARKS_BINARY_H_
