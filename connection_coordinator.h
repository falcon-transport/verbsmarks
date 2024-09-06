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

#ifndef VERBSMARKS_CONNECTION_COORDINATOR_H_
#define VERBSMARKS_CONNECTION_COORDINATOR_H_

#include <stdint.h>

#include <memory>
#include <optional>
#include <string>
#include <thread>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "connection_coordinator.grpc.pb.h"
#include "connection_coordinator.pb.h"
#include "grpcpp/completion_queue.h"
#include "grpcpp/security/credentials.h"
#include "grpcpp/server_interface.h"
#include "grpcpp/support/status.h"
#include "verbsmarks.pb.h"

namespace verbsmarks {

// The ConnectionCoordinatorServer can create a gRPC service that listens at
// the specified address to respond to requests.
class ConnectionCoordinatorServer {
 public:
  ConnectionCoordinatorServer(
      const std::shared_ptr<grpc::ServerCredentials> &credentials,
      absl::string_view address,
      std::unique_ptr<proto::ConnectionCoordinator::Service> service);

  // If a server is running, shuts down the server and joins the thread that it
  // is running on.
  ~ConnectionCoordinatorServer();

  // Runs a gRPC server with the implementation passed to the constructor. The
  // server is run on a separate thread. If a server is already running, does
  // nothing.
  void RunInThread();

  // ConnectionCoordinatorServer is moveable but not copyable
  ConnectionCoordinatorServer(ConnectionCoordinatorServer &&other) = default;
  ConnectionCoordinatorServer &operator=(ConnectionCoordinatorServer &&other) =
      default;

 private:
  const std::string address_;
  const std::shared_ptr<grpc::ServerCredentials> credentials_;
  const std::unique_ptr<proto::ConnectionCoordinator::Service> service_;
  std::unique_ptr<grpc::Server> server_;
  std::optional<std::thread> server_thread_;
};

// ConnectionCoordinatorClient opens a connection to a server to which it can
// issue GetQueuePairAttributes requests.
class ConnectionCoordinatorClient {
 public:
  // Opens a connection to a server at the provided address with the given
  // creds.
  explicit ConnectionCoordinatorClient(
      absl::string_view server_address,
      std::shared_ptr<grpc::ChannelCredentials> creds);

  // Issues a gRPC request to the server. When the method returns, `response`
  // will be populated with the response from the server. The status is returned
  // from the server.
  grpc::Status GetQueuePairAttributes(
      const proto::QueuePairAttributesRequest &request,
      proto::QueuePairAttributesResponse &response);

  // ConnectionCoordinatorClient is moveable but not copyable
  ConnectionCoordinatorClient(ConnectionCoordinatorClient &&other) = default;
  ConnectionCoordinatorClient &operator=(ConnectionCoordinatorClient &&other) =
      default;

 private:
  std::unique_ptr<proto::ConnectionCoordinator::Stub> stub_;
};

// Dispatches requests for queue pair attributes to the correct gRPC channel.
// This class makes it so that there is single, lazily initialized, network
// stub/channel to each peer, regardless of how many queue pairs need to be
// connected to that peer.
class RemoteQueuePairAttributesFetcher {
 public:
  explicit RemoteQueuePairAttributesFetcher(
      const absl::flat_hash_map<int32_t, proto::Follower> *followers)
      : followers_(*followers) {}

  // Fetches queue pair attributes from the provided peer, using `request`. This
  // method blocks on network communication. Returns an error if the peer does
  // not exist or if the request fails.
  absl::StatusOr<const proto::QueuePairAttributesResponse>
  GetQueuePairAttributes(int32_t peer_id,
                         const proto::QueuePairAttributesRequest &request,
                         std::shared_ptr<grpc::ChannelCredentials> creds);

  // RemoteQueuePairAttributesFetcher is moveable but not copyable
  RemoteQueuePairAttributesFetcher(RemoteQueuePairAttributesFetcher &&other) =
      default;
  RemoteQueuePairAttributesFetcher &operator=(
      RemoteQueuePairAttributesFetcher &&other) = default;

 private:
  const absl::flat_hash_map<int32_t, proto::Follower> &followers_;

  absl::Mutex map_mutex_;
  // Map from peer id to connection.
  absl::flat_hash_map<int32_t, ConnectionCoordinatorClient> clients_
      ABSL_GUARDED_BY(map_mutex_);
};

}  // namespace verbsmarks

#endif  // VERBSMARKS_CONNECTION_COORDINATOR_H_
