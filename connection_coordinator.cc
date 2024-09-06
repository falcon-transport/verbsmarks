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

#include "connection_coordinator.h"

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/log/log.h"
#include "absl/meta/type_traits.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "connection_coordinator.grpc.pb.h"
#include "connection_coordinator.pb.h"
#include "grpcpp/client_context.h"
#include "grpcpp/create_channel.h"
#include "grpcpp/security/credentials.h"
#include "grpcpp/security/server_credentials.h"
#include "grpcpp/server.h"
#include "grpcpp/server_builder.h"
#include "grpcpp/support/status.h"
#include "utils.h"
#include "verbsmarks.pb.h"

namespace verbsmarks {

ConnectionCoordinatorServer::ConnectionCoordinatorServer(
    const std::shared_ptr<grpc::ServerCredentials> &credentials,
    const absl::string_view address,
    std::unique_ptr<proto::ConnectionCoordinator::Service> service)
    : address_(address),
      credentials_(credentials),
      service_(std::move(service)) {}

ConnectionCoordinatorServer::~ConnectionCoordinatorServer() {
  if (server_ != nullptr) {
    server_->Shutdown();
  }
  if (server_thread_.has_value()) {
    server_thread_->join();
  }
}

void ConnectionCoordinatorServer::RunInThread() {
  // If a server is already running, do nothing, as there is no need to start
  // another.
  if (server_ != nullptr) {
    return;
  }

  // Build and start the server. Do this on the main thread to avoid races with
  // shutdown.
  grpc::ServerBuilder builder;
  builder.AddListeningPort(address_, std::move(credentials_));
  builder.RegisterService(service_.get());
  server_ = builder.BuildAndStart();
  LOG(INFO) << "ConnectionCoordinator server listening on " << address_;

  // Run the server on a separate thread. Wait will return when `Shutdown` is
  // called from the destructor. If `Shutdown` is called before `Wait` starts
  // running on its own thread, `Wait` will return immediately.
  server_thread_.emplace(&grpc::Server::Wait, server_.get());
}

ConnectionCoordinatorClient::ConnectionCoordinatorClient(
    absl::string_view server_address,
    std::shared_ptr<grpc::ChannelCredentials> creds)
    : stub_(proto::ConnectionCoordinator::NewStub(
          grpc::CreateChannel(std::string(server_address), creds))) {}

grpc::Status ConnectionCoordinatorClient::GetQueuePairAttributes(
    const proto::QueuePairAttributesRequest &request,
    proto::QueuePairAttributesResponse &response) {
  grpc::ClientContext context;
  // Timeout if can not connect to the peer in 5s. It is likely that the peer
  // is down due to some error.
  context.set_deadline(std::chrono::system_clock::now() +
                       utils::kGrpcRequestTimeout);
  return stub_->GetQueuePairAttributes(&context, request, &response);
}

absl::StatusOr<const proto::QueuePairAttributesResponse>
RemoteQueuePairAttributesFetcher::GetQueuePairAttributes(
    const int32_t peer_id, const proto::QueuePairAttributesRequest &request,
    std::shared_ptr<grpc::ChannelCredentials> creds) {
  auto follower_it = followers_.find(peer_id);
  if (follower_it == followers_.end()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Peer id ", peer_id, " does not correspond to a valid follower."));
  }

  ConnectionCoordinatorClient *client;
  {
    absl::MutexLock lock(&map_mutex_);
    auto it = clients_.try_emplace(
        peer_id, ConnectionCoordinatorClient(
                     follower_it->second.http_server_address(), creds));
    client = &it.first->second;
  }

  proto::QueuePairAttributesResponse response;
  grpc::Status status = client->GetQueuePairAttributes(request, response);
  if (!status.ok()) {
    return absl::Status(static_cast<absl::StatusCode>(status.error_code()),
                        status.error_message());
    ;
  }
  return response;
}

}  // namespace verbsmarks
