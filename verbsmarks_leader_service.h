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

#ifndef VERBSMARKS_VERBSMARKS_LEADER_SERVICE_H_
#define VERBSMARKS_VERBSMARKS_LEADER_SERVICE_H_

#include <memory>
#include <optional>
#include <thread>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "absl/synchronization/notification.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "grpcpp/completion_queue.h"
#include "grpcpp/security/server_credentials.h"
#include "grpcpp/server.h"
#include "grpcpp/server_builder.h"
#include "grpcpp/server_context.h"
#include "grpcpp/support/async_unary_call.h"
#include "verbsmarks.grpc.pb.h"
#include "verbsmarks.pb.h"
#include "verbsmarks_leader.h"

namespace verbsmarks {
// VerbsMarksLeaderService handles all boilerplate for the gRPC service and
// interacts with a VerbsMarksLeader class which handles the main logic of
// VerbsMarks leader.
class VerbsMarksLeaderService
    : public verbsmarks::proto::VerbsMarks::AsyncService {
  friend class VerbsMarksTest;

 public:
  VerbsMarksLeaderService(
      const std::shared_ptr<grpc::ServerCredentials>& credentials, int port,
      std::unique_ptr<VerbsMarksLeaderInterface> leader);

  // Shuts the server down. Uses deadline for graceful shutdown if provided.
  void Shutdown(const absl::Time deadline = absl::Now())
      ABSL_LOCKS_EXCLUDED(server_shutdown_mutex_);

  // Builds and starts the server, and start the completion queue. This call is
  // non-blocking.
  //
  void Start();

  // Returns true if the leader is finished a cycle of experiment.
  std::pair<bool, absl::Status> IsFinished() { return leader_->IsFinished(); }

 private:
  // gRPC completion queue requires the server to provision resources and bind
  // handlers, and explicitly request the gRPC runtime to send RPCs. Here are
  // methods to support the process from provisioning to cleanup. Other than the
  // method required to be requested and the function that the leader needs to
  // execute, all the messages require the same process. Therefore we provide
  // templatized methods.

  //  - PrepareToAcceptRequest: provisions resource and request an RPC to gRPC
  //  runtime. It calls RequestRpc which will request different RPC depending on
  //  the message type and register ProcessResponse to handle the response once
  //  ready.
  //  - ProcessRequest: once a request is received, prepares for another request
  //  then calls ExecuteByLeader to invoke an appropriate method on the leader
  //  based on the message type.
  // - ProcessResponse: finish the responder and register a cleanup call back.
  // - CleanupAfterRequest: a callback to clean up the resource when a request
  // is completely finished (success or failure).

  // Provosions resources and request an RPC to gRPC runtime. Internally calls
  // RequestRPC.
  template <typename request_type, typename response_type>
  void PrepareToAcceptRequest() ABSL_LOCKS_EXCLUDED(server_shutdown_mutex_);

  // Request an appropriate RCP call based on the request type. Specialized
  // methods are defined in the cc file.
  template <typename request_type, typename response_type>
  inline void RequestRpc(
      grpc::ServerContext* context, request_type* request,
      grpc::ServerAsyncResponseWriter<response_type>* responder,
      grpc::CompletionQueue* new_call_cq,
      grpc::ServerCompletionQueue* notification_cq, void* tag);

  // Called when a request is received. Calls PrepareToAcceptRequest to allow a
  // next request coming in and calls ExecuteByLeader.
  template <typename request_type, typename response_type>
  void ProcessRequest(grpc::ServerContext* ctx, request_type* request,
                      grpc::ServerAsyncResponseWriter<response_type>* responder,
                      bool ok);

  // Invokes an apporppriate method of the leader, based on the request type.
  // Specialized methods are in the cc file.
  template <typename request_type, typename response_type>
  inline void ExecuteByLeader(grpc::ServerContext* context, request_type*,
                              grpc::ServerAsyncResponseWriter<response_type>*);

  // Upon receiving a response, finish the responder and pass a clean up call
  // back.
  template <typename request_type, typename response_type>
  void ProcessResponse(
      grpc::ServerContext* ctx, request_type* request,
      grpc::ServerAsyncResponseWriter<response_type>* responder,
      response_type response) ABSL_LOCKS_EXCLUDED(server_shutdown_mutex_);

  // Cleans up request and responder after finished.
  template <typename request_type, typename response_type>
  void CleanupAfterRequest(
      grpc::ServerContext* ctx, request_type* request,
      grpc::ServerAsyncResponseWriter<response_type>* responder,
      bool ignored_ok) ABSL_LOCKS_EXCLUDED(server_shutdown_mutex_);

  verbsmarks::proto::VerbsMarks::AsyncService service_;
  const int port_;
  std::optional<std::thread> server_thread_;

  absl::Mutex server_shutdown_mutex_;
  bool server_shutdown_ ABSL_GUARDED_BY(server_shutdown_mutex_);
  std::unique_ptr<grpc::Server> server_;
  std::unique_ptr<grpc::ServerCompletionQueue> cq_;
  absl::Notification rpcs_completed_;
  int rpcs_pending_ ABSL_GUARDED_BY(server_shutdown_mutex_);

  // The verbsmarks leader object which implements the actual logic of the
  // verbsmarks leader.
  std::unique_ptr<VerbsMarksLeaderInterface> leader_;
  const std::shared_ptr<grpc::ServerCredentials>& credentials_;
};

}  // namespace verbsmarks

#endif  // VERBSMARKS_VERBSMARKS_LEADER_SERVICE_H_
