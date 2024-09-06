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

#include "verbsmarks_leader_service.h"

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/flags/flag.h"
#include "absl/functional/bind_front.h"
#include "absl/log/log.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/synchronization/notification.h"
#include "absl/time/time.h"
#include "grpc/support/time.h"
#include "grpcpp/completion_queue.h"
#include "grpcpp/security/server_credentials.h"
#include "grpcpp/server.h"
#include "grpcpp/server_builder.h"
#include "grpcpp/server_context.h"
#include "grpcpp/support/async_unary_call.h"
#include "grpcpp/support/status.h"
#include "grpcpp/support/time.h"
#include "verbsmarks.grpc.pb.h"
#include "verbsmarks.pb.h"
#include "verbsmarks_leader.h"

ABSL_FLAG(std::string, ip, "[::]", "server ip");
ABSL_FLAG(absl::Duration, graceful_termination_timeout, absl::Seconds(2),
          "If the leader finished experiments, we terminate the server after "
          "this timeout.");

namespace verbsmarks {

VerbsMarksLeaderService::VerbsMarksLeaderService(
    const std::shared_ptr<grpc::ServerCredentials>& credentials, int port,
    std::unique_ptr<VerbsMarksLeaderInterface> leader)
    : port_(port),
      server_shutdown_(false),
      rpcs_pending_(0),
      leader_(std::move(leader)),
      credentials_(credentials) {}

void VerbsMarksLeaderService::Shutdown(const absl::Time deadline) {
  {
    absl::MutexLock lock(&server_shutdown_mutex_);
    if (server_shutdown_) {
      return;
    }
    server_shutdown_ = true;
  }

  // Only shutdown if we've started the server.
  if (server_ != nullptr) {
    server_->Shutdown(
        gpr_time_from_nanos(absl::ToUnixNanos(deadline), GPR_CLOCK_MONOTONIC));
    rpcs_completed_.WaitForNotificationWithTimeout(absl::Seconds(2));
    cq_->Shutdown();
    server_thread_->join();
  }
}

void VerbsMarksLeaderService::Start() {
  // If a server is already running, do nothing, as there is no need to start
  // another.
  if (server_ != nullptr) {
    return;
  }

  // Start the server.
  std::string server_address =
      absl::StrCat(absl::GetFlag(FLAGS_ip), ":", port_);

  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, credentials_);
  builder.RegisterService(&service_);

  cq_ = builder.AddCompletionQueue();
  server_ = builder.BuildAndStart();

  LOG(INFO) << "Server started: " << server_address;

  // Bind handlers and request Quorum RPC.
  PrepareToAcceptRequest<proto::QuorumRequest, proto::QuorumResponse>();
  PrepareToAcceptRequest<proto::ReadyRequest, proto::ReadyResponse>();
  PrepareToAcceptRequest<proto::StartExperimentRequest,
                         proto::StartExperimentResponse>();
  PrepareToAcceptRequest<proto::ResultRequest, proto::ResultResponse>();
  PrepareToAcceptRequest<proto::HeartbeatRequest, proto::HeartbeatResponse>();

  // Proceed to the server's main loop.
  server_thread_.emplace([this]() {
    bool ok = true;
    // Need to drain the CQ when ok == false but cq_->Next returns true.
    while (true) {
      void* tag;
      if (!cq_->Next(&tag, &ok)) {
        LOG(INFO) << "Completion queue shutdown.";
        return;
      }
      std::unique_ptr<std::function<void(bool)>> func_ptr(
          static_cast<std::function<void(bool)>*>(tag));
      (*func_ptr)(ok);
    }
  });
}

template <typename request_type, typename response_type>
void VerbsMarksLeaderService::CleanupAfterRequest(
    grpc::ServerContext* ctx, request_type* request,
    grpc::ServerAsyncResponseWriter<response_type>* responder,
    ABSL_ATTRIBUTE_UNUSED bool ok) {
  // clean up used data
  delete request;
  delete responder;
  delete ctx;
  // If there are now no RPCs pending after this RPC is finished, then the
  // server is fully shutdown because we would have already requested a new RPC
  // before finishing this one if the server were not shutdown. That invariant
  // makes sure that rpcs_pending_ hits 0 exactly once.
  {
    absl::MutexLock lock(&server_shutdown_mutex_);
    --rpcs_pending_;
    if (rpcs_pending_ == 0) {
      // Ignore mutants, this only prevents racy shutdown.
      rpcs_completed_.Notify();
    }
  }
}

template <typename request_type, typename response_type>
void VerbsMarksLeaderService::PrepareToAcceptRequest() {
  {
    absl::MutexLock lock(&server_shutdown_mutex_);
    if (server_shutdown_) {
      LOG(INFO) << "Server shutdown, do not request any more.";
      return;
    }
    ++rpcs_pending_;
  }
  grpc::ServerContext* ctx = new grpc::ServerContext();
  request_type* request = new request_type();
  grpc::ServerAsyncResponseWriter<response_type>* responder =
      new grpc::ServerAsyncResponseWriter<response_type>(ctx);
  auto process_callback = new std::function<void(bool)>(absl::bind_front(
      &VerbsMarksLeaderService::ProcessRequest<request_type, response_type>,
      this, ctx, request, responder));
  RequestRpc(ctx, request, responder, cq_.get(), cq_.get(), process_callback);
}

template <typename request_type, typename response_type>
void VerbsMarksLeaderService::ProcessResponse(
    grpc::ServerContext* ctx, request_type* request,
    grpc::ServerAsyncResponseWriter<response_type>* responder,
    response_type response) {
  auto finish_get_quorum_callback =
      new std::function<void(bool)>(absl::bind_front(
          &VerbsMarksLeaderService::CleanupAfterRequest<request_type,
                                                        response_type>,
          this, ctx, request, responder));
  responder->Finish(response, grpc::Status::OK, finish_get_quorum_callback);
}

template <typename request_type, typename response_type>
void VerbsMarksLeaderService::ProcessRequest(
    grpc::ServerContext* ctx, request_type* request,
    grpc::ServerAsyncResponseWriter<response_type>* responder, bool ok) {
  if (!ok) {
    // clean up immediately. successful messages will be cleaned up when the
    // responder finishes.
    CleanupAfterRequest(ctx, request, responder, ok);
    return;
  }
  PrepareToAcceptRequest<request_type, response_type>();
  ExecuteByLeader(ctx, request, responder);
}

// RequestRpc requests an appropriate RPC depending on the request and response
// type.
template <>
inline void VerbsMarksLeaderService::RequestRpc<proto::QuorumRequest,
                                                proto::QuorumResponse>(
    grpc::ServerContext* context, proto::QuorumRequest* request,
    grpc::ServerAsyncResponseWriter<proto::QuorumResponse>* responder,
    grpc::CompletionQueue* new_call_cq,
    grpc::ServerCompletionQueue* notification_cq, void* tag) {
  service_.RequestSeekQuorum(context, request, responder, new_call_cq,
                             notification_cq, tag);
}

template <>
inline void
VerbsMarksLeaderService::RequestRpc<proto::ReadyRequest, proto::ReadyResponse>(
    grpc::ServerContext* context, proto::ReadyRequest* request,
    grpc::ServerAsyncResponseWriter<proto::ReadyResponse>* responder,
    grpc::CompletionQueue* new_call_cq,
    grpc::ServerCompletionQueue* notification_cq, void* tag) {
  service_.RequestGetReady(context, request, responder, new_call_cq,
                           notification_cq, tag);
}

template <>
inline void VerbsMarksLeaderService::RequestRpc<proto::StartExperimentRequest,
                                                proto::StartExperimentResponse>(
    grpc::ServerContext* context, proto::StartExperimentRequest* request,
    grpc::ServerAsyncResponseWriter<proto::StartExperimentResponse>* responder,
    grpc::CompletionQueue* new_call_cq,
    grpc::ServerCompletionQueue* notification_cq, void* tag) {
  service_.RequestStartExperiment(context, request, responder, new_call_cq,
                                  notification_cq, tag);
}

template <>
inline void VerbsMarksLeaderService::RequestRpc<proto::ResultRequest,
                                                proto::ResultResponse>(
    grpc::ServerContext* context, proto::ResultRequest* request,
    grpc::ServerAsyncResponseWriter<proto::ResultResponse>* responder,
    grpc::CompletionQueue* new_call_cq,
    grpc::ServerCompletionQueue* notification_cq, void* tag) {
  service_.RequestFinishExperiment(context, request, responder, new_call_cq,
                                   notification_cq, tag);
}

template <>
inline void VerbsMarksLeaderService::RequestRpc<proto::HeartbeatRequest,
                                                proto::HeartbeatResponse>(
    grpc::ServerContext* context, proto::HeartbeatRequest* request,
    grpc::ServerAsyncResponseWriter<proto::HeartbeatResponse>* responder,
    grpc::CompletionQueue* new_call_cq,
    grpc::ServerCompletionQueue* notification_cq, void* tag) {
  service_.RequestHeartbeat(context, request, responder, new_call_cq,
                            notification_cq, tag);
}

// ExecuteByLeader invokes an appropriate method on the leader depending on the
// request and response type.
template <>
inline void VerbsMarksLeaderService::ExecuteByLeader(
    grpc::ServerContext* context, verbsmarks::proto::QuorumRequest* request,
    grpc::ServerAsyncResponseWriter<proto::QuorumResponse>* responder) {
  leader_->SeekQuorum(
      request,
      std::make_unique<std::function<void(proto::QuorumResponse)>>(
          absl::bind_front(
              &VerbsMarksLeaderService::ProcessResponse<proto::QuorumRequest,
                                                        proto::QuorumResponse>,
              this, context, request, responder)));
}

template <>
inline void VerbsMarksLeaderService::ExecuteByLeader(
    grpc::ServerContext* context, proto::ReadyRequest* request,
    grpc::ServerAsyncResponseWriter<proto::ReadyResponse>* responder) {
  leader_->GetReady(
      request->follower_id(),
      std::make_unique<std::function<void(proto::ReadyResponse)>>(
          absl::bind_front(
              &VerbsMarksLeaderService::ProcessResponse<proto::ReadyRequest,
                                                        proto::ReadyResponse>,
              this, context, request, responder)));
}

template <>
inline void VerbsMarksLeaderService::ExecuteByLeader(
    grpc::ServerContext* context, proto::StartExperimentRequest* request,
    grpc::ServerAsyncResponseWriter<proto::StartExperimentResponse>*
        responder) {
  leader_->StartExperiment(
      request,
      std::make_unique<std::function<void(proto::StartExperimentResponse)>>(
          absl::bind_front(&VerbsMarksLeaderService::ProcessResponse<
                               proto::StartExperimentRequest,
                               proto::StartExperimentResponse>,
                           this, context, request, responder)));
}

template <>
inline void VerbsMarksLeaderService::ExecuteByLeader(
    grpc::ServerContext* context, proto::ResultRequest* request,
    grpc::ServerAsyncResponseWriter<proto::ResultResponse>* responder) {
  leader_->FinishExperiment(
      request,
      std::make_unique<std::function<void(proto::ResultResponse)>>(
          absl::bind_front(
              &VerbsMarksLeaderService::ProcessResponse<proto::ResultRequest,
                                                        proto::ResultResponse>,
              this, context, request, responder)));
}

template <>
inline void VerbsMarksLeaderService::ExecuteByLeader(
    grpc::ServerContext* context, proto::HeartbeatRequest* request,
    grpc::ServerAsyncResponseWriter<proto::HeartbeatResponse>* responder) {
  leader_->HandleHeartbeat(
      request, std::make_unique<std::function<void(proto::HeartbeatResponse)>>(
                   absl::bind_front(
                       &VerbsMarksLeaderService::ProcessResponse<
                           proto::HeartbeatRequest, proto::HeartbeatResponse>,
                       this, context, request, responder)));
}

}  // namespace verbsmarks
