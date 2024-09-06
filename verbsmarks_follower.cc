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

#include "verbsmarks_follower.h"

#include <chrono>
#include <cmath>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "absl/algorithm/container.h"
#include "absl/container/flat_hash_map.h"
#include "absl/flags/flag.h"
#include "absl/log/log.h"
#include "absl/meta/type_traits.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "connection_coordinator.h"
#include "connection_coordinator.pb.h"
#include "google/rpc/code.pb.h"
#include "google/rpc/status.pb.h"
#include "grpcpp/client_context.h"
#include "grpcpp/server_context.h"
#include "grpcpp/support/status.h"
#include "ibverbs_utils.h"
#include "traffic_generator.h"
#include "utils.h"
#include "verbsmarks.grpc.pb.h"
#include "verbsmarks.pb.h"
#include "verbsmarks_binary_flags.h"

namespace verbsmarks {

using ::google::rpc::Code;

void VerbsMarksFollower::StartConnectionCoordinatorServer() {
  // Only start the server once.
  if (connection_coordinator_server_.has_value()) {
    return;
  }
  connection_coordinator_server_.emplace(
      server_credentials_, http_server_addr_,
      std::make_unique<ConnectionCoordinatorService>(this));
  connection_coordinator_server_->RunInThread();
}

absl::Status VerbsMarksFollower::Run() {
  StartConnectionCoordinatorServer();

  if (absl::Status status = JoinQuorum(); !status.ok()) return status;
  if (absl::Status status = GetReady(); !status.ok()) {
    LOG(ERROR) << "Ready failed. " << status
               << " Reporting error to the leader: "
               << SendStartExperimentRequest(status.raw_code());
    return status;
  }
  if (absl::Status status = StartExperiment(); !status.ok()) {
    LOG(ERROR) << "Running experiment failed with error: " << status;
    return status;
  }

  if (absl::Status status = FinishExperiment(); !status.ok()) {
    LOG(ERROR) << "Finishing experiment failed with error: " << status;
    return status;
  }
  return absl::OkStatus();
}

absl::Status VerbsMarksFollower::JoinQuorum() {
  proto::QuorumRequest request;
  proto::QuorumResponse response;
  request.set_http_server_address(http_server_addr_);
  request.set_follower_alias(alias_);

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       utils::kGrpcLongRequestTimeout);
  grpc::Status grpc_status =
      leader_stub_->SeekQuorum(&context, request, &response);

  if (!grpc_status.ok()) {
    LOG(ERROR) << "JoinQuorum RPC failed with error: "
               << grpc_status.error_message();
    return absl::Status(static_cast<absl::StatusCode>(grpc_status.error_code()),
                        grpc_status.error_message());
  }

  if (response.status().code() != Code::OK) {
    return absl::InternalError(
        absl::StrCat("JoinQuorum has error response: ", response));
  }
  bool found_me = false;
  for (const auto& follower : response.followers()) {
    followers_[follower.follower_id()] = follower;
    if (follower.http_server_address() == http_server_addr_) {
      found_me = true;
      my_id_ = follower.follower_id();
      LOG(INFO) << "Joined quorum. My follower information: " << follower;
    }
  }
  if (!found_me) {
    return absl::InternalError(
        absl::StrCat("The quorum does not include me: ", http_server_addr_, " ",
                     alias_, " in ", response));
  }
  if (followers_.empty()) {
    return absl::InternalError(
        absl::StrCat("The quorum does not have any follower", response));
  }
  return absl::OkStatus();
}

absl::Status VerbsMarksFollower::GetReady() {
  proto::ReadyRequest request;
  proto::ReadyResponse response;
  request.set_follower_id(my_id_);
  // First request ReadyResponse to the leader. It will contain information
  // required.
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       utils::kGrpcRequestTimeout);
  LOG(INFO) << "Follower sending GetReady to leader";
  if (grpc::Status status =
          leader_stub_->GetReady(&context, request, &response);
      !status.ok()) {
    LOG(ERROR) << "RPC to leader failed: " << status.error_message();
    return absl::Status(static_cast<absl::StatusCode>(status.error_code()),
                        status.error_message());
  }
  if (response.status().code() != Code::OK) {
    LOG(ERROR) << "Ready response contains an error. Cannot proceed: "
               << response.status();
    return absl::FailedPreconditionError(response.status().DebugString());
  }

  auto status_or_qp_max_wr = ibverbs_utils::GetMaxQpWr(verbs_context_.get());
  if (!status_or_qp_max_wr.ok()) {
    return status_or_qp_max_wr.status();
  }
  int qp_max_wr = status_or_qp_max_wr.value();
  for (const proto::PerFollowerTrafficPattern& traffic_pattern :
       response.per_follower_traffic_patterns()) {
    if (traffic_pattern.traffic_characteristics().batch_size() > qp_max_wr) {
      return absl::FailedPreconditionError(absl::StrCat(
          "Requested batch size is bigger than QP max wr on hardware, ",
          qp_max_wr));
    }
  }

  // If memory resource policy is not set in config, set a default values.
  proto::MemoryResourcePolicy memory_resource_policy;
  if (response.has_memory_resource_policy()) {
    memory_resource_policy = response.memory_resource_policy();
  } else {
    memory_resource_policy.set_pd_allocation_policy(proto::PD_PER_QP);
    memory_resource_policy.set_qp_mr_mapping(proto::QP_HAS_DEDICATED_MRS);
    memory_resource_policy.set_num_mrs_per_qp(1);
  }
  // Initialize all protection domains, memory blocks and memory regions.
  auto status = memory_manager_.InitializeResources(
      verbs_context_.get(), memory_resource_policy,
      response.per_follower_traffic_patterns());
  if (!status.ok()) {
    return status;
  }

  // Create a TrafficGenerator for each traffic pattern.
  {
    absl::Time start = absl::Now();
    absl::Mutex pool_mutex;
    utils::ThreadPool pool(response.per_follower_traffic_patterns().size());
    absl::Status pool_status = absl::OkStatus();
    for (const proto::PerFollowerTrafficPattern& traffic_pattern :
         response.per_follower_traffic_patterns()) {
      pool.Add([&]() {
        {
          absl::MutexLock lock(&pool_mutex);
          if (!pool_status.ok()) return;
        }
        if (absl::StatusOr<std::unique_ptr<TrafficGenerator>>
                status_or_traffic_generator(TrafficGenerator::Create(
                    verbs_context_.get(), local_ibverbs_address_,
                    traffic_pattern,
                    /*override_queue_pairs=*/{}, &memory_manager_));
            !status_or_traffic_generator.ok()) {
          std::string error_message = absl::StrCat(
              "VerbsMarksFollower could not create TrafficGenerator with id ",
              traffic_pattern.global_traffic_pattern_id(), ": ",
              status_or_traffic_generator.status().message());
          LOG(ERROR) << error_message;
          absl::MutexLock lock(&pool_mutex);
          pool_status = absl::Status(
              status_or_traffic_generator.status().code(), error_message);
        } else {
          absl::MutexLock lock(&traffic_generators_mutex_);
          traffic_generators_.try_emplace(
              traffic_pattern.global_traffic_pattern_id(),
              std::move(status_or_traffic_generator.value()));
        }
      });
    }
    pool.Wait();
    LOG(INFO) << "Creating traffic generators took " << absl::Now() - start
              << " with status: " << pool_status;
    if (!pool_status.ok()) {
      return pool_status;
    }
    // Mark ready so that other nodes can request queue pair attributes from our
    // traffic generators.
    absl::MutexLock lock(&traffic_generators_mutex_);
    traffic_generators_ready_ = true;
  }

  // Connect all queue pairs.
  // Use a reader lock so that we can hold the lock concurrently to iterate over
  // the traffic generator map to request queue pair information from peers, and
  // to search through the traffic generator map when peers request queue pair
  // information from us.
  absl::ReaderMutexLock lock(&traffic_generators_mutex_);
  RemoteQueuePairAttributesFetcher remote_qp_attributes_fetcher(&followers_);
  {
    absl::Time start = absl::Now();
    absl::Mutex pool_mutex;
    utils::ThreadPool pool(response.per_follower_traffic_patterns().size());
    absl::Status pool_status = absl::OkStatus();
    for (auto& it : traffic_generators_) {
      std::unique_ptr<TrafficGenerator>& traffic_generator = it.second;
      pool.Add([&]() {
        if (absl::Status status = traffic_generator->ConnectQueuePairs(
                remote_qp_attributes_fetcher,
                utils::GetGrpcChannelCredentials());
            !status.ok()) {
          std::string error_message = absl::StrCat(
              "VerbsMarksFollower could not connect "
              "queue pairs for TrafficGenerator: ",
              status.message());
          LOG(ERROR) << error_message;
          absl::MutexLock lock(&pool_mutex);
          pool_status = absl::Status(status.code(), error_message);
        }
      });
    }
    pool.Wait();
    LOG(INFO) << "Connecting queue pairs took " << absl::Now() - start
              << " with status: " << pool_status;
    if (!pool_status.ok()) {
      return pool_status;
    }
  }

  // Prepare for traffic generation: e.g. send pingpong requires preposting
  // and write pingpong requires initializing the memory.
  for (auto& it : traffic_generators_) {
    std::unique_ptr<TrafficGenerator>& traffic_generator = it.second;
    if (absl::Status status = traffic_generator->Prepare(); !status.ok()) {
      std::string error_message = absl::StrCat(
          "VerbsMarksFollower failed to prepare:", status.message());
      LOG(ERROR) << error_message;
      return absl::Status(status.code(), error_message);
    }
  }

  LOG(INFO) << "Follower '" << alias_ << "' finished getting ready.";
  return absl::OkStatus();
}

absl::Status VerbsMarksFollower::SendStartExperimentRequest(int raw_code) {
  proto::StartExperimentRequest request;
  proto::StartExperimentResponse response;
  request.set_follower_id(my_id_);
  request.mutable_status()->set_code(raw_code);
  // Only proceed if reader gives an OK.
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       utils::kGrpcRequestTimeout);
  if (grpc::Status status =
          leader_stub_->StartExperiment(&context, request, &response);
      !status.ok()) {
    LOG(ERROR) << "RPC to leader failed: " << status.error_message();
    return absl::Status(static_cast<absl::StatusCode>(status.error_code()),
                        status.error_message());
  }
  if (response.status().code() != Code::OK) {
    LOG(ERROR) << "StartExperiment response contains an error. Cannot proceed: "
               << response.status();
    return absl::FailedPreconditionError(response.status().DebugString());
  }
  if (response.enable_heartbeats()) {
    heartbeat_send_interval_ = kDefaultHeartbeatSendInterval;
  }
  return absl::OkStatus();
}

absl::StatusOr<bool> VerbsMarksFollower::CheckFinished() {
  // Inspect status_of_traffic_generators_ for error or successful finish.
  absl::ReaderMutexLock lock(&traffic_generators_mutex_);
  for (const auto& it : status_of_traffic_generators_) {
    if (!it.second.ok()) {
      // If any traffic generator ended with error status, return the error.
      LOG(ERROR) << "Follower is exiting due to traffic generator error!";
      LOG(ERROR) << "Traffic generator " << it.first << " failed with status "
                 << it.second;
      return it.second;
    }
  }

  // Finished (return true) if status vector is filled. False if not finished
  // and no error.
  return (status_of_traffic_generators_.size() == traffic_generators_.size());
}

absl::Status VerbsMarksFollower::StartExperiment() {
  if (auto status = SendStartExperimentRequest(Code::OK); !status.ok()) {
    return status;
  }
  std::vector<std::thread> bundle;
  {
    absl::ReaderMutexLock lock(&traffic_generators_mutex_);
    for (const auto& traffic_generator : traffic_generators_) {
      bundle.push_back(std::thread(&VerbsMarksFollower::StartTraffic, this,
                                   traffic_generator.first,
                                   traffic_generator.second.get()));
    }
  }

  auto experiment_status = SendHeartbeatsUntilFinished();
  if (!experiment_status.ok()) {
    // Follower must abort traffic generator threads due to a local error or
    // an unhealthy response from leader.
    absl::MutexLock lock(&traffic_generators_mutex_);
    for (const auto& traffic_generator : traffic_generators_) {
      traffic_generator.second->Abort();
    }
  }

  for (auto& traffic_pattern_worker : bundle) {
    if (traffic_pattern_worker.joinable()) {
      traffic_pattern_worker.join();
    }
  }

  {
    absl::ReaderMutexLock lock(&traffic_generators_mutex_);
    for (const auto& status : status_of_traffic_generators_) {
      if (!status.second.ok()) {
        LOG(ERROR) << "Follower is returning error status due to failed "
                   << "traffic generator " << status.first
                   << " with status: " << status.second;
        return status.second;
      }
    }
  }
  return experiment_status;
}

proto::CurrentStats VerbsMarksFollower::GetCurrentStatistics() {
  proto::CurrentStats current_stats;
  absl::MutexLock lock(&traffic_generators_mutex_);
  for (auto& traffic_generator : traffic_generators_) {
    *current_stats.add_traffic_current_stats() =
        traffic_generator.second->GetCurrentStatistics();
  }
  return current_stats;
}

absl::Status VerbsMarksFollower::SendHeartbeatsUntilFinished() {
  // Skip entirely (and go to thread join) if heartbeat duration is unspecified.
  if (heartbeat_send_interval_ == absl::InfiniteDuration()) {
    return absl::OkStatus();
  }

  proto::HeartbeatRequest request;
  proto::HeartbeatResponse response;
  bool continue_sending_heartbeats = true;

  request.set_follower_id(my_id_);
  request.mutable_follower_status()->set_code(Code::OK);

  while (continue_sending_heartbeats) {
    auto heartbeat_send_time = absl::Now();
    auto finished_or_error = CheckFinished();
    if (!finished_or_error.ok()) {
      request.mutable_follower_status()->set_code(
          finished_or_error.status().raw_code());
      continue_sending_heartbeats = false;
    } else if (finished_or_error.value()) {
      // CheckFinished returned true, so follower finished successfully.
      continue_sending_heartbeats = false;
    }

    *request.mutable_current_stats() = GetCurrentStatistics();

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         utils::kGrpcRequestTimeout);
    auto rpc_status = leader_stub_->Heartbeat(&context, request, &response);
    if (!rpc_status.ok()) {
      LOG(ERROR) << "Heartbeat to leader failed with status: "
                 << rpc_status.error_message();
      LOG(ERROR) << "Failed heartbeat request: " << request;
      LOG(ERROR) << "Check leader log for failure of leader or other follower.";
      return absl::UnavailableError("Failed to complete heartbeat RPC");
    }

    // Abort and exit if we just received a response from the leader with an
    // error status.
    if (response.leader_status().code() != Code::OK) {
      LOG(ERROR) << "Heartbeat response from leader has error status: "
                 << response.leader_status().code()
                 << " and message: " << response.leader_status();
      LOG(ERROR) << "Check leader log for failure of leader or other follower.";
      return absl::CancelledError("Ending due to leader heartbeat response");
    }
    // Abort and exit if this follower encountered a local error. This is
    // checked after sending the heartbeat to ensure that the leader is
    // notified of the error.
    if (request.follower_status().code() != Code::OK) {
      LOG(ERROR) << "Aborting after sending error heartbeat to leader";
      return absl::CancelledError("Ending due to local failure");
    }

    absl::SleepFor(heartbeat_send_interval_ -
                   (absl::Now() - heartbeat_send_time));
  }

  return absl::OkStatus();
}

proto::ResultRequest VerbsMarksFollower::GetResults() {
  proto::ResultRequest request;
  request.set_follower_id(my_id_);
  request.set_follower_alias(alias_);

  request.mutable_status()->set_code(Code::OK);
  // Check status of workers, if anyone failed, it reports an error to the
  // leader.
  absl::MutexLock lock(&traffic_generators_mutex_);
  for (auto& status : status_of_traffic_generators_) {
    if (!status.second.ok()) {
      LOG(ERROR) << "Follower GetResults is returning error status due to "
                 << "failed traffic generator " << status.first
                 << " with status: " << status.second;
      request.mutable_status()->set_code(Code::INTERNAL);
      break;
    }
  }

  absl::Duration average_latency = absl::ZeroDuration();
  double_t total_latency_obtained = 0;  // Use double for floating point math.
  proto::ThroughputResult total_tput = utils::MakeThroughputResult(0, 0);
  // For latency approximation for all the flows in the traffic.
  std::vector<absl::Duration> median_latencies;
  std::vector<absl::Duration> p99_latencies;
  std::vector<absl::Duration> p999_latencies;
  std::vector<absl::Duration> p9999_latencies;
  absl::Duration min_latency = absl::InfiniteDuration();

  for (auto& traffic_generator : traffic_generators_) {
    const proto::PerTrafficResult& single_result =
        traffic_generator.second->GetSummary();
    *request.add_per_traffic_results() = single_result;
    if (single_result.num_latency_obtained() == 0) {
      // Nothing to add.
      continue;
    }
    median_latencies.push_back(
        utils::ProtoToDuration(single_result.latency().median_latency()));
    p99_latencies.push_back(
        utils::ProtoToDuration(single_result.latency().p99_latency()));
    p999_latencies.push_back(
        utils::ProtoToDuration(single_result.latency().p999_latency()));
    p9999_latencies.push_back(
        utils::ProtoToDuration(single_result.latency().p9999_latency()));

    double_t old_total_latency = total_latency_obtained;
    total_latency_obtained += single_result.num_latency_obtained();
    average_latency =
        (old_total_latency / total_latency_obtained) * average_latency +
        (static_cast<double_t>(single_result.num_latency_obtained()) /
         total_latency_obtained) *
            utils::ProtoToDuration(single_result.latency().average_latency());
    if (min_latency >
        utils::ProtoToDuration(single_result.latency().min_latency())) {
      min_latency =
          utils::ProtoToDuration(single_result.latency().min_latency());
    }

    utils::AppendThroughputResult(total_tput, single_result.throughput());
  }
  if (total_latency_obtained > 0) {
    utils::DurationToProto(average_latency, *request.mutable_average_latency());
    absl::c_sort(median_latencies);
    absl::c_sort(p99_latencies);
    absl::c_sort(p999_latencies);
    absl::c_sort(p9999_latencies);
    int idx = median_latencies.size() / 2;
    if (median_latencies.size() % 2 == 0) {
      absl::Duration v1 = median_latencies.at(idx - 1);
      absl::Duration v2 = median_latencies.at(idx);
      absl::Duration v3 = (v1 + v2) * 0.5;
      utils::DurationToProto(
          v3, *request.mutable_latency()->mutable_median_latency());
    } else {
      utils::DurationToProto(
          median_latencies.at(idx),
          *request.mutable_latency()->mutable_median_latency());
    }
    utils::DurationToProto(min_latency,
                           *request.mutable_latency()->mutable_min_latency());
  }
  *request.mutable_throughput() = total_tput;

  return request;
}

absl::Status VerbsMarksFollower::FinishExperiment() {
  proto::ResultRequest request = GetResults();
  proto::ResultResponse response;

  int num_traffic_generators = 0;
  {
    absl::MutexLock lock(&traffic_generators_mutex_);
    num_traffic_generators = traffic_generators_.size();
  }
  utils::ThreadPool pool(num_traffic_generators);
  // Cleanup all traffic generators to speed up qp destroy. This will be a no-op
  // if the traffic generators has less than 1K Qps.
  {
    absl::MutexLock lock(&traffic_generators_mutex_);
    for (auto& traffic_generator : traffic_generators_) {
      pool.Add([&]() {
        LOG(INFO) << "Cleaning up traffic generator: "
                  << traffic_generator.second->Cleanup();
      });
    }
  }
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       utils::kGrpcLongRequestTimeout);
  grpc::Status grpc_status =
      leader_stub_->FinishExperiment(&context, request, &response);

  if (!grpc_status.ok()) {
    LOG(ERROR) << "Reporting FinishExperiment to leader failed: "
               << grpc_status.error_message();
    return absl::Status(static_cast<absl::StatusCode>(grpc_status.error_code()),
                        grpc_status.error_message());
  }
  LOG(INFO) << "Reporting is done. Wait for clean up to finish.";
  pool.Wait();
  return absl::OkStatus();
}

void VerbsMarksFollower::StartTraffic(const int traffic_pattern_id,
                                      TrafficGenerator* traffic_generator) {
  absl::Status status = traffic_generator->Run();

  absl::MutexLock lock(&traffic_generators_mutex_);
  status_of_traffic_generators_[traffic_pattern_id] = status;
}

grpc::Status
VerbsMarksFollower::ConnectionCoordinatorService::GetQueuePairAttributes(
    grpc::ServerContext* context,
    const proto::QueuePairAttributesRequest* request,
    proto::QueuePairAttributesResponse* response) {
  response->set_responder_id(follower_->my_id_);
  response->set_traffic_pattern_id(request->traffic_pattern_id());
  response->set_queue_pair_id(request->queue_pair_id());
  response->set_responder_ip_addr(follower_->local_ibverbs_address_.ip_addr);

  // Wait on the condition that the traffic generators have been initialized.
  // Use a reader lock so that we can hold the lock concurrently to iterate over
  // the traffic generator map to request queue pair information from peers, and
  // to search through the traffic generator map when peers request queue pair
  // information from us.
  absl::ReaderMutexLock lock(
      &follower_->traffic_generators_mutex_,
      absl::Condition(&follower_->traffic_generators_ready_));

  if (auto it =
          follower_->traffic_generators_.find(request->traffic_pattern_id());
      it != follower_->traffic_generators_.end()) {
    std::unique_ptr<TrafficGenerator>& traffic_generator = it->second;

    if (absl::Status status =
            traffic_generator->PopulateAttributesForQps(*request, *response);
        !status.ok()) {
      return grpc::Status(
          grpc::StatusCode::INTERNAL,
          absl::StrCat("ConnectionCoordinatorService failed to get traffic "
                       "generator to populate remote attributes: ",
                       status.message()));
    }
  } else {
    return grpc::Status(
        grpc::StatusCode::NOT_FOUND,
        absl::StrCat(
            "ConnectionCoordinatorService cannot find traffic pattern with id ",
            request->traffic_pattern_id()));
  }

  return grpc::Status::OK;
}

}  // namespace verbsmarks
