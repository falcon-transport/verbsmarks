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

#include "traffic_generator.h"

#include <pthread.h>
#include <stdlib.h>
#include <sys/stat.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/flags/flag.h"
#include "absl/log/log.h"
#include "absl/memory/memory.h"
#include "absl/meta/type_traits.h"
#include "absl/numeric/bits.h"
#include "absl/random/discrete_distribution.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "completion_queue_manager.h"
#include "connection_coordinator.h"
#include "connection_coordinator.pb.h"
#include "folly/stats/TDigest.h"
#include "google/protobuf/duration.pb.h"
#include "google/protobuf/repeated_ptr_field.h"
#include "grpcpp/security/credentials.h"
#include "ibverbs_utils.h"
#include "infiniband/verbs.h"
#include "load_generator.h"
#include "memory_manager.h"
#include "queue_pair.h"
#include "special_traffic_generator.h"
#include "utils.h"
#include "verbsmarks.pb.h"
#include "verbsmarks_binary_flags.h"

namespace verbsmarks {
namespace {

constexpr int kMaxConnectThreads = 5;
constexpr int kMaxCleanupThreads = 10;
constexpr absl::Duration kCleanupGracePeriod = absl::Seconds(3);

inline absl::Duration ToDuration(const google::protobuf::Duration& proto) {
  return absl::Seconds(proto.seconds()) + absl::Nanoseconds(proto.nanos());
}

static absl::StatusOr<std::unique_ptr<QueuePairSelector>>
CreateQueuePairSelector(const proto::PerFollowerTrafficPattern& pattern) {
  switch (pattern.traffic_type()) {
    case proto::TRAFFIC_TYPE_EXPLICIT:
      return std::make_unique<RoundRobinQueuePairSelector>(pattern);
    case proto::TRAFFIC_TYPE_UNIFORM_RANDOM:
      return std::make_unique<UniformRandomQueuePairSelector>(pattern);
    case proto::TRAFFIC_TYPE_PINGPONG:
      // In pingpong traffic type, there is only one QueuePair to select from as
      // there is only one peer and one flow. RoundRobinQueuePair selector will
      // keep returning the same QueuePair as there is only one to choose from.
      return std::make_unique<RoundRobinQueuePairSelector>(pattern);
    case proto::TRAFFIC_TYPE_BANDWIDTH:
      return std::make_unique<RoundRobinQueuePairSelector>(pattern);
    default:
      return absl::UnimplementedError(absl::StrCat(
          "QueuePairSelector not implemented for ", pattern.traffic_type()));
  }
}

void SetAffinityIfNeeded(
    const proto::PerFollowerTrafficPattern& traffic_pattern, bool poller) {
  if (!traffic_pattern.has_thread_affinity_info()) {
    return;
  }
  const proto::ThreadAffinityInfo& affinity =
      traffic_pattern.thread_affinity_info();
  int cpu_to_pin = affinity.cpu_to_pin_poster();
  if (poller) {
    cpu_to_pin = affinity.cpu_to_pin_poller();
  }
  bool pinned = utils::SetThreadAffinityAndLog(utils::AssignableCpu(cpu_to_pin),
                                               pthread_self());
  if (affinity.halt_if_fail_to_pin() && !pinned) {
    LOG(FATAL) << "Halting because strict thread affinity is requested and "
                  "failed to pin to the requested core.";
  }
}
}  // namespace

OpTypeSize TrafficGenerator::GetOpTypeSize() {
  return rdma_op_types_[operation_picker_.value()(gen_)];
}

QueuePair* TrafficGenerator::GetNextQueuePair() {
  if (initiator_queue_pair_ids_.empty()) {
    return nullptr;
  }
  auto idx = queue_pair_selector_->GetNextQueuePairId();
  return queue_pairs_[idx].get();
}

absl::StatusOr<std::unique_ptr<CompletionQueueManager>>
TrafficGenerator::InitializeCompletionQueueManager(
    ibv_context* context,
    const proto::PerFollowerTrafficPattern traffic_pattern) {
  // If no policy is provided, default to private CQs
  proto::CompletionQueuePolicy policy =
      proto::COMPLETION_QUEUE_POLICY_PRIVATE_CQ;
  if (traffic_pattern.completion_queue_policy() !=
      proto::COMPLETION_QUEUE_POLICY_UNSPECIFIED) {
    policy = traffic_pattern.completion_queue_policy();
  }

  uint32_t cq_depth = 0;
  int num_cqs = 0;

  // Add recvs to CQ depth calculation if workload includes send/recv ops.
  bool has_recv_ops = false;
  for (const auto& op : traffic_pattern.traffic_characteristics().op_ratio()) {
    if (op.op_code() == proto::RDMA_OP_SEND_RECEIVE ||
        op.op_code() == proto::RDMA_OP_SEND_WITH_IMMEDIATE_RECEIVE) {
      has_recv_ops = true;
      break;
    }
  }

  if (policy == proto::COMPLETION_QUEUE_POLICY_SHARED_CQ) {
    VLOG(2) << "Initializing CompletionQueueManager with shared CQ policy";
    num_cqs = 1;
    // Calculate CQ depth based on sum of outstanding ops limit and max recv
    // counts across all QPs. Same formula as existing CQ init in QueuePair.
    for (const auto& queue_pair : traffic_pattern.queue_pairs()) {
      uint32_t outstanding_ops = 0, recv_ops = 0;
      if (queue_pair.override_queue_depth() > 0) {
        outstanding_ops = queue_pair.override_queue_depth();
      } else if (queue_pair.max_outstanding_ops() > 0) {
        outstanding_ops = queue_pair.max_outstanding_ops();
      } else {
        // If neither max_outstanding_ops nor override_queue_depth is defined,
        // then we skip calculation (cq_depth=0) to use device HW max.
        break;
      }
      if (has_recv_ops) {
        recv_ops = outstanding_ops + queue_pair.num_prepost_receive_ops();
      }
      cq_depth += outstanding_ops + recv_ops;
    }

  } else if (policy == proto::COMPLETION_QUEUE_POLICY_PRIVATE_CQ) {
    VLOG(2) << "Initializing CompletionQueueManager with private CQ policy";
    num_cqs = traffic_pattern.queue_pairs_size();
    // Calculate needed CQ depth based on maximum outstanding ops and recv
    // counts across all QPs.
    for (const auto& queue_pair : traffic_pattern.queue_pairs()) {
      uint32_t outstanding_ops = 0, recv_ops = 0;
      if (queue_pair.override_queue_depth() > 0) {
        outstanding_ops = queue_pair.override_queue_depth();
      } else if (queue_pair.max_outstanding_ops() > 0) {
        outstanding_ops = queue_pair.max_outstanding_ops();
      } else {
        // If neither max_outstanding_ops nor override_queue_depth is defined,
        // then we skip calculation (cq_depth=0) to use device HW max.
        break;
      }
      if (has_recv_ops) {
        recv_ops = outstanding_ops + queue_pair.num_prepost_receive_ops();
      }
      if (outstanding_ops + recv_ops > cq_depth) {
        cq_depth = outstanding_ops + recv_ops;
      }
    }
  } else {
    return absl::InvalidArgumentError(
        absl::StrCat("Completion queue policy not implemented: ", policy));
  }

  // Align CQ size to ceil power of 2. Skip if cq_depth is zero to use HW max
  // size as default.
  if (cq_depth > 0) {
    cq_depth = absl::bit_ceil(cq_depth);
  }

  std::unique_ptr<CompletionQueueManager> completion_queue_manager;
  if (traffic_pattern.use_event()) {
    completion_queue_manager = std::make_unique<EventCompletionQueueManager>();
  } else {
    completion_queue_manager = std::make_unique<CompletionQueueManager>();
  }

  absl::Status status =
      completion_queue_manager->InitializeResources(context, num_cqs, cq_depth);
  if (!status.ok()) {
    return absl::Status(
        status.code(),
        absl::StrCat(
            "TrafficGenerator failed to initialize CompletionQueueManager: ",
            status.message()));
  }
  return completion_queue_manager;
}

absl::StatusOr<std::unique_ptr<TrafficGenerator>> TrafficGenerator::Create(
    ibv_context* context,
    const ibverbs_utils::LocalIbverbsAddress local_address,
    const proto::PerFollowerTrafficPattern traffic_pattern,
    absl::flat_hash_map<int32_t, std::unique_ptr<QueuePair>>
        override_queue_pairs,
    const MemoryManager* memory_manager,
    std::unique_ptr<CompletionQueueManager> override_completion_queue_manager) {
  SetAffinityIfNeeded(traffic_pattern, true);
  absl::flat_hash_map<int32_t, std::unique_ptr<QueuePair>> queue_pairs;
  bool is_pingpong = false;
  bool qp_overridden = false;

  if (traffic_pattern.traffic_type() == proto::TRAFFIC_TYPE_PINGPONG) {
    is_pingpong = true;
  }

  std::unique_ptr<CompletionQueueManager> completion_queue_manager;
  if (override_completion_queue_manager != nullptr) {
    completion_queue_manager = std::move(override_completion_queue_manager);
  } else {
    auto cq_manager_or_status =
        InitializeCompletionQueueManager(context, traffic_pattern);
    if (!cq_manager_or_status.ok()) {
      return cq_manager_or_status.status();
    }
    completion_queue_manager = std::move(cq_manager_or_status.value());
  }

  if (!override_queue_pairs.empty()) {
    if (override_queue_pairs.size() != traffic_pattern.queue_pairs_size()) {
      return absl::InvalidArgumentError("QueuePair override does not match");
    }
    VLOG(2) << "Overriding QPs";
    queue_pairs = std::move(override_queue_pairs);
    qp_overridden = true;
  } else {
    // Creates the queue pairs needed to generate the traffic in the pattern.
    queue_pairs.reserve(traffic_pattern.queue_pairs_size());
    for (const proto::QueuePairConfig& queue_pair_config :
         traffic_pattern.queue_pairs()) {
      // Creates the QP based on the connection type.
      switch (queue_pair_config.connection_type()) {
        case proto::ConnectionType::CONNECTION_TYPE_RC:
          queue_pairs[queue_pair_config.queue_pair_id()] =
              std::make_unique<RcQueuePair>(context, local_address,
                                            queue_pair_config, is_pingpong,
                                            completion_queue_manager.get());
          break;
        case proto::ConnectionType::CONNECTION_TYPE_UD:
          queue_pairs[queue_pair_config.queue_pair_id()] =
              std::make_unique<UdQueuePair>(context, local_address,
                                            queue_pair_config, is_pingpong,
                                            completion_queue_manager.get());
          break;
        default:
          return absl::InvalidArgumentError(
              absl::StrCat("TrafficGenerator does not support connection type",
                           queue_pair_config.connection_type()));
          break;
      }
    }
  }
  if (queue_pairs.empty()) {
    return absl::InvalidArgumentError("Can't work without any queue pair");
  }
  int min_wqe_cap = std::numeric_limits<int>::max();
  for (auto& pair : queue_pairs) {
    int wqe_cap = pair.second->GetWqeCap();
    if (min_wqe_cap > wqe_cap) {
      min_wqe_cap = wqe_cap;
    }
  }
  absl::StatusOr<std::unique_ptr<LoadGenerator>> load_generator_or_status =
      CreateLoadGenerator(traffic_pattern, min_wqe_cap);
  if (!load_generator_or_status.ok()) {
    const absl::Status& status = load_generator_or_status.status();
    return absl::Status(
        status.code(),
        absl::StrCat("TrafficGenerator failed to create load generator: ",
                     status.message()));
  }
  if (traffic_pattern.traffic_characteristics().op_ratio().empty()) {
    return absl::InvalidArgumentError("Operation ratio cannot be empty");
  }
  for (const auto& op_ratio :
       traffic_pattern.traffic_characteristics().op_ratio()) {
    if (op_ratio.ratio() <= 0) {
      return absl::InvalidArgumentError(
          "Operation ratio should be bigger than 0");
    }
    if (op_ratio.op_code() == proto::RDMA_OP_UNKNOWN) {
      return absl::InvalidArgumentError("Operation code must be valid");
    }
    if (traffic_pattern.traffic_type() == proto::TRAFFIC_TYPE_PINGPONG) {
      //
      if (op_ratio.op_code() != proto::RDMA_OP_SEND_RECEIVE &&
          op_ratio.op_code() != proto::RDMA_OP_WRITE) {
        return absl::InvalidArgumentError(
            "Pingpong only supports SEND and WRITE");
      }
    }
  }
  if (traffic_pattern.traffic_characteristics()
              .message_size_distribution_type() !=
          proto::MESSAGE_SIZE_DISTRIBUTION_BY_OPS &&
      traffic_pattern.traffic_characteristics()
              .message_size_distribution_type() !=
          proto::MESSAGE_SIZE_DISTRIBUTION_FIXED) {
    return absl::UnimplementedError("Message distribution is not supported.");
  }
  if (traffic_pattern.traffic_characteristics()
          .size_distribution_params_in_bytes()
          .mean() < 1) {
    return absl::InvalidArgumentError(
        "Size distribution mean should be bigger than 0");
  }
  auto queue_pair_selector_or_status = CreateQueuePairSelector(traffic_pattern);
  if (auto status = queue_pair_selector_or_status.status();
      !queue_pair_selector_or_status.ok()) {
    return absl::Status(
        status.code(),
        absl::StrCat("TrafficGenerator failed to create queue pair selector: ",
                     status.message()));
  }
  if (memory_manager == nullptr) {
    return absl::InvalidArgumentError("Memory manager must be provided.");
  }
  absl::Time start = absl::Now();
  for (const proto::QueuePairConfig& queue_pair_config :
       traffic_pattern.queue_pairs()) {
    std::unique_ptr<QueuePair>& queue_pair =
        queue_pairs[queue_pair_config.queue_pair_id()];

    auto resources_or_status = memory_manager->GetQueuePairMemoryResources(
        traffic_pattern.global_traffic_pattern_id(),
        queue_pair_config.queue_pair_id());
    if (!resources_or_status.ok()) {
      LOG(ERROR) << "Failed to get queue pair "
                 << queue_pair_config.queue_pair_id() << ": "
                 << resources_or_status.status();
      return absl::Status(
          resources_or_status.status().code(),
          absl::StrCat("TrafficGenerator failed to get queue pair memory "
                       "resources: ",
                       resources_or_status.status().message()));
    }
    auto status = queue_pair->InitializeResources(
        resources_or_status.value(),
        traffic_pattern.traffic_characteristics().inline_threshold());

    if (!status.ok()) {
      LOG(ERROR) << "Failed to initialize queue pair "
                 << queue_pair_config.queue_pair_id() << ": " << status;
      return status;
    }
  }
  LOG(INFO) << "Initialize QPs took " << absl::Now() - start;
  return absl::WrapUnique(new TrafficGenerator(
      context, local_address, traffic_pattern,
      std::move(load_generator_or_status.value()),
      std::move(queue_pair_selector_or_status.value()), std::move(queue_pairs),
      std::move(completion_queue_manager)));
}

TrafficGenerator::TrafficGenerator(
    ibv_context* context,
    const ibverbs_utils::LocalIbverbsAddress local_address,
    const proto::PerFollowerTrafficPattern traffic_pattern,
    std::unique_ptr<LoadGenerator> load_generator,
    std::unique_ptr<QueuePairSelector> queue_pair_selector,
    absl::flat_hash_map<int32_t, std::unique_ptr<QueuePair>> queue_pairs,
    std::unique_ptr<CompletionQueueManager> completion_queue_manager)
    : post_finished_(false),
      load_generator_(std::move(load_generator)),
      queue_pair_selector_(std::move(queue_pair_selector)),
      traffic_pattern_(traffic_pattern),
      completion_queue_manager_(std::move(completion_queue_manager)),
      queue_pairs_(std::move(queue_pairs)),
      started_measurements_(false),
      stopped_measurements_(false),
      need_to_resume_(false) {
  std::vector<double> rdma_op_ratios;
  const auto& traffic_char = traffic_pattern_.traffic_characteristics();
  rdma_op_ratios.reserve(traffic_char.op_ratio().size());
  rdma_op_types_.reserve(traffic_char.op_ratio().size());
  int fixed_message_size =
      traffic_char.size_distribution_params_in_bytes().mean();
  for (const auto& op_ratio : traffic_char.op_ratio()) {
    // If `initiators` are specified, the follower needs to be one of them to
    // initiate.
    if (!op_ratio.initiators().empty()) {
      bool matched = false;
      for (const auto& initiator : op_ratio.initiators()) {
        if (initiator == traffic_pattern_.follower_id()) {
          matched = true;
          break;
        }
      }
      if (!matched) continue;
    }
    rdma_op_ratios.push_back(op_ratio.ratio());
    OpTypeSize op_type_size;
    if (traffic_char.message_size_distribution_type() ==
        proto::MESSAGE_SIZE_DISTRIBUTION_BY_OPS) {
      op_type_size.op_size = op_ratio.op_size();
    } else if (traffic_char.message_size_distribution_type() ==
               proto::MESSAGE_SIZE_DISTRIBUTION_FIXED) {
      op_type_size.op_size = fixed_message_size;
    }
    op_type_size.op_type = op_ratio.op_code();
    rdma_op_types_.push_back(op_type_size);
  }
  operation_picker_ = absl::discrete_distribution<int>(rdma_op_ratios.begin(),
                                                       rdma_op_ratios.end());

  for (const proto::QueuePairConfig& queue_pair_config :
       traffic_pattern_.queue_pairs()) {
    if (queue_pair_config.is_initiator()) {
      initiator_queue_pair_ids_.push_back(queue_pair_config.queue_pair_id());
    }
  }
  for (auto& it : queue_pairs_) {
    auto* queue_pair = it.second.get();
    queue_pair_pointers_.push_back(queue_pair);
    queue_pairs_by_ibv_qp_num_[queue_pair->GetIbvQpNum()] = queue_pair;

    // Create stats trackers for all op types/sizes initiated by this QP.
    for (const auto& rdma_op_type_size : rdma_op_types_) {
      if (queue_pair->IsInitiator()) {
        queue_pair->InstallStatsTrackerKey(
            std::pair{rdma_op_type_size.op_type, rdma_op_type_size.op_size});
      }
    }
  }
  initiator_queue_pair_ids_iterator_ = initiator_queue_pair_ids_.begin();

  VLOG(1) << "Created TrafficGenerator for pattern: " << traffic_pattern_;
}

absl::Status TrafficGenerator::ConnectQueuePairs(
    RemoteQueuePairAttributesFetcher& remote_qp_attributes_fetcher,
    std::shared_ptr<grpc::ChannelCredentials> creds) {
  auto start = absl::Now();
  absl::flat_hash_map<int32_t, proto::QueuePairAttributesRequest> peer_req_map;
  absl::flat_hash_map<int32_t, proto::QueuePairAttributesResponse>
      peer_resp_map;
  absl::flat_hash_map<int32_t, proto::ConnectionType> connection_type_map;

  // Find the peers to connect to and prepare one request per peer.
  for (auto& queue_pair_config : traffic_pattern_.queue_pairs()) {
    const int32_t queue_pair_id = queue_pair_config.queue_pair_id();
    int32_t peer_id = queue_pair_config.peer();
    proto::QueuePairAttributesRequest* req;
    if (!peer_req_map.contains(peer_id)) {
      peer_req_map[peer_id] = proto::QueuePairAttributesRequest();
      req = &peer_req_map[peer_id];
      req->set_requester_id(traffic_pattern_.follower_id());
      req->set_traffic_pattern_id(traffic_pattern_.global_traffic_pattern_id());
    }
    req = &peer_req_map[peer_id];
    req->add_queue_pair_ids(queue_pair_id);
    connection_type_map[queue_pair_id] = queue_pair_config.connection_type();
  }
  // Send the requests to the peers and receive the responses.
  for (auto& [peer_id, req] : peer_req_map) {
    absl::StatusOr<proto::QueuePairAttributesResponse> resp =
        remote_qp_attributes_fetcher.GetQueuePairAttributes(peer_id, req,
                                                            creds);
    if (!resp.ok()) {
      return absl::Status(
          resp.status().code(),
          absl::StrCat("TrafficGenerator failed to fetch queue pair attributes "
                       "from remote for queue pair with id ",
                       peer_id, ": ", resp.status().message()));
    }
    peer_resp_map[peer_id] = resp.value();
  }
  LOG(INFO) << "Fetching took " << absl::Now() - start;
  absl::Mutex mutex;
  absl::Status pool_status = absl::OkStatus();
  // If there are more than 1k queue pairs, we will use a thread per 1k.
  int kqps = (traffic_pattern_.queue_pairs_size() + 999) / 1000;
  // Limit the number of threads to 5.
  int num_workers = std::min(kqps, kMaxConnectThreads);
  utils::ThreadPool pool(num_workers);
  LOG(INFO) << "Connecting queue pairs with " << num_workers << " workers";
  // Iterate over the responses and connect to the remote queue pairs.
  // `ConnextToRemote` will do the RTR/RTS transition for each QP. As this can
  // take a long time, we do it in parallel.
  for (auto& [peer_id, resp] : peer_resp_map) {
    if (resp.remote_attribute_per_qp().empty()) {
      // For backward compatibility, keep this until completely deprecated.
      //
      auto remote_attribute = resp.remote_attributes();
      pool.Add([&]() {
        {
          absl::MutexLock lock(&mutex);
          if (!pool_status.ok()) return;
        }
        int32_t queue_pair_id = remote_attribute.queue_pair_id();
        absl::Status connect_status =
            queue_pairs_[queue_pair_id]->ConnectToRemote(remote_attribute);
        if (!connect_status.ok()) {
          absl::MutexLock lock(&mutex);
          pool_status = connect_status;
        }
      });
    }
    for (auto& remote_attribute : resp.remote_attribute_per_qp()) {
      pool.Add([&]() {
        {
          absl::MutexLock lock(&mutex);
          if (!pool_status.ok()) return;
        }
        int32_t queue_pair_id = remote_attribute.queue_pair_id();
        absl::Status connect_status =
            queue_pairs_[queue_pair_id]->ConnectToRemote(remote_attribute);
        if (!connect_status.ok()) {
          absl::MutexLock lock(&mutex);
          pool_status = connect_status;
          LOG(ERROR) << "Failed to connect queue pair " << queue_pair_id
                     << " to remote: " << connect_status;
        }
      });
    }
  }
  pool.Wait();
  if (!pool_status.ok()) {
    LOG(ERROR) << "Failed to connect queue pairs: " << pool_status;
    return pool_status;
  }
  LOG(INFO) << "ConnectQueuePairs took " << absl::Now() - start;
  return absl::OkStatus();
}

bool TrafficGenerator::Post() {
  if (absl::GetCurrentTimeNanos() >= stop_experiment_time_ns_) {
    post_finished_ = true;
    return false;
  }

  int should_post = load_generator_->ShouldPost();

  for (int i = 0; i < should_post; ++i) {
    QueuePair* queue_pair = GetNextQueuePair();
    if (queue_pair == nullptr) {
      VLOG(1)
          << "Follower " << traffic_pattern_.follower_id()
          << ": TrafficGenerator halting Post because there is no queue pair "
             "on which to issue operations.";
      post_finished_ = true;
      return false;
    }

    // Advance the number of outstanding operations before posting to avoid
    // races with the polling thread calling `Complete` on the load generator
    // for operations for which `Advance` has yet to finish.
    auto& characteristics = traffic_pattern_.traffic_characteristics();
    load_generator_->Advance(characteristics.batch_size());
    QueuePair::OpSignalType op_signal_type =
        QueuePair::OpSignalType::kSignalAll;
    if (characteristics.signal_only_last_op_in_batch()) {
      op_signal_type = QueuePair::OpSignalType::kLastInBatch;
    }
    absl::Status status = queue_pair->PostOps(
        GetOpTypeSize(), characteristics.batch_size(), op_signal_type);
    if (!status.ok()) {
      auto key = std::make_pair(status.code(), std::string(status.message()));
      if (failed_operation_posts_.contains(key)) {
        failed_operation_posts_[key] += 1;
      } else {
        failed_operation_posts_[key] = 1;
        LOG(ERROR) << "Error (first time) when posting operations: " << status;
      }
      // If the operation failed to post, we won't receive a completion.
      load_generator_->Complete(
          traffic_pattern_.traffic_characteristics().batch_size());
    }
    ++op_iteration_;
    if (max_ops_to_initiate_ > 0) {
      // Stop posting if max_ops_to_initiate_ is bigger than 0 and we reach that
      // number.
      if (++ops_initiated_ >= max_ops_to_initiate_) {
        post_finished_ = true;
        return false;
      }
    }
  }

  return true;
}

absl::Status TrafficGenerator::Cleanup() {
  // Wait for a short grace period to ensure initiator traffic generators drain
  // any outstanding operations before target QPs are destructed.
  absl::SleepFor(kCleanupGracePeriod);

  if (queue_pairs_.size() < 1000) {
    return absl::OkStatus();
  }
  // Speed up the destruction of queue pairs, when requested.
  utils::ThreadPool pool(kMaxCleanupThreads);
  for (auto& qp : queue_pairs_) {
    pool.Add([&]() { qp.second->Destroy(); });
  }
  pool.Wait();
  return absl::OkStatus();
}

bool TrafficGenerator::Poll() {
  uint64_t now_ns = absl::GetCurrentTimeNanos();
  // Check if we should stop or start collecting measurements.
  if (!started_measurements_ && (now_ns >= start_measurements_time_ns_)) {
    for (auto& qp : queue_pairs_) {
      qp.second->StartCollectingMeasurements();
    }
    started_measurements_ = true;
  } else if (need_to_resume_ && (now_ns >= start_measurements_time_ns_)) {
    for (auto& qp : queue_pairs_) {
      qp.second->ResumeCollectingMeasurements();
    }
    need_to_resume_ = false;
  } else if (!stopped_measurements_ && (now_ns >= stop_measurements_time_ns_)) {
    for (auto& qp : queue_pairs_) {
      qp.second->StopCollectingMeasurements();
    }
    stopped_measurements_ = true;
  }

  // Poll CompletionQueueManager until there are no available CQEs
  int num_completions = 0;
  do {
    auto result = completion_queue_manager_->Poll();
    if (!result.ok()) {
      LOG_EVERY_N(ERROR, 10000)
          << "Error when polling operations: " << result.status();
      auto key = std::make_pair(result.status().code(),
                                std::string(result.status().message()));
      if (failed_operation_polls_.contains(key)) {
        failed_operation_polls_[key] += 1;
      } else {
        failed_operation_polls_[key] = 1;
      }
      continue;
    }

    CompletionQueueManager::CompletionInfo& info = result.value();
    num_completions = info.num_completions;

    QueuePair* completion_qp = nullptr;
    uint32_t completion_qp_num = 0;
    int completed_op_count = 0;
    for (int i = 0; i < info.num_completions; ++i) {
      // Map qp_num in ibv_wc to source QueuePair, skipping subsequent wcs with
      // the same qp_num.
      if (info.completions[i].qp_num != completion_qp_num ||
          completion_qp == nullptr) {
        auto qp_it =
            queue_pairs_by_ibv_qp_num_.find(info.completions[i].qp_num);
        if (qp_it == queue_pairs_by_ibv_qp_num_.end()) {
          LOG(ERROR) << "Completion received for unknown qp_num "
                     << info.completions[i].qp_num;
          continue;
        }
        completion_qp = qp_it->second;
      }
      auto status_or_count = completion_qp->HandleExternalCompletion(
          info.completions[i], info.completion_before, info.completion_after);
      if (status_or_count.ok()) {
        completed_op_count += status_or_count.value();
      } else {
        LOG_EVERY_N(ERROR, 10000) << "Error handling wc: " << status_or_count;
      }
    }
    load_generator_->Complete(completed_op_count);
  } while (num_completions > 0);

  // End if abort has been requested by another thread.
  if (abort_.HasBeenNotified()) {
    return false;
  }

  // Determine whether we should continue polling.
  if (now_ns < stop_experiment_time_ns_) {
    // If the experiment is ongoing, we will have more operations to poll.
    return true;
  } else {
    // Because `stop_experiment_time` indicates when we should stop posting
    // operations, there may still be outstanding operations that need to be
    // polled after this time.
    if (!post_finished_) {
      return true;
    }

    int outstanding = 0;
    for (auto* queue_pair : queue_pair_pointers_) {
      outstanding += queue_pair->NumOutstandingOps();
    }
    if (outstanding == 0) {
      // If the experiment is over and there are no more outstanding operations,
      // we are done polling.
      return false;
    } else {
      if (now_ns < hard_cutoff_time_ns_) {
        // If there are outstanding operations after the end of the experiment,
        // wait for `kHardCutoffAfterExperiment` to see if the completions come
        // in.
        return true;
      } else {
        LOG(WARNING) << "Failed to drain the completion queue after "
                        "hard cutoff. Remaining outstanding operations: "
                     << outstanding;
        remaining_outstanding_ops_ = outstanding;
        return false;
      }
    }
  }
}

absl::Status TrafficGenerator::Prepare() {
  auto traffic_type = traffic_pattern_.traffic_type();
  SetAffinityIfNeeded(traffic_pattern_, true);
  if (traffic_type == proto::TRAFFIC_TYPE_BANDWIDTH) {
    if (traffic_pattern_.traffic_characteristics().op_ratio_size() != 1) {
      return absl::InvalidArgumentError(
          "bandwidth traffic should have exactly one op type");
    }
    auto rdma_op =
        traffic_pattern_.traffic_characteristics().op_ratio(0).op_code();
    if (rdma_op == proto::RDMA_OP_WRITE || rdma_op == proto::RDMA_OP_READ ||
        rdma_op == proto::RDMA_OP_SEND_RECEIVE) {
      special_traffic_generator_ = std::make_unique<BandwidthTraffic>(
          traffic_pattern_, queue_pairs_[0].get(),
          completion_queue_manager_.get(), &summary_);
    } else {
      return absl::InvalidArgumentError(
          "bandwidth traffic supports only WRITE / READ / SEND.");
    }
    return special_traffic_generator_->Prepare();
  }
  if (traffic_type == proto::TRAFFIC_TYPE_PINGPONG) {
    if (traffic_pattern_.traffic_characteristics().op_ratio_size() != 1) {
      return absl::InvalidArgumentError(
          "Pingpong traffic should have exactly one op type");
    }

    auto rdma_op =
        traffic_pattern_.traffic_characteristics().op_ratio(0).op_code();
    ibv_wr_opcode op_code;
    switch (rdma_op) {
      case proto::RDMA_OP_SEND_RECEIVE:
        op_code = IBV_WR_SEND;
        special_traffic_generator_ = std::make_unique<SendPingPongTraffic>(
            traffic_pattern_, GetNextQueuePair(),
            completion_queue_manager_.get(), &summary_);
        return special_traffic_generator_->Prepare();
        break;
      case proto::RDMA_OP_WRITE:
        op_code = IBV_WR_RDMA_WRITE;
        special_traffic_generator_ = std::make_unique<WritePingPongTraffic>(
            traffic_pattern_, GetNextQueuePair(),
            completion_queue_manager_.get(), &summary_);
        return special_traffic_generator_->Prepare();
      default:
        return absl::InvalidArgumentError(
            "Pingpong supports only SEND or WRITE");
    }
  }
  return absl::OkStatus();
}

absl::Status TrafficGenerator::Run() {
  if (queue_pairs_.empty()) {
    return absl::FailedPreconditionError("QueuePair empty, can't run");
  }
  // If the traffic is run repeatedly, we have stopped the measurement. Request
  // resume measurement.
  stopped_measurements_ = false;
  if (started_measurements_) {
    need_to_resume_ = true;
  }
  int64_t start_experiment_time_ns =
      absl::GetCurrentTimeNanos() +
      absl::ToInt64Nanoseconds(
          ToDuration(traffic_pattern_.traffic_characteristics().delay_time()));
  int64_t buffer_time_ns = absl::ToInt64Nanoseconds(
      ToDuration(traffic_pattern_.traffic_characteristics().buffer_time()));
  start_measurements_time_ns_ = start_experiment_time_ns + buffer_time_ns;
  stop_measurements_time_ns_ =
      start_measurements_time_ns_ +
      absl::ToInt64Nanoseconds((ToDuration(
          traffic_pattern_.traffic_characteristics().experiment_time())));
  stop_experiment_time_ns_ = stop_measurements_time_ns_ + buffer_time_ns;
  hard_cutoff_time_ns_ =
      stop_experiment_time_ns_ +
      absl::ToInt64Nanoseconds(utils::kHardCutoffAfterExperiment);
  max_ops_to_initiate_ =
      traffic_pattern_.traffic_characteristics().max_ops_to_initiate();
  ops_initiated_ = 0;

  absl::SleepFor(absl::FromUnixNanos(start_experiment_time_ns) - absl::Now());
  // Special traffics are handled by special traffic generator.
  auto traffic_type = traffic_pattern_.traffic_type();
  if (traffic_type == proto::TRAFFIC_TYPE_PINGPONG) {
    VLOG(1) << "Start Pingpong";
    SetAffinityIfNeeded(traffic_pattern_, true);
    return special_traffic_generator_->Run();
  }
  if (traffic_type == proto::TRAFFIC_TYPE_BANDWIDTH) {
    SetAffinityIfNeeded(traffic_pattern_, true);
    return special_traffic_generator_->Run();
  }

  bool enable_tracing = traffic_pattern_.metrics_collection().enable_tracing();
  bool enable_pause =
      enable_tracing && traffic_pattern_.queue_pairs(0).is_initiator();
  for (auto& qp : queue_pairs_) {
    qp.second->EnableTracing(enable_tracing);
  }

  int warmup =
      traffic_pattern_.metrics_collection().tracing_warmup_iterations();
  op_iteration_ = 0;
  bool keep_posting = true;
  SetAffinityIfNeeded(traffic_pattern_, true);
  LOG(INFO) << "Start experiment at approximately  " << absl::Now()
            << ", start measurements at " << start_measurements_time_ns_
            << ", stop measurements at " << stop_measurements_time_ns_
            << ", stop experiment at " << stop_experiment_time_ns_ << ".";
  load_generator_->Start();
  while (Poll()) {
    if (keep_posting) {
      keep_posting = Post();
      if (enable_pause && warmup == 0) {
        if (op_iteration_ == kTracingPauseAt) {
          auto until = absl::GetCurrentTimeNanos() + 1e9;
          while (absl::GetCurrentTimeNanos() < until) {
            Poll();
          }
          VLOG(1) << "Start trace: will pause for 10 seconds, post "
                     "3 ops, busy polling without posting for 10 seconds, "
                     "print a message, keep busy polling for 10 seconds: "
                  << traffic_pattern_.follower_id();
          until = absl::GetCurrentTimeNanos() + 1e10;
          while (absl::GetCurrentTimeNanos() < until) {
            Poll();
          }
        }
        if (op_iteration_ == kTracingStopAt) {
          auto until = absl::GetCurrentTimeNanos() + 5e9;
          post_finished_ = true;
          while (absl::GetCurrentTimeNanos() < until) {
            Poll();
          }
          VLOG(1) << "Stop tracing; will resume in 5 seconds";
          until = absl::GetCurrentTimeNanos() + 5e9;
          while (absl::GetCurrentTimeNanos() < until) {
            Poll();
          }
          VLOG(1) << "Resuming now";
          keep_posting = false;
        }
      }
      if (op_iteration_ == kTracingLatencyBufferSize) {
        --warmup;
        op_iteration_ = 0;
      }
    }
  }
  VLOG(1) << "Finished posting and polling at: " << absl::Now();

  // If buffer time is zero, it's possible that we won't have collected
  // measurements for the last second before exiting the poll loop.
  if (!stopped_measurements_) {
    for (auto* qp : queue_pair_pointers_) {
      qp->StopCollectingMeasurements();
    }
    stopped_measurements_ = true;
  }

  // Fills in some basic summary.
  std::vector<folly::TDigest> tdigests;
  proto::ThroughputResult total_tput = utils::MakeThroughputResult(0, 0);
  int64_t total_completions = 0;
  ::google::protobuf::Duration config_expr_time =
      traffic_pattern_.traffic_characteristics().experiment_time();
  int32_t experiment_time_seconds =
      absl::Seconds(config_expr_time.seconds()) / absl::Seconds(1);
  if (config_expr_time.nanos() > 0) ++experiment_time_seconds;
  // Prepares for no_op_time_sec (defined in verbsmarks.proto) calculation.
  // `no_op_time_set` variable is used to facilitate the calculation, which
  //  1) initialized with all the seconds in the experiments.
  //  2) if any ops in any queue pair is recorded for a specific second `s`,
  //     `s` will be removed from set.
  //  3) no_op_time_sec = the number of seconds left in `no_op_time_set`.
  absl::flat_hash_set<int> no_op_time_set = {};
  for (int i = 0; i < experiment_time_seconds; ++i) {
    no_op_time_set.insert(i);
  }
  for (auto& it : queue_pairs_) {
    proto::PerTrafficResult::PerQueuePairResult* per_queue_pair_result =
        summary_.add_per_queue_pair_results();
    per_queue_pair_result->set_queue_pair_id(it.first);

    std::unique_ptr<QueuePair>& queue_pair = it.second;
    total_completions += queue_pair->NumCompletedOperations();
    queue_pair->AccessStatistics(
        [per_queue_pair_result, &tdigests, &total_tput](
            const absl::flat_hash_map<std::pair<proto::RdmaOp, int32_t>,
                                      QueuePair::StatisticsTracker>&
                statistics_trackers,
            const utils::LatencyTracker& post_latency_tracker,
            const utils::LatencyTracker& poll_latency_tracker) {
          for (auto& it : statistics_trackers) {
            const auto& key_pair = it.first;
            const QueuePair::StatisticsTracker& stats_tracker = it.second;
            proto::PerTrafficResult::PerOpTypeSizeResult* tracker_result =
                per_queue_pair_result->add_per_op_type_size_results();

            *tracker_result = stats_tracker.GetFinalResults();
            // Fill in the op type/size, which are not set by GetFinalResults().
            tracker_result->set_op_code(key_pair.first);
            tracker_result->set_op_size(key_pair.second);

            // Combine tracker results into overall throughput/latency.
            utils::AppendThroughputResult(total_tput,
                                          tracker_result->throughput());
            tdigests.push_back(stats_tracker.GetLatencyTdigest());
          }
          google::protobuf::RepeatedPtrField<proto::Metric> post_metrics =
              post_latency_tracker.Dump();
          per_queue_pair_result->mutable_metrics()->MergeFrom(post_metrics);
          google::protobuf::RepeatedPtrField<proto::Metric> poll_metrics =
              poll_latency_tracker.Dump();
          per_queue_pair_result->mutable_metrics()->MergeFrom(poll_metrics);
        });

    // Remove the no_op second record if there are ops recorded in that second.
    for (const auto& op_type_result :
         per_queue_pair_result->per_op_type_size_results()) {
      if (no_op_time_set.empty()) {
        break;
      }
      for (const auto& statistic :
           op_type_result.statistics_per_second().statistics()) {
        if (statistic.throughput().ops_per_second() > 0) {
          no_op_time_set.erase(statistic.throughput().seconds_from_start());
        }
      }
    }
    VLOG(2) << *per_queue_pair_result;
  }
  summary_.set_no_op_time_sec(no_op_time_set.size());
  summary_.set_global_traffic_id(traffic_pattern_.global_traffic_pattern_id());
  uint32_t total_failed_operation_posts = 0;
  for (const auto& [key, value] : failed_operation_posts_) {
    total_failed_operation_posts += value;
  }
  summary_.set_num_failed_post_operations(total_failed_operation_posts);
  uint32_t total_failed_operation_polls = 0;
  for (const auto& [key, value] : failed_operation_polls_) {
    total_failed_operation_polls += value;
  }
  summary_.set_num_failed_poll_operations(total_failed_operation_polls);
  summary_.set_num_remaining_outstanding_ops(remaining_outstanding_ops_);
  summary_.set_num_completions(total_completions);
  folly::TDigest overall_tdigest = folly::TDigest::merge(tdigests);
  summary_.set_num_latency_obtained(overall_tdigest.count());
  if (overall_tdigest.count() > 0) {
    *summary_.mutable_latency() = utils::LatencyTdigestToProto(overall_tdigest);
  }
  *summary_.mutable_throughput() = total_tput;

  if (remaining_outstanding_ops_ > 0 &&
      !absl::GetFlag(FLAGS_ignore_outstanding_ops_at_cutoff)) {
    LOG(ERROR) << "Summary at failing point: " << summary_;
    return absl::DeadlineExceededError(
        absl::StrCat("Failed to drain outstanding operations, with ",
                     remaining_outstanding_ops_, " remaining ops at end time"));
  }

  int required_completions = traffic_pattern_.traffic_characteristics()
                                 .required_completions_for_success();
  if (required_completions > 0 && !initiator_queue_pair_ids_.empty()) {
    if (required_completions > total_completions) {
      LOG(ERROR) << "Summary at failing point: " << summary_;
      return absl::InternalError(absl::StrCat(
          "Failed to finish required completion: ", required_completions,
          " required, ", total_completions, " actually completed, ",
          total_failed_operation_polls, " failed to poll, ",
          total_failed_operation_posts, " failed to post"));
    }
  }

  return absl::OkStatus();
}

absl::Status TrafficGenerator::PopulateRemoteQueuePairAttributes(
    const int32_t queue_pair_id, proto::RemoteQueuePairAttributes& attrs) {
  if (const auto& it = queue_pairs_.find(queue_pair_id);
      it == queue_pairs_.end()) {
    return absl::InvalidArgumentError(
        absl::StrCat("Cannot get remote attributes for queue pair with id ",
                     queue_pair_id, " because the id is invalid."));
  } else {
    std::unique_ptr<QueuePair>& queue_pair = it->second;
    return queue_pair->PopulateRemoteAttributes(attrs);
  }
}

absl::Status TrafficGenerator::PopulateAttributesForQps(
    const proto::QueuePairAttributesRequest request,
    proto::QueuePairAttributesResponse& response) {
  for (const auto& queue_pair_id : request.queue_pair_ids()) {
    if (const auto& it = queue_pairs_.find(queue_pair_id);
        it == queue_pairs_.end()) {
      return absl::InvalidArgumentError(
          absl::StrCat("Cannot get remote attributes for queue pair with id ",
                       queue_pair_id, " because the id is invalid."));
    } else {
      std::unique_ptr<QueuePair>& queue_pair = it->second;
      auto qp_attr = response.add_remote_attribute_per_qp();
      qp_attr->set_queue_pair_id(queue_pair_id);
      auto status = queue_pair->PopulateRemoteAttributes(*qp_attr);
      if (!status.ok()) {
        LOG(ERROR) << "PopulateRemoteAttributes failed: " << status;
        return status;
      }
    }
  }
  return absl::OkStatus();
}

const proto::PerTrafficResult& TrafficGenerator::GetSummary() {
  return summary_;
}

proto::CurrentStats::TrafficCurrentStats
TrafficGenerator::GetCurrentStatistics() {
  proto::CurrentStats::TrafficCurrentStats traffic_current_stats;
  traffic_current_stats.set_global_traffic_id(
      traffic_pattern_.global_traffic_pattern_id());

  for (auto& qp : queue_pairs_) {
    proto::CurrentStats::QueuePairCurrentStats qp_stats =
        qp.second->GetCurrentStatistics();
    if (qp_stats.per_op_type_size_current_stats_size() > 0) {
      *traffic_current_stats.add_queue_pair_current_stats() = qp_stats;
    }
  }
  return traffic_current_stats;
}

void TrafficGenerator::Abort() {
  abort_.Notify();
  if (special_traffic_generator_) {
    special_traffic_generator_->Abort();
  }
}

}  // namespace verbsmarks
