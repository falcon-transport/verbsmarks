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

#ifndef VERBSMARKS_TRAFFIC_GENERATOR_H_
#define VERBSMARKS_TRAFFIC_GENERATOR_H_

#include <stdint.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/random/discrete_distribution.h"
#include "absl/random/random.h"
#include "absl/random/uniform_int_distribution.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "absl/synchronization/notification.h"
#include "completion_queue_manager.h"
#include "connection_coordinator.h"
#include "connection_coordinator.pb.h"
#include "folly/stats/TDigest.h"
#include "grpcpp/security/credentials.h"
#include "ibverbs_utils.h"
#include "infiniband/verbs.h"
#include "load_generator.h"
#include "memory_manager.h"
#include "queue_pair.h"
#include "special_traffic_generator.h"
#include "throughput_computer.h"
#include "utils.h"
#include "verbsmarks.pb.h"

namespace verbsmarks {

// QueuePairSelector generates the ID of next QueuePair to generate traffic on.
class QueuePairSelector {
 public:
  virtual ~QueuePairSelector() = default;
  virtual uint32_t GetNextQueuePairId() = 0;
};

// RoundRobinQueuePairSelector returns the ID of initiator queue pairs in the
// round robin manner. Used in explicit traffic.
class RoundRobinQueuePairSelector : public QueuePairSelector {
 public:
  explicit RoundRobinQueuePairSelector(
      const proto::PerFollowerTrafficPattern &pattern) {
    for (const auto &queue_pair : pattern.queue_pairs()) {
      if (queue_pair.is_initiator()) {
        initiator_queue_pair_ids_.emplace_back(queue_pair.queue_pair_id());
      }
    }
    num_initiators_ = initiator_queue_pair_ids_.size();
    // GetNextQueuePairId increments before using the position and restarts when
    // it matches to num_initiators_. To start at position 0, we set this to
    // num_initiators_ - 1.
    position_ = num_initiators_ - 1;
  }

  uint32_t GetNextQueuePairId() override {
    if (++position_ == num_initiators_) {
      position_ = 0;
    }
    return initiator_queue_pair_ids_[position_];
  }

 private:
  uint32_t num_initiators_;
  // Holds the IDs of initiating QPs.
  std::vector<int> initiator_queue_pair_ids_;
  // The position in the initiator_queue_pair_ids_.
  uint32_t position_;
};

// UniformRandomQueuePairSelector selects a queue pair ID to generate traffic on
// using uniform random distribution.
class UniformRandomQueuePairSelector : public QueuePairSelector {
 public:
  explicit UniformRandomQueuePairSelector(
      const proto::PerFollowerTrafficPattern &pattern) {
    for (const auto &queue_pair : pattern.queue_pairs()) {
      if (queue_pair.is_initiator()) {
        initiator_queue_pair_ids_.emplace_back(queue_pair.queue_pair_id());
      }
    }
  }

  uint32_t GetNextQueuePairId() override {
    absl::InsecureBitGen gen;
    uint32_t position = absl::uniform_int_distribution<uint32_t>(
        0, initiator_queue_pair_ids_.size() - 1)(gen);
    return initiator_queue_pair_ids_[position];
  }

 private:
  std::vector<int> initiator_queue_pair_ids_;
};

// The TrafficGenerator class generates a stream of ibverbs traffic across
// multiple queue pairs according to the specification it is configured with.
// This class also generates results and statistics after execution.
class TrafficGenerator {
 public:
  // Factory function for TrafficGenerator. Creates QueuePairs according to the
  // traffic pattern and pass them to the constructor. If `override_queue_pairs`
  // or `override_completion_queue_manager` are provided, these objects will be
  // used instead of creating them internally (for testing purposes).
  // logic (testing purpose).
  static absl::StatusOr<std::unique_ptr<TrafficGenerator>> Create(
      ibv_context *context, ibverbs_utils::LocalIbverbsAddress local_address,
      proto::PerFollowerTrafficPattern traffic_pattern,
      absl::flat_hash_map<int32_t, std::unique_ptr<QueuePair>>
          override_queue_pairs = {},
      const MemoryManager *memory_manager = nullptr,
      std::unique_ptr<CompletionQueueManager>
          override_completion_queue_manager = nullptr);

  // Handles any preparation that happens after initialization and connecting
  // QPs. e.g. pingpong preparation.
  absl::Status Prepare();

  // Destroy qps. Optional.
  absl::Status Cleanup();

  ~TrafficGenerator() { Cleanup().IgnoreError(); }

  std::unique_ptr<SpecialTrafficGenerator> special_traffic_generator_;

  // Connects all queue pairs in the traffic pattern to their remote
  // counterparts, using `remote_qp_attributes_fetcher` for network requests.
  // This method blocks on network communication.
  absl::Status ConnectQueuePairs(
      RemoteQueuePairAttributesFetcher &remote_qp_attributes_fetcher,
      std::shared_ptr<grpc::ChannelCredentials> creds =
          utils::GetGrpcChannelCredentials());

  // Based on the proto used to configure this instance, determines when to
  // issue new operations, and what the type and size of those operations should
  // be. Between new operations, triggers each queue pair to poll for
  // completions.
  absl::Status Run();

  // Asks the queue pair with `queue_pair_id` to fill in `attrs`. Returns an
  // error if `queue_pair_id` is invalid.
  absl::Status PopulateRemoteQueuePairAttributes(
      int32_t queue_pair_id, proto::RemoteQueuePairAttributes &attrs);

  // Populate remote queue pair attributes for multiple queue pairs. Returns an
  // error if any queue pair id is invalid or the QP is in a bad state.
  absl::Status PopulateAttributesForQps(
      proto::QueuePairAttributesRequest request,
      proto::QueuePairAttributesResponse &response);

  // Retrieves per-second statistics from each QP, returning any metrics that
  // are available since the previous call to this function. This is thread-
  // safe, intended to be called by a separate thread from the TrafficGenerator.
  proto::CurrentStats::TrafficCurrentStats GetCurrentStatistics();

  // Returns the aggregated results of each queue pair in this traffic pattern.
  const proto::PerTrafficResult &GetSummary();

  // Shuts down the traffic generator immediately due to an external error, by
  // setting `stop_experiment_time_ns_` to the current time and
  // SpecialTrafficGenerator `iterations_` to zero. May be called by a separate
  // thread (e.g., the main follower thread).
  void Abort();

 private:
  TrafficGenerator(
      ibv_context *context, ibverbs_utils::LocalIbverbsAddress local_address,
      proto::PerFollowerTrafficPattern traffic_pattern,
      std::unique_ptr<LoadGenerator> load_generator,
      std::unique_ptr<QueuePairSelector> queue_pair_selector_,
      absl::flat_hash_map<int32_t, std::unique_ptr<QueuePair>> queue_pairs,
      std::unique_ptr<CompletionQueueManager> completion_queue_manager);

  // Initializes CompletionQueueManager based on the TrafficGenerator's
  // traffic pattern. Returns null for traffic types which do not utilize the
  // CompletionQueueManager. Used internally by TrafficGenerator factory.
  static absl::StatusOr<std::unique_ptr<CompletionQueueManager>>
  InitializeCompletionQueueManager(
      ibv_context *context, proto::PerFollowerTrafficPattern traffic_pattern);

  // Returns next RDMA operation and its size to issue based on the traffic
  // pattern.
  OpTypeSize GetOpTypeSize();

  // RDMA OP ratio and the corresponding types and sizes.
  std::vector<OpTypeSize> rdma_op_types_;
  std::optional<absl::discrete_distribution<int>> operation_picker_;
  absl::BitGen gen_;

  // Returns next QueuePair to post an operation based on the traffic pattern.
  // Returns nullptr if there is no initiating queue pair.
  QueuePair *GetNextQueuePair();

  // If `time_to_post_next_op` has passed and `stop_experiment_time` has not yet
  // passed, picks the appropriate operation type, size, and queue pair on which
  // to issue an operation, and issues the operation. When an operation is
  // posted, updates `time_to_post_next_op` with the time after which the next
  // operation should be posted. Returns true if posting should continue, false
  // if posting has completed (i.e. if the experiment is over or if there are no
  // queue pairs on which to issue operations).
  bool Post();

  // Polls completions via CompletionQueueManager for all queue pairs.
  bool Poll();

  // For tracing; keeps track of the op iteration so we can pause and stop
  // experiment accordingly.
  int op_iteration_;

  // If bigger than 0, do not initiate more than max_ops_to_initiate_.
  int32_t max_ops_to_initiate_;
  int32_t ops_initiated_;

  absl::Mutex mutex_;
  bool post_finished_;  // ABSL_GUARDED_BY(mutex_);

  // Determines when operations should be posted, according to the attributes of
  // the traffic pattern.
  std::unique_ptr<LoadGenerator> load_generator_;

  // Chooses the next queue pair ID based on the distribution.
  std::unique_ptr<QueuePairSelector> queue_pair_selector_;

  bool use_event_;

  // The set of queue pair ids on which operations should be issued.
  std::vector<int32_t> initiator_queue_pair_ids_;

  // An iterator for `initiator_queue_pair_ids_` that keeps tracks of the id of
  // the next queue pair on which to issue an operation.
  std::vector<int32_t>::iterator initiator_queue_pair_ids_iterator_;

  // Map to count failed posted ops by error type. Maps status code/message to
  // failure count.
  absl::flat_hash_map<std::pair<absl::StatusCode, std::string>, uint32_t>
      failed_operation_posts_;

  // Map to count failed poll ops by error type. Maps status code/message to
  // failure count. Does not include poll operations returning no completions.
  absl::flat_hash_map<std::pair<absl::StatusCode, std::string>, uint32_t>
      failed_operation_polls_;

  // The number of ops remaining after the experiment finishes.
  uint32_t remaining_outstanding_ops_ = 0;

  const proto::PerFollowerTrafficPattern traffic_pattern_;
  proto::PerTrafficResult summary_;

  // CQ manager, if utilized by the specified traffic type, otherwise nullptr.
  std::unique_ptr<CompletionQueueManager> completion_queue_manager_;

  // Map from queue pair id to queue pair.
  absl::flat_hash_map<int32_t, std::unique_ptr<QueuePair>> queue_pairs_;
  // A vector of references to Queue Pairs for slightly faster iteration.
  std::vector<QueuePair *> queue_pair_pointers_;
  // Map of queue pair pointers by internal ibverbs qp_num, to match a
  // completion to the corresponding QueuePair.
  absl::flat_hash_map<uint32_t, QueuePair *> queue_pairs_by_ibv_qp_num_;

  // State for tracking whether the traffic generator has started or stopped
  // measurements. These are only accessed by polling thread, so no lock needed.
  bool started_measurements_;
  bool stopped_measurements_;
  bool need_to_resume_;

  // Timestamps to stop and start measurements, end experiment, and hard cutoff
  // to complete outstanding operations.
  int64_t start_measurements_time_ns_;
  int64_t stop_measurements_time_ns_;
  int64_t stop_experiment_time_ns_;
  int64_t hard_cutoff_time_ns_;

  // Traffic generator level metrics.
  ThroughputComputer throughput_computer_;
  folly::TDigest latency_tdigest_;

  // Abort notification to end experiment before it is finished, set via Abort()
  // method, called by follower main thread.
  absl::Notification abort_;
};

}  // namespace verbsmarks

#endif  // VERBSMARKS_TRAFFIC_GENERATOR_H_
