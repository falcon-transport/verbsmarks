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

#ifndef VERBSMARKS_VERBSMARKS_FOLLOWER_H_
#define VERBSMARKS_VERBSMARKS_FOLLOWER_H_

#include <memory>
#include <optional>
#include <string>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/flags/flag.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "connection_coordinator.grpc.pb.h"
#include "connection_coordinator.h"
#include "connection_coordinator.pb.h"
#include "grpcpp/security/server_credentials.h"
#include "grpcpp/server_context.h"
#include "grpcpp/support/status.h"
#include "ibverbs_utils.h"
#include "infiniband/verbs.h"
#include "memory_manager.h"
#include "traffic_generator.h"
#include "utils.h"
#include "verbsmarks.grpc.pb.h"
#include "verbsmarks.pb.h"
#include "verbsmarks_binary_flags.h"

namespace grpc {
class ServerCredentials;
}  // namespace grpc

namespace verbsmarks {

// Default interval for sending follower heartbeats to the leader. Should be,
// at most, half the leader's heartbeat check interval.
static constexpr absl::Duration kDefaultHeartbeatSendInterval =
    absl::Seconds(1);

// VerbsMarks follower. It joins the leader's quorum. It generates/receives
// traffic to/from other followers based on configuration and measures
// performance metrics.
// A follower's lifecycle is the following:
// - Join Quorum -> QuorumAchieved
// - Request Traffic Patterns (can get multiple)
// - Generate Traffics
// - Export Results
// - Leave Quorum
// Once a lifecycle is over, follower terminates. If we decide that there is a
// value of allowing multiple lifecycle, we can reconsider; it would not require
// significant design changes.
// This class is thread compatible.
class VerbsMarksFollower {
 public:
  //
  VerbsMarksFollower(
      const std::shared_ptr<grpc::ServerCredentials>& server_credentials,
      int local_port, const absl::string_view local_ip,
      const absl::string_view alias,
      proto::VerbsMarks::StubInterface* leader_stub,
      const absl::string_view device_name = "")
      : leader_stub_(leader_stub),
        heartbeat_send_interval_(absl::InfiniteDuration()),
        http_server_addr_(utils::HostPortString(local_ip, local_port)),
        server_credentials_(server_credentials),
        alias_(alias),
        verbs_context_(ibverbs_utils::OpenDeviceOrDie(device_name)),
        local_ibverbs_address_(
            ibverbs_utils::GetLocalAddressOrDie(verbs_context_.get())),
        traffic_generators_ready_(false) {
    leader_address_ = utils::HostPortString(absl::GetFlag(FLAGS_leader_address),
                                            absl::GetFlag(FLAGS_leader_port));
  }

  // Start connection coordinator server so that peers can request queue pair
  // information.
  void StartConnectionCoordinatorServer();

  // Run a lifecycle of a follower from Join to FinishExperiment.
  absl::Status Run();

  absl::Status JoinQuorum();

  // Requests per follower traffic patterns and prepare resources for
  // experiment.
  absl::Status GetReady() ABSL_LOCKS_EXCLUDED(traffic_generators_mutex_);

  // Requests StartExperiment results and once received, start traffic.
  absl::Status StartExperiment() ABSL_LOCKS_EXCLUDED(traffic_generators_mutex_);

  // Checks whether the follower's TrafficGenerator threads have finished.
  // Returns true if all threads finished successfully, false if still running,
  // or if any thread ended with an error, returns the error status of the first
  // failed thread.
  virtual absl::StatusOr<bool> CheckFinished()
      ABSL_LOCKS_EXCLUDED(traffic_generators_mutex_);

  // Sends heartbeats to the leader with current experiment status. Continues
  // until heartbeat response contains an "abort" status from the leader, or
  // until status_of_traffic_generators_ indicates the experiment has finished
  // or a failure has occurred. Returns ok status if the experiment finishes
  // without error. Returns aborted status if the leader aborts the experiment
  // or a local error occurs.
  absl::Status SendHeartbeatsUntilFinished();

  // Returns the concatenated results from all traffic patterns that were
  // executed.
  proto::ResultRequest GetResults()
      ABSL_LOCKS_EXCLUDED(traffic_generators_mutex_);

  // Checks status of individual traffic generation result and send
  // a ResultRequest. Once it receives ResultRequest, the follower can destroy
  // all the resources.
  absl::Status FinishExperiment();

  virtual ~VerbsMarksFollower() = default;

 private:
  class ConnectionCoordinatorService
      : public proto::ConnectionCoordinator::Service {
   public:
    explicit ConnectionCoordinatorService(VerbsMarksFollower* follower)
        : follower_(follower) {}
    grpc::Status GetQueuePairAttributes(
        grpc::ServerContext* context,
        const proto::QueuePairAttributesRequest* request,
        proto::QueuePairAttributesResponse* response) override;

   private:
    VerbsMarksFollower* follower_;  // Reference to outer class so that we can
                                    // access private traffic generators.
  };

  absl::Status SendStartExperimentRequest(int raw_code);

  // Retrieves current experiment stats while traffic generators are running,
  // calling TrafficGenerator::GetCurrentStatistics for each TG.
  proto::CurrentStats GetCurrentStatistics();

  std::string leader_address_;

  // Stub to the leader service.
  proto::VerbsMarks::StubInterface* leader_stub_;

  // Period for follower-initiated heartbeats to the leader.
  absl::Duration heartbeat_send_interval_;

  // Followers indexed by their numeric IDs. Their information will be used for
  // any OOB communication and also the traffic peers will be described by their
  // IDs.
  absl::flat_hash_map<int, verbsmarks::proto::Follower> followers_;

  // The follower's server address; used for OOB communication.
  const std::string http_server_addr_;
  // The server credentials to use on connection coordinator server.
  const std::shared_ptr<grpc::ServerCredentials>& server_credentials_;

  // A follower alias if provided, for debugging purpose. This is not necessary
  // and http_server_addr_ will be used if not provided. We will evaluate its
  // usefulness when we have actual traffic going on.
  const std::string alias_;

  int my_id_;

  // The context corresponding to an open ibverbs device.
  std::unique_ptr<ibv_context, ibverbs_utils::ContextDeleter> verbs_context_;
  // A valid address corresponding to `verbs_context_`.
  ibverbs_utils::LocalIbverbsAddress local_ibverbs_address_;

  // Allocating and holding all protection domains, memory regions and memory
  // blocks in this follower.
  MemoryManager memory_manager_;

  // The traffic generators that post ibverbs operations.
  absl::Mutex traffic_generators_mutex_;
  absl::flat_hash_map<int, std::unique_ptr<TrafficGenerator>>
      traffic_generators_ ABSL_GUARDED_BY(traffic_generators_mutex_);
  bool traffic_generators_ready_ ABSL_GUARDED_BY(traffic_generators_mutex_);
  // Status of StartTraffic indexed by the global traffic pattern ids.
  absl::flat_hash_map<int, absl::Status> status_of_traffic_generators_
      ABSL_GUARDED_BY(traffic_generators_mutex_);

  // Server that allows peers to request information necessary to connect queue
  // pairs.
  std::optional<ConnectionCoordinatorServer> connection_coordinator_server_;

  // Start a traffic for a traffic pattern. The result should be written at
  // status_of_traffic_patterns_.
  void StartTraffic(int traffic_pattern_id, TrafficGenerator* traffic_generator)
      ABSL_LOCKS_EXCLUDED(traffic_generators_mutex_);

  // Not movable or copyable.
  VerbsMarksFollower(const VerbsMarksFollower&) = delete;
  VerbsMarksFollower& operator=(const VerbsMarksFollower&) = delete;
};

}  // namespace verbsmarks

#endif  // VERBSMARKS_VERBSMARKS_FOLLOWER_H_
