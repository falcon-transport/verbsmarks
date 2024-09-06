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

#ifndef VERBSMARKS_VERBSMARKS_LEADER_H_
#define VERBSMARKS_VERBSMARKS_LEADER_H_

#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <optional>
#include <thread>
#include <tuple>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/container/btree_map.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "google/rpc/status.pb.h"
#include "verbsmarks.pb.h"

namespace verbsmarks {

// Default interval for checking follower heartbeats at the leader. Should be,
// at minimum, twice the follower heartbeat send interval.
static constexpr absl::Duration kDefaultHeartbeatCheckInterval =
    absl::Seconds(5);

// Interval for saving periodic report output files with experiment stats, if
// flag is enabled.
static constexpr absl::Duration kPeriodicResultSaveInterval = absl::Seconds(5);

// Quorum request and a handler to process the response.
struct QuorumArrival {
  verbsmarks::proto::QuorumRequest* request;
  std::unique_ptr<std::function<void(verbsmarks::proto::QuorumResponse)>>
      response_handler;
};

// Helper class for tracking follower health as leader receives heartbeats.
class FollowerHealthTracker {
 public:
  explicit FollowerHealthTracker(
      absl::Duration heartbeat_check_interval = kDefaultHeartbeatCheckInterval)
      : tracking_started_(false),
        experiment_healthy_(true),
        heartbeat_check_interval_(heartbeat_check_interval) {}

  // Begins tracking follower heartbeats given the set of follower ids.
  void BeginTracking(const absl::flat_hash_set<int32_t>& follower_ids);

  // Updates health status when a follower heartbeat message is received.
  void RegisterHeartbeat(int32_t follower_id, bool is_healthy);

  // Stops tracking heartbeats for a follower that has finished executing.
  void RegisterFinished(int32_t follower_id);

  // Returns true if the overall experiment is healthy.
  bool ExperimentIsHealthy();

 private:
  bool tracking_started_;
  bool experiment_healthy_;
  absl::flat_hash_set<int32_t> follower_set_;
  absl::flat_hash_set<int32_t> follower_heartbeats_received_;
  absl::Duration heartbeat_check_interval_;
  absl::Time heartbeat_interval_start_;
};

// Helper class to receive stats sent by followers during experiment runtime,
// output real-time metrics, and generate final result output.
class StatisticsCollector {
 public:
  // Stores per-second statistics received from a follower.
  void CollectStatistics(const proto::CurrentStats& follower_stats,
                         int follower_id);

  // Stores per-second statistics received from a follower's final ResultReport.
  void CollectFinalReportStatistics(proto::ResultReport* report);

  // Writes all collected per-second statistics into the provided ResultReport.
  // ResultReport should already include cumulative stats received from each
  // follower. This function will reorder any per-second stats in the report
  // proto to ensure correct sequential order.
  void WriteFinalReport(proto::ResultReport* report);

  // Generates a ResultReport with periodic statistics, then clears the stats
  // from StatisticsCollector storage. If no stats are available for output,
  // returns nullopt.
  std::optional<proto::PeriodicResultReport> WritePeriodicReport();

 private:
  using StatsKey = std::tuple<int, int, int, proto::RdmaOp, int>;
  static StatsKey MakeStatsKey(int follower_id, int traffic_id,
                               int queue_pair_id, proto::RdmaOp op_type,
                               int op_size) {
    return std::tie(follower_id, traffic_id, queue_pair_id, op_type, op_size);
  }

  // Ordered containers to allow for efficient ResultReport output.
  std::map<StatsKey, absl::btree_map<int, proto::Statistics>> stats_;
};

class VerbsMarksLeaderInterface {
 public:
  virtual ~VerbsMarksLeaderInterface() = default;

  // Adds a follower to the quorum. When the quorum is formed, a response with
  // all follower information will be passed to the response_handler. On
  // failure, a response with a failure code is passed to the response_handler.
  virtual void SeekQuorum(
      verbsmarks::proto::QuorumRequest*,
      std::unique_ptr<std::function<void(verbsmarks::proto::QuorumResponse)>>
          response_handler) = 0;

  // Gives ReadyResponse to the corresponding follower.
  virtual void GetReady(
      int32_t follower_id,
      std::unique_ptr<std::function<void(verbsmarks::proto::ReadyResponse)>>
          response_handler) = 0;

  // Processes StartExperiment requests and send a response to everyone to start
  // experiment if all followers report ready. If anyone reports failure,
  // everyone will receive an error response.
  virtual void StartExperiment(
      verbsmarks::proto::StartExperimentRequest*,
      std::unique_ptr<
          std::function<void(verbsmarks::proto::StartExperimentResponse)>>
          response_handler) = 0;

  // Collects ResultRequests and send a response when everyone reported.
  virtual void FinishExperiment(
      verbsmarks::proto::ResultRequest*,
      std::unique_ptr<std::function<void(verbsmarks::proto::ResultResponse)>>
          response_handler) = 0;

  // Handles follower heartbeat and leader health response.
  virtual void HandleHeartbeat(
      verbsmarks::proto::HeartbeatRequest*,
      std::unique_ptr<std::function<void(verbsmarks::proto::HeartbeatResponse)>>
          response_handler) = 0;

  // Returns true if the experiment is finished.
  virtual std::pair<bool, absl::Status> IsFinished() = 0;
};

// Implements the logic of VerbsMarks leader.
class VerbsMarksLeaderImpl : public VerbsMarksLeaderInterface {
 public:
  VerbsMarksLeaderImpl(verbsmarks::proto::LeaderConfig config,
                       bool enable_heartbeats = false)
      : config_(config),
        quorum_formation_status_(QuorumFormationStatus::kForming),
        traffic_pattern_generated_(false),
        starting_experiment_failed_(false),
        finished_({false, absl::OkStatus()}),
        quorum_waiting_thread_(
            &VerbsMarksLeaderImpl::WaitForQuorum, this,
            config_.quorum_time_out_sec() > 0
                ? absl::Now() + absl::Seconds(config_.quorum_time_out_sec())
                : absl::InfiniteFuture()),
        enable_heartbeats_(enable_heartbeats),
        periodic_result_save_count_(0) {
    // Start generating traffic patterns from the beginning.
    GeneratePerFollowerTraffic();
    // If failed to generate traffic pattern, shutdown.
    bool traffic_pattern_ok;
    {
      absl::MutexLock lock(&traffic_pattern_mutex_);
      traffic_pattern_ok = traffic_pattern_status_.ok();
    }
    if (!traffic_pattern_ok) {
      LOG(INFO) << "Traffic pattern failed. Halt quorum forming.";
      absl::MutexLock lock(&quorum_mutex_);
      quorum_formation_status_ = QuorumFormationStatus::kHalted;
    }
  }

  ~VerbsMarksLeaderImpl() override {
    {
      absl::MutexLock lock(&quorum_mutex_);
      quorum_formation_status_ = QuorumFormationStatus::kHalted;
    }
    quorum_waiting_thread_.join();
  }

  void SeekQuorum(
      verbsmarks::proto::QuorumRequest*,
      std::unique_ptr<std::function<void(verbsmarks::proto::QuorumResponse)>>
          response_handler) override
      ABSL_LOCKS_EXCLUDED(quorum_mutex_, traffic_pattern_mutex_);

  void GetReady(
      int32_t follower_id,
      std::unique_ptr<std::function<void(verbsmarks::proto::ReadyResponse)>>
          response_handler) override
      ABSL_LOCKS_EXCLUDED(quorum_mutex_, traffic_pattern_mutex_);

  void StartExperiment(
      verbsmarks::proto::StartExperimentRequest*,
      std::unique_ptr<
          std::function<void(verbsmarks::proto::StartExperimentResponse)>>
          response_handler) override ABSL_LOCKS_EXCLUDED(experiment_mutex_);

  void FinishExperiment(
      verbsmarks::proto::ResultRequest*,
      std::unique_ptr<std::function<void(verbsmarks::proto::ResultResponse)>>
          response_handler) override ABSL_LOCKS_EXCLUDED(experiment_mutex_);

  void HandleHeartbeat(
      verbsmarks::proto::HeartbeatRequest*,
      std::unique_ptr<std::function<void(verbsmarks::proto::HeartbeatResponse)>>
          response_handler) override ABSL_LOCKS_EXCLUDED(experiment_mutex_);

  std::pair<bool, absl::Status> IsFinished() override
      ABSL_LOCKS_EXCLUDED(experiment_mutex_);

 private:
  // Writes periodic result with output from statistics_collector_ if periodic
  // output files are enabled. Skips if not enabled, or if time_check is set
  // and a full kPeriodicResultSaveInterval has not elapsed since last output.
  void SavePeriodicResult(bool time_check = true);

  const verbsmarks::proto::LeaderConfig config_;

  // For Quorum.
  absl::Mutex quorum_mutex_;
  // A queue keeps quorum requests received.
  std::list<QuorumArrival> quorum_queue_ ABSL_GUARDED_BY(quorum_mutex_);
  enum class QuorumFormationStatus { kForming, kFormed, kFailed, kHalted };
  QuorumFormationStatus quorum_formation_status_ ABSL_GUARDED_BY(quorum_mutex_);

  // Followers indexed by their numeric IDs, assigned by the leader.
  absl::flat_hash_map<int, verbsmarks::proto::Follower> followers_
      ABSL_GUARDED_BY(quorum_mutex_);

  // For traffic patterns.
  absl::Mutex traffic_pattern_mutex_ ABSL_ACQUIRED_AFTER(quorum_mutex_);
  // Ready responses indexed by the follower IDs.
  absl::flat_hash_map<int, verbsmarks::proto::ReadyResponse> ready_responses_
      ABSL_GUARDED_BY(traffic_pattern_mutex_);
  void GeneratePerFollowerTraffic() ABSL_LOCKS_EXCLUDED(traffic_pattern_mutex_);
  // Protects from sending incomplete ready_responses.
  bool traffic_pattern_generated_ ABSL_GUARDED_BY(traffic_pattern_mutex_);
  // If a failure happens while generating per follower traffic, first failure
  // is recorded here and stops. It will be sent to all followers through
  // ready_response.
  absl::Status traffic_pattern_status_ ABSL_GUARDED_BY(traffic_pattern_mutex_);

  // For starting and finishing experiment.
  absl::Mutex experiment_mutex_ ABSL_ACQUIRED_AFTER(quorum_mutex_);
  absl::flat_hash_map<int, std::unique_ptr<std::function<void(
                               verbsmarks::proto::StartExperimentResponse)>>>
      ready_followers_ ABSL_GUARDED_BY(experiment_mutex_);
  absl::flat_hash_set<int> error_followers_ ABSL_GUARDED_BY(experiment_mutex_);
  bool starting_experiment_failed_ ABSL_GUARDED_BY(experiment_mutex_);

  absl::flat_hash_map<
      int,
      std::unique_ptr<std::function<void(verbsmarks::proto::ResultResponse)>>>
      finished_followers_ ABSL_GUARDED_BY(experiment_mutex_);
  // The final report collection.
  proto::ResultReport final_report_;
  std::pair<bool, absl::Status> finished_ ABSL_GUARDED_BY(experiment_mutex_);

  std::thread quorum_waiting_thread_;
  // Waits until enough nodes join the quorum, the deadline passes, or traffic
  // pattern translation fails. Upon any of these events, sends responses to all
  // nodes that have attempted to join the quorum.
  void WaitForQuorum(absl::Time deadline)
      ABSL_LOCKS_EXCLUDED(quorum_mutex_, traffic_pattern_mutex_);

  // If set, leader will expect HeartbeatRequest messages from followers and
  // will use FollowerHealthTracker to abort when a timeout occurs without
  // receiving heartbeats from all followers. Otherwise, heartbeat messages will
  // be handled as no-ops at the leader.
  bool enable_heartbeats_;

  // Follower health tracking metadata, updated when heartbeats received from
  // followers or a leader timeout occurs.
  FollowerHealthTracker follower_health_;

  // Follower id and status of the follower failure which triggered experiment
  // abort, if any.
  std::optional<std::pair<int, google::rpc::Status>> first_follower_failure_;

  // Collects current stats received from followers throughout experiment
  // runtime, for real-time output and final result report.
  StatisticsCollector statistics_collector_;

  // Timestamp of last periodic result output, if enabled.
  absl::Time periodic_result_save_time_;

  // Sequence number for saved periodic result files.
  int periodic_result_save_count_;
};

}  // namespace verbsmarks

#endif  // VERBSMARKS_VERBSMARKS_LEADER_H_
