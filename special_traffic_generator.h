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

#ifndef VERBSMARKS_SPECIAL_TRAFFIC_GENERATOR_H_
#define VERBSMARKS_SPECIAL_TRAFFIC_GENERATOR_H_

#include <array>
#include <cstdint>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/synchronization/notification.h"
#include "completion_queue_manager.h"
#include "folly/stats/TDigest.h"
#include "infiniband/verbs.h"
#include "queue_pair.h"
#include "utils.h"
#include "verbsmarks.pb.h"

namespace verbsmarks {

int64_t AdjustTraffic(int op_size, int64_t iterations,
                      int64_t max_traffic_in_gb);

// SpecialTrafficGenerator implements traffic patterns that need special
// optimization.
class SpecialTrafficGenerator {
 public:
  explicit SpecialTrafficGenerator(
      proto::PerFollowerTrafficPattern traffic_pattern,
      proto::PerTrafficResult *summary)
      : traffic_pattern_(traffic_pattern),
        should_trace_(traffic_pattern.metrics_collection().enable_tracing()),
        warm_up_(
            traffic_pattern.metrics_collection().tracing_warmup_iterations()),
        iterations_(traffic_pattern.iterations()),
        op_size_(traffic_pattern.queue_pairs(0).max_op_size()),
        summary_(summary) {}
  virtual ~SpecialTrafficGenerator() = default;

  // Prepares for the special traffic generator. Called in the general traffic
  // generator's Prepare().
  virtual absl::Status Prepare() { return absl::OkStatus(); }
  // Generates the special traffic, called from general traffic generator's
  // Run().
  virtual absl::Status Run() { return absl::OkStatus(); }

  virtual void Update() {
    if (summary_ != nullptr) {
      summary_->set_global_traffic_id(
          traffic_pattern_.global_traffic_pattern_id());
      summary_->set_num_latency_obtained(latency_tdigest_.count());
      *summary_->mutable_latency() =
          utils::LatencyTdigestToProto(latency_tdigest_);
    }
  }

  // Ends an active Run() polling loop due to an external error.
  virtual void Abort();

 protected:
  proto::PerFollowerTrafficPattern traffic_pattern_;
  bool should_trace_;
  int warm_up_;
  int64_t iterations_;
  int32_t op_size_;
  folly::TDigest latency_tdigest_;
  proto::PerTrafficResult *summary_;
  absl::Notification abort_;
};

// Pingpong traffic.

// Initiator           Target
//    t1: post (send/write)
//                       t2: receive completion / memory poll
//                       -- post receive --
//                       t3: post (send/write)
//    t4: poll CQ (send/write)
//    t5: poll CQ for receive or mem_polled
//    -- post receive
//    repeat from t1.

// A structure that holds the timing and/or receive information for pingpong.
struct PingPongInfo {
  int num_recv;
  // Timestamps before and after calling `post`.
  int64_t post_before;
  int64_t post_after;
  // Timestamps before and after a successful `poll` of write or send op.
  int64_t completion_after;
  int64_t completion_before;
  // The time when memory was polled, if memory polling was used.
  int64_t mem_polled;
  // Timestamps before and after a successful `poll` of receive op, when
  // send/receive was used.
  int64_t recv_before;
  int64_t recv_after;
  // If the initator sent timestamp in the payload, this carries the value.
  int64_t sent_time_received;
};

// Generates a pingpong traffic.
class WritePingPongTraffic : public SpecialTrafficGenerator {
 public:
  explicit WritePingPongTraffic(
      proto::PerFollowerTrafficPattern traffic_pattern, QueuePair *queue_pair,
      CompletionQueueManager *completion_queue_manager,
      proto::PerTrafficResult *summary)
      : SpecialTrafficGenerator(traffic_pattern, summary),
        queue_pair_(queue_pair),
        completion_queue_manager_(completion_queue_manager) {
    for (int i = 0; i < kTracingLatencyBufferSize; ++i) {
      detail_time_[i] = {0, 0, 0, 0, 0, 0, 0, 0, 0};
    }
  }
  absl::Status Prepare() override;
  absl::Status Run() override;

 private:
  absl::Status RunInitiator();
  absl::Status RunTarget();
  uint64_t ping_address_remote_;
  uint64_t ping_address_local_;
  uint64_t pong_address_;
  // Send ping here.
  uint64_t *ping_ptr_;

  std::array<PingPongInfo, kTracingLatencyBufferSize> detail_time_;
  std::vector<double> latency_;
  // PingPong uses only one queue pair.
  QueuePair *queue_pair_;
  CompletionQueueManager *completion_queue_manager_;
};

// Generates a pingpong traffic using SEND / RECEIVE, either polling or
// event-driven (modes handled within CompletionQueueManager).
// We reuse one buffer for pingpong.
class SendPingPongTraffic : public SpecialTrafficGenerator {
 public:
  explicit SendPingPongTraffic(proto::PerFollowerTrafficPattern traffic_pattern,
                               QueuePair *queue_pair,
                               CompletionQueueManager *completion_queue_manager,
                               proto::PerTrafficResult *summary)
      : SpecialTrafficGenerator(traffic_pattern, summary),
        queue_pair_(queue_pair),
        completion_queue_manager_(completion_queue_manager),
        skip_latency_stats_(
            traffic_pattern.pingpong_traffic().skip_latency_stats()) {
    for (int i = 0; i < kTracingLatencyBufferSize; ++i) {
      detail_time_[i] = {0, 0, 0, 0, 0, 0, 0, 0, 0};
    }
  }
  absl::Status Prepare() override;
  absl::Status Run() override;
  void Update() override;

 private:
  absl::Status RunInitiator();
  absl::Status RunTarget();
  absl::Status Pong(int i, PingPongInfo &info, bool send_completed = false);

  uint64_t ping_address_;
  uint64_t pong_address_;

  // Easy data access.
  uint8_t *ping_ptr_;
  std::array<PingPongInfo, kTracingLatencyBufferSize> detail_time_;
  std::vector<double> latency_;
  int prepost_;
  // PingPong uses only one queue pair.
  QueuePair *queue_pair_;
  CompletionQueueManager *completion_queue_manager_;
  bool skip_latency_stats_;
};

// BandwidthTraffic is optimized to avoid computations during the experiment as
// much as possible. Small message size is suitable for measuring OP rate and
// larger message size is suitable for measuring bandwidth. It is essentially
// closed loop test with batching and signaling options where all the work
// requests are pre-created and chained before the experiment starts. Individual
// latencies are not tracked and simply takes the time taken to complete N
// iterations. Bidirectional and unidirectional traffic are supported.
class BandwidthTraffic : public SpecialTrafficGenerator {
 public:
  BandwidthTraffic(proto::PerFollowerTrafficPattern traffic_pattern,
                   QueuePair *queue_pair,
                   CompletionQueueManager *completion_queue_manager,
                   proto::PerTrafficResult *summary)
      : SpecialTrafficGenerator(traffic_pattern, summary),
        queue_pair_(queue_pair),
        completion_queue_manager_(completion_queue_manager) {}

  // Prepares work requests that will be reused throughout the experiment.
  absl::Status Prepare() override;

  // Runs the experiment. Reports ops per second.
  absl::Status Run() override;

  // Track the time sent and # of work requests in a batch.
  struct BatchTracker {
    ibv_send_wr *work_request_ptr;
    int cnt;
    uint64_t time_sent;
    struct BatchTracker *next;
  };

 private:
  // Send traffic needs the target to repost recv buffers when recv buffer
  // consumed. Use a separate function for efficiency.
  absl::Status RunSend();
  absl::Status Finish(uint64_t outstanding, int k, uint64_t begin,
                      uint64_t total_completed, uint64_t total_received,
                      uint64_t num_hit_outstanding);
  int batch_size_;
  int max_outstanding_;
  bool signal_only_one_in_batch_;
  std::vector<ibv_send_wr> bw_work_requests_;
  std::vector<ibv_sge> scatter_gather_entries_;
  std::vector<ibv_recv_wr> recv_work_requests_;
  std::vector<ibv_sge> recv_scatter_gather_entries_;
  // Store bandwidth_trackers.
  std::vector<struct BatchTracker> batch_trackers_;
  // Mapped by wr_id of the first work item for fast reverse lookup.
  absl::flat_hash_map<int, struct BatchTracker *> tracker_finder_;
  absl::flat_hash_map<int, struct ibv_recv_wr *> recv_wr_finder_;
  struct BatchTracker *next_batch_;
  std::vector<double> latency_;
  // Bandwidth traffic uses only one op code.
  ibv_wr_opcode ibv_opcode_;
  // Bandwidth traffic uses only one queue pair per traffic pattern.
  QueuePair *queue_pair_;
  CompletionQueueManager *completion_queue_manager_;
};

}  // namespace verbsmarks

#endif  // VERBSMARKS_SPECIAL_TRAFFIC_GENERATOR_H_
