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

#ifndef VERBSMARKS_COMPLETION_QUEUE_MANAGER_H_
#define VERBSMARKS_COMPLETION_QUEUE_MANAGER_H_

#include <cstdint>
#include <memory>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "ibverbs_utils.h"
#include "infiniband/verbs.h"
#include "verbsmarks.pb.h"

namespace verbsmarks {

// Manages a set of completion queues (ibverbs ibv_cq), including
// initialization, polling, association to QueuePairs, and unique work request
// identifier (wr_id) generation.
class CompletionQueueManager {
 public:
  virtual absl::Status InitializeResources(ibv_context* verbs_context,
                                           int num_completion_queues,
                                           int queue_depth);

  // Translates a QP id to CQ index using the default modular policy.
  // Assumes a valid QP id and that InitializeResources has already
  // succeeded, since this is a datapath function.
  // Provide alternate QP-to-CQ assignment policies by overriding this function.
  virtual int GetCompletionQueueIndex(int32_t queue_pair_id);

  // Returns the ibv_cq associated with a QP id given the translation policy.
  // Intended for use in QP initialization (not polling or other CQ actions).
  virtual absl::StatusOr<ibv_cq*> GetCompletionQueue(int32_t queue_pair_id);

  // Returns the next available wr_id for a provided QP id. wr_id values for a
  // QP id are guaranteed to be unique, and are also unique within the QP's
  // assigned CQ.
  virtual uint64_t GetNextWrIdForQueuePair(int32_t queue_pair_id);

  //
  // A structure that holds the number of completed operations, split into the
  // number that were initiated by this queue pair and the number that were
  // received by this queue pair. It also carries timestamps when applicable.
  struct CompletionInfo {
    int num_completions;
    int64_t completion_after;
    int64_t completion_before;
    std::vector<ibv_wc>& completions;
  };

  // Polls all completion queues once using a round-robin policy. Checks each CQ
  // sequentially from the last polled CQ, returning once available completions
  // are found or all CQs have been polled once.
  virtual absl::StatusOr<CompletionQueueManager::CompletionInfo> Poll();

  // Polls all completion queues using a round-robin policy. Checks each CQ
  // sequentially from the last polled CQ, returning once available completions
  // are found or a timeout occurs.
  virtual absl::StatusOr<CompletionQueueManager::CompletionInfo>
  PollUntilTimeout(int64_t timeout_nanos);

  // Same as above, polling until the default 30s timeout is reached.
  virtual absl::StatusOr<CompletionQueueManager::CompletionInfo>
  PollUntilTimeout();

  // Polls the completion queue at the specified index once.
  virtual absl::StatusOr<CompletionQueueManager::CompletionInfo> PollAtIndex(
      int queue_index);

  virtual ~CompletionQueueManager() = default;

 protected:
  std::unique_ptr<ibv_comp_channel, ibverbs_utils::CompChannelDeleter> channel_;
  std::vector<ibv_wc> completions_;
  int queue_depth_;

 private:
  std::vector<std::unique_ptr<ibv_cq, ibverbs_utils::CompletionQueueDeleter>>
      completion_queues_;
  absl::flat_hash_map<int32_t, int> queue_pair_id_to_cq_index_;
  std::vector<uint64_t> next_wr_ids_;
  int roundrobin_poll_index_;

  // Polls once on the provided queue index, helper for public Poll*() methods.
  absl::StatusOr<CompletionQueueManager::CompletionInfo> TryPollAtIndex(
      int queue_index);
};

// Event-driven variant of the completion queue manager. This subclass initiates
// an ibverbs event channel and instead of busy-polling, awaits an event
// notification before retrieving completions from a CQ. This provides an
// alternative to PollUntilTimeout which does not spend CPU cycles spinning.
class EventCompletionQueueManager : public CompletionQueueManager {
 public:
  // Initializes the completion queue manager and creates an ibverbs event
  // channel for event-driven completion notifications.
  absl::Status InitializeResources(ibv_context* verbs_context,
                                   int num_completion_queues,
                                   int queue_depth) override;

  // EventCompletionManager only supports one polling mechanism, which awaits
  // an event before retrieving completions from the corresponding CQ. This
  // matches the behavior of the base PollUntilTimeout(), except avoids
  // spinning in favor of event notification. Other methods such as Poll() and
  // PollAtIndex() fall back to default behavior.
  absl::StatusOr<CompletionQueueManager::CompletionInfo> PollUntilTimeout()
      override;
};

}  // namespace verbsmarks

#endif  // VERBSMARKS_COMPLETION_QUEUE_MANAGER_H_
