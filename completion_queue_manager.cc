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

#include "completion_queue_manager.h"

#include <errno.h>

#include <cstdint>
#include <cstring>
#include <memory>

#include "absl/base/optimization.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/time/clock.h"
#include "ibverbs_utils.h"
#include "infiniband/verbs.h"
#include "utils.h"
#include "verbsmarks.pb.h"

namespace verbsmarks {

constexpr int kDefaultMaxCqe = 1024;
constexpr int64_t kPollingTimeoutNanos = 30 * utils::kSecondInNanoseconds;

absl::Status CompletionQueueManager::InitializeResources(
    ibv_context *verbs_context, int num_completion_queues, int queue_depth) {
  if (verbs_context == nullptr) {
    return absl::InvalidArgumentError("verbs_context must not be null");
  }

  if (queue_depth < 0) {
    return absl::InvalidArgumentError("queue_depth must be non-negative");
  }

  ibv_device_attr dev_attr;
  int max_cqe;
  if (ibv_query_device(verbs_context, &dev_attr) == 0) {
    max_cqe = dev_attr.max_cqe;
  } else {
    // Limit queue depth to 1024 if unspecified ibv_device_attr (matching the
    // behavior of QueuePair::InitializeResources).
    max_cqe = kDefaultMaxCqe;
  }
  if (queue_depth > max_cqe || queue_depth == 0) {
    LOG(INFO) << "Overriding requested completion queue depth " << queue_depth
              << " with hardware limit max_cqe " << max_cqe;
    queue_depth = max_cqe;
  }
  queue_depth_ = queue_depth;

  // Sizing ibv_wc buffer based on queue depth.
  completions_.resize(queue_depth_);

  if (num_completion_queues <= 0) {
    return absl::InvalidArgumentError("num_completion_queues must be positive");
  }

  if (!completion_queues_.empty() || !next_wr_ids_.empty()) {
    return absl::FailedPreconditionError(
        "InitializeResources must be called only once");
  }

  completion_queues_.reserve(num_completion_queues);
  next_wr_ids_.reserve(num_completion_queues);
  for (int i = 0; i < num_completion_queues; ++i) {
    completion_queues_.push_back(
        std::unique_ptr<ibv_cq, ibverbs_utils::CompletionQueueDeleter>(
            ibv_create_cq(verbs_context, queue_depth, /*cq_context=*/nullptr,
                          /*channel=*/channel_.get(), /*comp_vector=*/0)));
    if (completion_queues_[i] == nullptr) {
      // Destroy any CQs already created.
      completion_queues_.clear();
      next_wr_ids_.clear();
      return absl::InternalError(
          absl::StrCat("Failed to create completion queue ", i,
                       " with error: ", std::strerror(errno)));
    }

    if (channel_ != nullptr) {
      // Request event notification if channel is configured.
      if (ibv_req_notify_cq(completion_queues_[i].get(),
                            /*solicited_only=*/0) != 0) {
        return absl::InternalError(absl::StrCat(
            "ibv_req_notify_cq failed with error: ", std::strerror(errno)));
      }
    }

    next_wr_ids_.push_back(1);
    VLOG(2) << "Initialized resources for completion queue with index " << i;
  }

  roundrobin_poll_index_ = 0;

  return absl::OkStatus();
}

int CompletionQueueManager::GetCompletionQueueIndex(int32_t queue_pair_id) {
  if (ABSL_PREDICT_FALSE(completion_queues_.empty())) {
    return -1;
  }

  // For the default mapping policy, we compute the cq index upon each call,
  // since we use a simple calculation. More sophisticated policies may override
  // this function and store the assignments in a map structure, etc.
  return queue_pair_id % completion_queues_.size();
}

absl::StatusOr<ibv_cq *> CompletionQueueManager::GetCompletionQueue(
    int32_t queue_pair_id) {
  if (ABSL_PREDICT_FALSE(completion_queues_.empty())) {
    return absl::FailedPreconditionError("Completion queues not initialized");
  }
  return completion_queues_[GetCompletionQueueIndex(queue_pair_id)].get();
}

uint64_t CompletionQueueManager::GetNextWrIdForQueuePair(
    int32_t queue_pair_id) {
  return next_wr_ids_[GetCompletionQueueIndex(queue_pair_id)]++;
}

absl::StatusOr<CompletionQueueManager::CompletionInfo>
CompletionQueueManager::TryPollAtIndex(int queue_index) {
  CompletionQueueManager::CompletionInfo completion_info = {0, 0, 0,
                                                            completions_};

  completion_info.completion_before = absl::GetCurrentTimeNanos();

  // Assuming valid index, since TryPollAtIndex is called within a tight loop.
  const int num_polled = ibv_poll_cq(completion_queues_[queue_index].get(),
                                     queue_depth_, completions_.data());
  if (num_polled < 0) {
    return absl::InternalError(
        absl::StrCat("ibv_poll_cq failed with error: ", std::strerror(errno)));
  }

  completion_info.completion_after = absl::GetCurrentTimeNanos();
  completion_info.num_completions = num_polled;
  return completion_info;
}

absl::StatusOr<CompletionQueueManager::CompletionInfo>
CompletionQueueManager::PollAtIndex(int queue_index) {
  if (ABSL_PREDICT_FALSE(queue_index >= completion_queues_.size())) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Completion queue index ", queue_index, " is out of range"));
  }

  return TryPollAtIndex(queue_index);
}

absl::StatusOr<CompletionQueueManager::CompletionInfo>
CompletionQueueManager::Poll() {
  // One polling iteration for each CQ.
  for (int i = 0; i < completion_queues_.size(); i++) {
    // Poll at current roundrobin index then advance.
    auto completion_info = TryPollAtIndex(roundrobin_poll_index_);
    if (++roundrobin_poll_index_ >= completion_queues_.size()) {
      roundrobin_poll_index_ = 0;
    }
    // Finish if error or valid completions.
    if (!completion_info.ok() || (completion_info->num_completions > 0)) {
      return completion_info;
    }
  }
  return CompletionInfo{0, 0, 0, completions_};
}

absl::StatusOr<CompletionQueueManager::CompletionInfo>
CompletionQueueManager::PollUntilTimeout(int64_t timeout_nanos) {
  int64_t deadline = absl::GetCurrentTimeNanos() + timeout_nanos;

  while (true) {
    auto completion_info = TryPollAtIndex(roundrobin_poll_index_);
    if (++roundrobin_poll_index_ >= completion_queues_.size()) {
      roundrobin_poll_index_ = 0;
    }
    if (completion_info.ok() && completion_info->num_completions == 0) {
      if (completion_info->completion_after >= deadline) {
        return absl::DeadlineExceededError("Polling timed out");
      }
      // Keep polling until completion available or deadline exceeded.
      continue;
    }
    // Exit polling loop due to successful CompletionInfo or polling error.
    return completion_info;
  }
}

absl::StatusOr<CompletionQueueManager::CompletionInfo>
CompletionQueueManager::PollUntilTimeout() {
  return PollUntilTimeout(kPollingTimeoutNanos);
}

absl::Status EventCompletionQueueManager::InitializeResources(
    ibv_context *verbs_context, int num_completion_queues, int queue_depth) {
  if (verbs_context == nullptr) {
    return absl::InvalidArgumentError("verbs_context must not be null");
  }

  channel_ =
      std::unique_ptr<ibv_comp_channel, ibverbs_utils::CompChannelDeleter>(
          ibv_create_comp_channel(verbs_context),
          ibverbs_utils::CompChannelDeleter());
  if (channel_ == nullptr) {
    return absl::InternalError(absl::StrCat(
        "ibv_create_comp_channel failed with error", std::strerror(errno)));
  }

  return CompletionQueueManager::InitializeResources(
      verbs_context, num_completion_queues, queue_depth);
}

absl::StatusOr<CompletionQueueManager::CompletionInfo>
EventCompletionQueueManager::PollUntilTimeout() {
  struct ibv_cq *ev_cq;
  void *ev_ctx;
  CompletionQueueManager::CompletionInfo completion_info = {0, 0, 0,
                                                            completions_};

  completion_info.completion_before = absl::GetCurrentTimeNanos();

  if (ibv_get_cq_event(channel_.get(), &ev_cq, &ev_ctx) != 0) {
    return absl::InternalError(absl::StrCat(
        "ibv_get_cq_event failed with error: ", std::strerror(errno)));
  }
  if (ibv_req_notify_cq(ev_cq, /*solicited_only=*/0) != 0) {
    return absl::InternalError(absl::StrCat(
        "ibv_req_notify_cq failed with error: ", std::strerror(errno)));
  }
  ibv_ack_cq_events(ev_cq, 1);

  int num_polled = ibv_poll_cq(ev_cq, queue_depth_, completions_.data());
  if (ABSL_PREDICT_FALSE(num_polled < 0)) {
    return absl::InternalError(
        absl::StrCat("ibv_poll_cq failed with error: ", std::strerror(errno)));
  }

  completion_info.completion_after = absl::GetCurrentTimeNanos();
  completion_info.num_completions = num_polled;
  return completion_info;
}

}  // namespace verbsmarks
