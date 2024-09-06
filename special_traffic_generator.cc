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

#include "special_traffic_generator.h"

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>

#include "absl/base/optimization.h"
#include "absl/flags/flag.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/notification.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "completion_queue_manager.h"
#include "infiniband/verbs.h"
#include "queue_pair.h"
#include "utils.h"
#include "verbsmarks.pb.h"

ABSL_FLAG(int, bandwidth_timeout_sec, 3600,
          "If the bandwidth test doesn't finish in this time, fail the test. "
          "Increase this value when the # of iteration is large");

ABSL_FLAG(int, max_traffic_in_gb, 0, "If non zero, limits iterations.");

namespace verbsmarks {

absl::Status WritePingPongTraffic::Prepare() {
  ping_address_local_ = queue_pair_->GetLocallyControlledMemoryBaseAddr();
  pong_address_ = queue_pair_->GetRemoteControlledMemoryBaseAddr();
  ping_address_remote_ = queue_pair_->GetRemoteMemoryBaseAddr();
  ping_ptr_ = reinterpret_cast<uint64_t*>(ping_address_local_);
  // Set initial value.
  *ping_ptr_ = 0;
  *(reinterpret_cast<uint64_t*>(pong_address_)) = 0;
  latency_.resize(kTracingLatencyBufferSize);
  return absl::OkStatus();
}

absl::Status SendPingPongTraffic::Prepare() {
  prepost_ = traffic_pattern_.queue_pairs(0).max_outstanding_ops() +
             traffic_pattern_.queue_pairs(0).num_prepost_receive_ops();
  // For debugging purpose, initiator payload is wr_id + 1, target payload is
  // wr_id + 100.
  ping_address_ = queue_pair_->GetLocallyControlledMemoryBaseAddr();
  pong_address_ = queue_pair_->GetReceiveMemoryBaseAddr();
  ping_ptr_ = reinterpret_cast<uint8_t*>(ping_address_);
  // Prepare receive addresses and prepost receive ops.
  for (int i = 0; i < prepost_; ++i) {
    if (auto status =
            queue_pair_->SimplePostReceive(pong_address_, op_size_, i);
        !status.ok()) {
      return status.status();
    }
  }
  if (skip_latency_stats_) {
    // In this case we store only the latest singular latency sample.
    latency_.resize(1);
  } else {
    latency_.resize(kTracingLatencyBufferSize);
  }

  return absl::OkStatus();
}

absl::Status WritePingPongTraffic::Run() {
  bool is_initiator = traffic_pattern_.is_pingpong_initiator();
  absl::Status status = absl::OkStatus();
  if (is_initiator) {
    status = RunInitiator();
  } else {
    status = RunTarget();
  }
  Update();
  return status;
}

absl::Status SendPingPongTraffic::Run() {
  bool is_initiator = traffic_pattern_.is_pingpong_initiator();
  absl::Status status = absl::OkStatus();
  if (is_initiator) {
    status = RunInitiator();
  } else {
    status = RunTarget();
  }
  Update();
  return status;
}

absl::Status WritePingPongTraffic::RunInitiator() {
  absl::Status ret_status = absl::OkStatus();
  int i = 0;
  PingPongInfo time_info;
  int64_t total;
  // Wait until the target says they are ready.
  if (auto status_or_polltime = queue_pair_->PollMemory(pong_address_);
      !status_or_polltime.ok()) {
    LOG(ERROR) << "Initiator failed to receive the initial signal: "
               << status_or_polltime.status();
    return status_or_polltime.status();
  }

  VLOG(1) << "Initiator starts.";
  for (total = 0; total < iterations_ && !abort_.HasBeenNotified(); ++total) {
    // Ping payload starts from 1.
    *ping_ptr_ = (i + 1);
    if (auto status_or_times = queue_pair_->SimplePostWrite(
            ping_address_local_, op_size_, i, ping_address_remote_);
        !status_or_times.ok()) {
      ret_status = status_or_times.status();
      LOG(ERROR) << "Initiator failed to post pong: "
                 << status_or_times.status() << " after: " << total;
      break;
    } else {
      time_info.post_before = status_or_times.value().before;
      time_info.post_after = status_or_times.value().after;
    }
    // Clear completion of the ping write.
    auto status_or_completion = completion_queue_manager_->PollUntilTimeout();
    if (!status_or_completion.ok()) {
      ret_status = status_or_completion.status();
      LOG(ERROR) << "Initiator failed to clear pong: " << ret_status;
      break;
    } else {
      time_info.completion_after =
          status_or_completion.value().completion_after;
      time_info.completion_before =
          status_or_completion.value().completion_before;
    }
    if (auto status_or_polltime = queue_pair_->PollMemory(pong_address_);
        !status_or_polltime.ok()) {
      ret_status = status_or_polltime.status();
      LOG(ERROR) << "Initiator failed to receive pong: "
                 << status_or_polltime.status() << " after: " << total;
      break;
    } else {
      time_info.mem_polled = status_or_polltime.value();
    }
    detail_time_[i] = time_info;
    latency_[i] = (time_info.mem_polled - time_info.post_before) / 2.0;
    ++i;
    if (should_trace_ && warm_up_ == 0 && i == kTracingPauseAt) {
      printf("Start tracing at %d; 20 seconds before next op.\n", i);
      utils::BusyPolling(20 * utils::kSecondInNanoseconds);
    }
    if (i == kTracingLatencyBufferSize) {
      i = 0;
      latency_tdigest_ = latency_tdigest_.merge(latency_);
      if (should_trace_ && warm_up_ == 0) {
        // If tracing is requested, stop after some busy polling.
        utils::BusyPolling(1 * utils::kSecondInNanoseconds);
        printf("Stop tracing. Traffic will resume in 10 seconds.\n");
        utils::BusyPolling(10 * utils::kSecondInNanoseconds);
        i = 0;
        latency_tdigest_ = latency_tdigest_.merge(latency_);
        break;
      }
      --warm_up_;
    }
  }  // End of the iterations.
  if (i != 0) {
    latency_.resize(i);
    latency_tdigest_ = latency_tdigest_.merge(latency_);
  }
  if (!ret_status.ok()) {
    LOG(ERROR) << "Initiator failed at: " << total << " " << ret_status;
  } else {
    LOG(INFO) << "Initiator succeeded at: " << total;
  }
  // Print out latency tracing results.
  for (int i = 0; i < kTracingLatencyBufferSize; ++i) {
    auto info = detail_time_[i];
    VLOG(1) << i + 1 << "," << info.post_after - info.post_before << ","
            << info.completion_after - info.post_before << ","
            << (info.mem_polled - info.post_before) / 2.0;
  }
  return ret_status;
}

absl::Status WritePingPongTraffic::RunTarget() {
  absl::Status ret_status = absl::OkStatus();
  int i = 0;
  int64_t total;
  PingPongInfo time_info;
  // Before starting the traffic, tell the initiator that it is ready to start.
  *ping_ptr_ = 0xFF;
  if (auto status_or_times =
          queue_pair_->SimplePostWrite(ping_address_local_, op_size_,
                                       /* wr_id = */ 100, ping_address_remote_);
      !status_or_times.ok()) {
    LOG(ERROR) << "Failed to tell the initiator that we're ready: "
               << status_or_times.status();
    return status_or_times.status();
  }

  auto status_or_completion = completion_queue_manager_->PollUntilTimeout();
  if (!status_or_completion.ok()) {
    ret_status = status_or_completion.status();
    LOG(ERROR) << "Failed to tell the initiator that we're ready" << ret_status;
    return ret_status;
  }

  VLOG(1) << "Target starts.";
  // Receive pong by polling the memory.
  if (auto status_or_value = queue_pair_->PollMemory(pong_address_);
      !status_or_value.ok()) {
    ret_status = status_or_value.status();
    LOG(ERROR) << "Target failed to receive the initial pong: " << ret_status;
  } else {
    time_info.mem_polled = status_or_value.value();
  }
  for (total = 0; total < iterations_ && !abort_.HasBeenNotified(); ++total) {
    // Target payload: i + 100.
    *ping_ptr_ = i + 100;
    time_info.post_before = absl::GetCurrentTimeNanos();
    if (auto status_or_times = queue_pair_->SimplePostWrite(
            ping_address_local_, op_size_, i + 100, ping_address_remote_);
        !status_or_times.ok()) {
      ret_status = status_or_times.status();
      LOG(ERROR) << "Target failed to send the pong: " << ret_status;
      break;
    } else {
      time_info.post_before = status_or_times.value().before;
      time_info.post_after = status_or_times.value().after;
    }
    // Clear completion of the ping write.
    auto status_or_completion = completion_queue_manager_->PollUntilTimeout();
    if (!status_or_completion.ok()) {
      ret_status = status_or_completion.status();
      LOG(ERROR) << "Target failed to send the pong: " << ret_status;
      break;
    } else {
      time_info.completion_before =
          status_or_completion.value().completion_before;
      time_info.completion_after =
          status_or_completion.value().completion_after;
    }
    detail_time_[i] = time_info;
    latency_[i] = (time_info.post_after - time_info.mem_polled);
    ++i;
    if (should_trace_ && warm_up_ == 0 && i == kTracingPauseAt) {
      printf("Start tracing at %d; busy polling for 20 seconds\n", i);
      utils::BusyPolling(20 * utils::kSecondInNanoseconds);
    }
    if (i == kTracingLatencyBufferSize) {
      i = 0;
      latency_tdigest_ = latency_tdigest_.merge(latency_);
      if (should_trace_ && warm_up_ == 0) {
        // If tracing is requested, stop after some busy polling.
        utils::BusyPolling(utils::kSecondInNanoseconds);
        printf("Stop tracing. Cleaning up traffic will start in 10 seconds.\n");
        utils::BusyPolling(10 * utils::kSecondInNanoseconds);
        printf("Stopping at %ld th op\n", total);
        break;
      }
      --warm_up_;
    }
    if (total == iterations_ - 1) {
      LOG(INFO) << "Target finished: " << total;
      break;
    }
    // Poll the incoming message.
    if (auto status_or_value = queue_pair_->PollMemory(pong_address_);
        !status_or_value.ok()) {
      ret_status = status_or_value.status();
      LOG(ERROR) << "Target failed to receive the pong: " << ret_status
                 << " after: " << total;
      break;
    } else {
      time_info.mem_polled = status_or_value.value();
    }
  }  // End of the iterations.
  for (i = 0; i < kTracingLatencyBufferSize; ++i) {
    VLOG(1) << "target " << latency_[i];
  }
  if (i != 0) {
    latency_.resize(i);
    latency_tdigest_ = latency_tdigest_.merge(latency_);
  }
  if (!ret_status.ok()) {
    LOG(ERROR) << "Target failed at: " << total << " " << ret_status;
  }
  return ret_status;
}

absl::Status SendPingPongTraffic::Pong(int i, PingPongInfo& info,
                                       bool send_completed) {
  bool recv_completed = false;
  while (!recv_completed || !send_completed) {
    // Unless `complete_one` is true, we expect two completions, one for send
    // and one for receive. They may come together or separately.
    auto status_or_polltime = completion_queue_manager_->PollUntilTimeout();
    if (!status_or_polltime.ok()) {
      return status_or_polltime.status();
    }

    auto compl_info = status_or_polltime.value();
    for (int i = 0; i < compl_info.num_completions; ++i) {
      if (compl_info.completions[i].status != IBV_WC_SUCCESS) {
        LOG(ERROR) << "Failed completion with wr_id "
                   << compl_info.completions[i].wr_id << " and status "
                   << compl_info.completions[i].status;
        continue;
      }

      if (compl_info.completions[i].opcode == IBV_WC_RECV) {
        info.recv_before = compl_info.completion_before;
        info.recv_after = compl_info.completion_after;
        if (auto status =
                queue_pair_->SimplePostReceive(pong_address_, op_size_, i);
            !status.ok()) {
          return status.status();
        }
        recv_completed = true;
      } else if (compl_info.completions[i].opcode == IBV_WC_SEND) {
        info.completion_before = compl_info.completion_before;
        info.completion_after = compl_info.completion_after;
        send_completed = true;
      }
    }
  }
  return absl::OkStatus();
}

absl::Status SendPingPongTraffic::RunInitiator() {
  absl::Status ret_status = absl::OkStatus();
  int i = 0;
  PingPongInfo time_info;
  int64_t total = 0;
  // Before really starting the traffic, post an extra receive and wait for a
  // message from the target to ensure that the target is ready for pingpong
  // traffic.
  if (auto status = queue_pair_->SimplePostReceive(pong_address_, op_size_, i);
      !status.ok()) {
    return status.status();
  }
  auto status = completion_queue_manager_->PollUntilTimeout().status();
  if (!status.ok()) {
    LOG(ERROR) << status.message();
    return absl::InternalError(
        "Failed to get a signal that the target is ready.");
  }
  VLOG(1) << "Target is ready. Initiator starts now.";
  for (total = 0; total < iterations_ && !abort_.HasBeenNotified(); ++total) {
    VLOG(3) << "Iteration: " << total;
    *ping_ptr_ = i + 1;
    if (auto status_or_times =
            queue_pair_->SimplePostSend(ping_address_, op_size_, i + 1);
        !status_or_times.ok()) {
      ret_status = status_or_times.status();
      break;
    } else {
      time_info.post_before = status_or_times.value().before;
      time_info.post_after = status_or_times.value().after;
    }
    if (auto status = Pong(i, time_info); !status.ok()) {
      ret_status = status;
      break;
    }
    latency_[i] = (time_info.recv_after - time_info.post_before) / 2.0;
    if (!skip_latency_stats_) {
      detail_time_[i] = time_info;

      // Maintain most recent kTracingLatencyBufferSize samples and merge into
      // tdigest.
      ++i;
      if (should_trace_ && warm_up_ == 0 && i == kTracingPauseAt) {
        printf("Start tracing at %d; 20 seconds before next op.\n", i);
        utils::BusyPolling(20 * utils::kSecondInNanoseconds);
      }
      if (i == kTracingLatencyBufferSize) {
        i = 0;
        latency_tdigest_ = latency_tdigest_.merge(latency_);
        if (should_trace_ && warm_up_ == 0) {
          // If tracing is requested, stop after some busy polling.
          utils::BusyPolling(1 * utils::kSecondInNanoseconds);
          printf("Stop tracing. Traffic will resume in 10 seconds.\n");
          utils::BusyPolling(10 * utils::kSecondInNanoseconds);
          i = 0;
          latency_tdigest_ = latency_tdigest_.merge(latency_);
          break;
        }
        --warm_up_;
      }
    }
  }  // End of the iterations.

  if (!ret_status.ok()) {
    LOG_EVERY_N_SEC(ERROR, 60)
        << "Initiator failed at: " << total << " " << ret_status;
  }
  if (!skip_latency_stats_) {
    if (i != 0) {
      latency_.resize(i);
      latency_tdigest_ = latency_tdigest_.merge(latency_);
    }
    for (int i = 0; i < kTracingLatencyBufferSize; ++i) {
      auto info = detail_time_[i];
      VLOG(1) << i + 1 << "," << info.post_after - info.post_before << ","
              << info.completion_after - info.post_before << ","
              << (info.recv_after - info.post_before) / 2.0;
    }
  }
  return ret_status;
}

absl::Status SendPingPongTraffic::RunTarget() {
  absl::Status ret_status = absl::OkStatus();
  int i = 0;
  int64_t total = 0;
  PingPongInfo time_info;
  // Before we really starting the traffic, tell the initiator we're ready. This
  // is not counted towards latency measuring.
  if (auto status_or_time =
          queue_pair_->SimplePostSend(ping_address_, op_size_, 1);
      !status_or_time.ok()) {
    return absl::InternalError(
        "Failed to tell initiator that the target is ready");
  }
  VLOG(1) << "Initiator is notified. Target starts now.";
  if (auto status = Pong(i, time_info, true); !status.ok()) {
    return status;
  }
  for (total = 0; total < iterations_ && !abort_.HasBeenNotified(); ++total) {
    // Target's payload starts at 100 for debugging.
    *ping_ptr_ = i + 100;
    time_info.post_before = absl::GetCurrentTimeNanos();
    // Now send ping.
    if (auto status_or_times =
            queue_pair_->SimplePostSend(ping_address_, op_size_, i + 100);
        !status_or_times.ok()) {
      ret_status = status_or_times.status();
      break;
    } else {
      time_info.post_before = status_or_times.value().before;
      time_info.post_after = status_or_times.value().after;
    }
    latency_[i] = (time_info.post_after - time_info.recv_before);

    if (!skip_latency_stats_) {
      detail_time_[i] = time_info;

      // Maintain most recent kTracingLatencyBufferSize samples and merge into
      // tdigest.
      ++i;
      if (should_trace_ && warm_up_ == 0 && i == kTracingPauseAt) {
        printf("Start tracing at %d; busy polling for 20 seconds\n", i);
        utils::BusyPolling(20 * utils::kSecondInNanoseconds);
      }
      if (i == kTracingLatencyBufferSize) {
        i = 0;
        latency_tdigest_ = latency_tdigest_.merge(latency_);
        if (should_trace_ && warm_up_ == 0) {
          // If tracing is requested, stop after some busy polling.
          utils::BusyPolling(utils::kSecondInNanoseconds);
          printf("Stop tracing. Cleaning up traffic starts in 10 seconds.\n");
          utils::BusyPolling(10 * utils::kSecondInNanoseconds);
          printf("Stopping at %ld th op\n", total);
          break;
        }
        --warm_up_;
      }
    }

    // Last pong won't happen.
    if (total == iterations_ - 1) {
      break;
    }
    auto status = Pong(i, time_info);
    if (!status.ok()) {
      return status;
    }
  }  // End of the iterations.

  if (!skip_latency_stats_) {
    for (int i = 0; i < kTracingLatencyBufferSize; ++i) {
      VLOG(1) << "target " << i << ", "
              << detail_time_[i].post_after - detail_time_[i].post_before
              << ", " << latency_[i];
    }
    if (i != 0) {
      latency_.resize(i);
      latency_tdigest_ = latency_tdigest_.merge(latency_);
    }
  }
  if (!ret_status.ok()) {
    LOG_EVERY_N_SEC(ERROR, 60)
        << "Target failed at: " << total << " " << ret_status;
  }
  return ret_status;
}

void SendPingPongTraffic::Update() {
  if (summary_ == nullptr) return;

  summary_->set_global_traffic_id(traffic_pattern_.global_traffic_pattern_id());

  if (skip_latency_stats_) {
    summary_->set_num_latency_obtained(1);

    proto::LatencyResult result;
    // Populates only the last latency value in the buffer, i.e. does not
    // actually compute aggregate statistics.
    utils::DurationToProto(absl::Nanoseconds(latency_[0]),
                           *result.mutable_average_latency());
    utils::DurationToProto(absl::Nanoseconds(latency_[0]),
                           *result.mutable_median_latency());
    utils::DurationToProto(absl::Nanoseconds(latency_[0]),
                           *result.mutable_p99_latency());
    utils::DurationToProto(absl::Nanoseconds(latency_[0]),
                           *result.mutable_p999_latency());
    utils::DurationToProto(absl::Nanoseconds(latency_[0]),
                           *result.mutable_p9999_latency());
    utils::DurationToProto(absl::Nanoseconds(latency_[0]),
                           *result.mutable_min_latency());

    *summary_->mutable_latency() = result;
  } else {
    summary_->set_num_latency_obtained(latency_tdigest_.count());
    *summary_->mutable_latency() =
        utils::LatencyTdigestToProto(latency_tdigest_);
  }
}

int64_t AdjustTraffic(int op_size, int64_t iterations,
                      int64_t max_traffic_in_gb) {
  int64_t new_iterations = iterations;
  // If max_traffic_in_gb is unset, do not limit.
  if (max_traffic_in_gb > 0) {
    uint64_t total_in_byte = max_traffic_in_gb / 8 * 1e9;
    if (total_in_byte < op_size * iterations) {
      new_iterations = total_in_byte / op_size;
      LOG(INFO) << "Adjust iterations from " << iterations << " to "
                << new_iterations;
    } else {
      LOG(INFO) << "Do not adjust iterations because total_in_byte is "
                << total_in_byte << " is smaller than " << op_size * iterations;
    }
  }
  return new_iterations;
}

absl::Status BandwidthTraffic::Prepare() {
  if (queue_pair_ == nullptr) {
    return absl::FailedPreconditionError("Queue pair is null");
  }
  // Bandwidth traffic expects only one op_ratio, otherwise the traffic
  // generator does not create bandwidth traffic.
  auto rdma_op =
      traffic_pattern_.traffic_characteristics().op_ratio(0).op_code();
  switch (rdma_op) {
    case proto::RDMA_OP_READ:
      ibv_opcode_ = IBV_WR_RDMA_READ;
      break;
    case proto::RDMA_OP_WRITE:
      ibv_opcode_ = IBV_WR_RDMA_WRITE;
      break;
    case proto::RDMA_OP_SEND_RECEIVE:
      ibv_opcode_ = IBV_WR_SEND;
      break;
    default:
      return absl::InvalidArgumentError(
          absl::StrCat("Invalid op code: ", rdma_op));
  }
  if (!queue_pair_->IsInitiator() && ibv_opcode_ != IBV_WR_SEND) {
    // Read and write targets do not generate any traffic.
    return absl::OkStatus();
  }

  batch_size_ = traffic_pattern_.traffic_characteristics().batch_size();
  max_outstanding_ = traffic_pattern_.closed_loop_max_outstanding();
  signal_only_one_in_batch_ =
      traffic_pattern_.traffic_characteristics().signal_only_last_op_in_batch();
  iterations_ = AdjustTraffic(op_size_, iterations_,
                              absl::GetFlag(FLAGS_max_traffic_in_gb));

  int wr_id = 0;
  // Post recv buffers for target.
  // Buffer size equals to max_outstanding + num_prepost_receive_ops.
  if (queue_pair_->IsTarget() && ibv_opcode_ == IBV_WR_SEND) {
    // Prepare WRs and SGEs for RECEIVE ops on target.
    int max_recv_completion =
        max_outstanding_ +
        traffic_pattern_.traffic_characteristics().num_prepost_receive_ops();
    recv_scatter_gather_entries_.resize(max_recv_completion);
    recv_work_requests_.resize(max_recv_completion);

    LOG(INFO) << "Target posts recvs";
    uint64_t addr = queue_pair_->GetReceiveMemoryBaseAddr();
    //
    for (int i = 0; i < max_recv_completion; ++i) {
      recv_scatter_gather_entries_[i] = {
          .addr = addr,
          .length = static_cast<uint32_t>(op_size_),
          .lkey = queue_pair_->GetReceiveKey()};
      recv_work_requests_[i] = {
          .wr_id = static_cast<uint64_t>(++wr_id),
          .next = nullptr,
          .sg_list = &recv_scatter_gather_entries_[i],
          .num_sge = 1,
      };
      recv_wr_finder_[recv_work_requests_[i].wr_id] = &recv_work_requests_[i];
    }
    // Chain all recv WR and post all at once.
    for (int i = 0; i < recv_work_requests_.size() - 1; ++i) {
      recv_work_requests_[i].next = &recv_work_requests_[i + 1];
    }
    ibv_recv_wr* bad_recv_wr;
    if (int ret = queue_pair_->PostRecv(&recv_work_requests_[0], &bad_recv_wr);
        ret < 0) {
      return absl::FailedPreconditionError(
          absl::StrCat("ibv_post_recv failed.", ret, strerror(errno)));
    }
    LOG(INFO) << "wr 1 ~ " << wr_id << " are for recvs";
  }

  // Prepare work requests and scatters gather entries for initiator.
  if (queue_pair_->IsInitiator()) {
    scatter_gather_entries_.resize(max_outstanding_);
    bw_work_requests_.resize(max_outstanding_);
    batch_trackers_.resize((max_outstanding_ + batch_size_ - 1) / batch_size_);

    uint64_t initiator_op_addr =
        queue_pair_->GetLocallyControlledMemoryBaseAddr();
    for (int i = 0; i < bw_work_requests_.size(); ++i) {
      scatter_gather_entries_[i] = {.addr = initiator_op_addr,
                                    .length = static_cast<uint32_t>(op_size_),
                                    .lkey = queue_pair_->GetLocalKey()};
      unsigned int send_flags = IBV_SEND_SIGNALED;
      if (signal_only_one_in_batch_ && (i + 1) % batch_size_ != 0) {
        // Do not signal if `signal_only_one_in_batch_` is true and this is not
        // the last item.
        send_flags = 0;
      }
      // The last batch's last item might not be aligned with batch_size. We
      // still need to signal it.
      if (i + 1 == max_outstanding_) send_flags = IBV_SEND_SIGNALED;

      // Check if inline flag needs to be set.
      if (op_size_ <=
          traffic_pattern_.traffic_characteristics().inline_threshold()) {
        send_flags = send_flags | IBV_SEND_INLINE;
      }
      bw_work_requests_[i] = {
          .wr_id = static_cast<uint64_t>(++wr_id),
          .next = nullptr,
          .sg_list = &scatter_gather_entries_[i],
          .num_sge = 1,
          .opcode = ibv_opcode_,
          .send_flags = send_flags,
      };
      // Only read and write have remote addr.
      if (ibv_opcode_ != IBV_WR_SEND) {
        bw_work_requests_[i].wr.rdma.remote_addr =
            queue_pair_->GetRemoteMemoryBaseAddr();
        bw_work_requests_[i].wr.rdma.rkey = queue_pair_->GetRemoteKey();
      }
    }
    // Chain work requests and make a tracker for each batch.
    for (int i = 0; i < batch_trackers_.size(); ++i) {
      int base = i * batch_size_;
      ibv_send_wr* first_wr = &bw_work_requests_[base];
      ibv_send_wr* last_wr = first_wr;
      for (int j = base + 1; j < batch_size_ + base && j < max_outstanding_;
           ++j) {
        bw_work_requests_[j - 1].next = &bw_work_requests_[j];
        last_wr = &bw_work_requests_[j];
      }
      struct BatchTracker* cur_tracker = &batch_trackers_[i];
      cur_tracker->work_request_ptr = first_wr;
      cur_tracker->cnt = batch_size_;
      if ((i + 1) * batch_size_ > max_outstanding_) {
        cur_tracker->cnt = max_outstanding_ - (i * batch_size_);
      }
      tracker_finder_[last_wr->wr_id] = cur_tracker;
      if (i > 0) {
        batch_trackers_[i - 1].next = &batch_trackers_[i];
      }
    }
    next_batch_ = &batch_trackers_[0];
    batch_trackers_[batch_trackers_.size() - 1].next = next_batch_;
    for (int i = 0; i < batch_trackers_.size(); ++i) {
      VLOG(1) << "tracker " << i << ": " << batch_trackers_[i].cnt;
    }
    LOG(INFO) << "Created WQ for " << batch_size_;
  }
  return absl::OkStatus();
}

absl::Status BandwidthTraffic::Run() {
  if (ibv_opcode_ == IBV_WR_SEND) return RunSend();
  if (!queue_pair_->IsInitiator()) {
    VLOG(2) << "Only initiator should generate traffic";
    return absl::OkStatus();
  }
  int64_t total_completed = 0;
  int64_t tried = 0;
  int64_t outstanding = 0;

  // Get experiment start and end time.
  int64_t begin = absl::GetCurrentTimeNanos();
  int64_t experiment_time_in_ns =
      traffic_pattern_.traffic_characteristics().experiment_time().seconds() *
          utils::kSecondInNanoseconds +
      traffic_pattern_.traffic_characteristics().experiment_time().nanos();
  int64_t end_time = begin + experiment_time_in_ns;

  // Start traffic.
  int64_t num_hit_outstanding = 0;
  int latency_index = 0;
  latency_.resize(kTracingLatencyBufferSize);
  // Traffic ending criteria includes 1) experiment_time, and /or 2) number of
  // op iterations. `iterations_ == 0` means the traffic will be bounded by
  // experiment time.
  while (
      ABSL_PREDICT_TRUE((iterations_ == 0 || total_completed < iterations_) &&
                        !abort_.HasBeenNotified())) {
    // Post ops to have max_outstanding_ ops on fly.
    ++tried;
    ibv_send_wr* bad_wr;
    if (outstanding < max_outstanding_) {
      // If there's a room, we should post. `next_batch` should have the right
      // # of operations in the batch, assuming completions come in order.
      tried = 0;
      next_batch_->time_sent = absl::GetCurrentTimeNanos();
      int ret = queue_pair_->PostSend(next_batch_->work_request_ptr, &bad_wr);
      if (ABSL_PREDICT_FALSE(ret != 0)) {
        LOG(ERROR) << "ibv_post_send failed: " << ret << " " << strerror(ret);
        return absl::InternalError(
            absl::StrCat("ibv_post_send failed: ", ret, strerror(errno)));
      }
      outstanding += next_batch_->cnt;
      next_batch_ = next_batch_->next;
    } else {
      ++num_hit_outstanding;
    }
    if (ABSL_PREDICT_FALSE((tried >> 10) > 0) &&
        ABSL_PREDICT_FALSE(absl::GetCurrentTimeNanos() > end_time)) {
      LOG(INFO) << "Reached experiment end time when trying to send. Completed "
                << total_completed << " ops with outstanding count "
                << outstanding;
      break;
    }

    // Poll results from completion queue.
    auto completion_info_or_status = completion_queue_manager_->Poll();
    if (ABSL_PREDICT_FALSE(!completion_info_or_status.ok())) {
      return completion_info_or_status.status();
    }
    auto completion_info = completion_info_or_status.value();
    if (ABSL_PREDICT_TRUE(completion_info.num_completions == 0)) {
      continue;
    }
    if (signal_only_one_in_batch_) {
      // Unsignaled ops do not generate completions. `num_polled` should be
      // small in that case.
      for (int i = 0; i < completion_info.num_completions; ++i) {
        auto tracker_it =
            tracker_finder_.find(completion_info.completions[i].wr_id);
        if (ABSL_PREDICT_FALSE(tracker_it == tracker_finder_.end())) {
          LOG(ERROR) << "Didn't find batch tracker for completed wr_id: "
                     << completion_info.completions[i].wr_id;
          LOG(ERROR) << "Completion status for wr_id "
                     << completion_info.completions[i].wr_id << " is "
                     << ibv_wc_status_str(
                            completion_info.completions[i].status);
          continue;
        }
        const auto* tracker = tracker_it->second;
        if (ABSL_PREDICT_FALSE(tracker == nullptr)) {
          LOG(ERROR) << "Tracker is null for completed wr_id: "
                     << completion_info.completions[i].wr_id;
          continue;
        }
        outstanding -= tracker->cnt;
        total_completed += tracker->cnt;
        latency_[latency_index++] =
            completion_info.completion_after - tracker->time_sent;
        if (latency_index == kTracingLatencyBufferSize) {
          latency_tdigest_ = latency_tdigest_.merge(latency_);
          latency_index = 0;
        }
      }
    } else {
      outstanding -= completion_info.num_completions;
      total_completed += completion_info.num_completions;
      for (int i = 0; i < completion_info.num_completions; ++i) {
        auto tracker_it =
            tracker_finder_.find(completion_info.completions[i].wr_id);
        if (tracker_it == tracker_finder_.end()) {
          continue;
        }
        const auto* tracker = tracker_it->second;
        if (ABSL_PREDICT_FALSE(tracker == nullptr)) {
          LOG(ERROR) << "Tracker is null for completed wr_id: "
                     << completion_info.completions[i].wr_id;
          continue;
        }
        latency_[latency_index++] =
            completion_info.completion_after - tracker->time_sent;
        if (latency_index == kTracingLatencyBufferSize) {
          latency_tdigest_ = latency_tdigest_.merge(latency_);
          latency_index = 0;
        }
      }
    }
    // Check whether the traffic has reached the experiment end time.
    if (completion_info.completion_after > end_time) break;
  }
  LOG(INFO) << "Waiting for up to "
            << absl::ToInt64Seconds(utils::kHardCutoffAfterExperiment)
            << " seconds to drain outstanding ops.";

  // Wait extra time to make sure pending ops have time to come back.
  int64_t hard_cutoff_time =
      absl::GetCurrentTimeNanos() +
      absl::ToInt64Nanoseconds(utils::kHardCutoffAfterExperiment);
  while (outstanding > 0 && absl::GetCurrentTimeNanos() <= hard_cutoff_time) {
    // Poll results from completion queue.
    auto completion_info_or_status = completion_queue_manager_->Poll();
    if (ABSL_PREDICT_FALSE(!completion_info_or_status.ok())) {
      return completion_info_or_status.status();
    }

    auto completion_info = completion_info_or_status.value();
    if (ABSL_PREDICT_TRUE(completion_info.num_completions <= 0)) {
      continue;
    }
    // Successfully gets completions.
    if (signal_only_one_in_batch_) {
      // Unsignaled ops do not generate completions. `num_polled` should be
      // small in that case.
      for (int i = 0; i < completion_info.num_completions; ++i) {
        auto tracker_it =
            tracker_finder_.find(completion_info.completions[i].wr_id);
        if (ABSL_PREDICT_FALSE(tracker_it == tracker_finder_.end())) {
          LOG(ERROR) << "Didn't find batch tracker for completed wr_id: "
                     << completion_info.completions[i].wr_id;
          LOG(ERROR) << "Completion status for wr_id "
                     << completion_info.completions[i].wr_id << " is "
                     << ibv_wc_status_str(
                            completion_info.completions[i].status);
          continue;
        }
        const auto* tracker = tracker_it->second;
        if (ABSL_PREDICT_FALSE(tracker == nullptr)) {
          LOG(ERROR) << "Tracker is null for completed wr_id: "
                     << completion_info.completions[i].wr_id;
          continue;
        }
        outstanding -= tracker->cnt;
        total_completed += tracker->cnt;
      }
    } else {
      outstanding -= completion_info.num_completions;
      total_completed += completion_info.num_completions;
    }
  }
  return Finish(outstanding, latency_index, begin, total_completed,
                /*total_received=*/0, num_hit_outstanding);
}

absl::Status BandwidthTraffic::RunSend() {
  int64_t total_completed = 0;
  int64_t total_received = 0;
  int64_t tried = 0;
  int64_t outstanding = 0;

  // Get experiment start and end time.
  int64_t begin = absl::GetCurrentTimeNanos();
  int64_t experiment_time_in_ns =
      traffic_pattern_.traffic_characteristics().experiment_time().seconds() *
          utils::kSecondInNanoseconds +
      traffic_pattern_.traffic_characteristics().experiment_time().nanos();
  int64_t end_time = begin + experiment_time_in_ns;

  int64_t num_hit_outstanding = 0;
  ibv_recv_wr* bad_recv_wr;
  int latency_index = 0;
  bool is_initiator = queue_pair_->IsInitiator();
  bool is_target = queue_pair_->IsTarget();
  latency_.resize(kTracingLatencyBufferSize);
  // Start traffic.
  // Traffic ending criteria includes 1) experiment_time, and /or 2) number of
  // op iterations. `iterations_ == 0` means the traffic will be bounded by
  // experiment time.
  while (ABSL_PREDICT_TRUE(
      (iterations_ == 0 || ((is_initiator && total_completed < iterations_) ||
                            (is_target && total_received < iterations_))) &&
      !abort_.HasBeenNotified())) {
    ++tried;
    ibv_send_wr* bad_wr;
    if (is_initiator) {
      if (outstanding < max_outstanding_) {
        // If there's a room, we should post. `next_batch` should have the
        // right # of operations in the batch, assuming completions come in
        // order.
        tried = 0;
        next_batch_->time_sent = absl::GetCurrentTimeNanos();
        int ret = queue_pair_->PostSend(next_batch_->work_request_ptr, &bad_wr);
        if (ABSL_PREDICT_FALSE(ret != 0)) {
          LOG_EVERY_N_SEC(ERROR, 60)
              << "ibv_post_send failed: " << ret << " " << strerror(ret);
          return absl::InternalError(
              absl::StrCat("ibv_post_send failed: ", ret, strerror(errno)));
        }
        outstanding += next_batch_->cnt;
        next_batch_ = next_batch_->next;
      } else {
        ++num_hit_outstanding;
      }
    }
    if (ABSL_PREDICT_FALSE((tried >> 10) > 0) &&
        ABSL_PREDICT_FALSE(absl::GetCurrentTimeNanos() > end_time)) {
      LOG(INFO) << "Reached experiment end time when trying to send. Completed "
                << total_completed << " ops with outstanding count "
                << outstanding;
      break;
    }

    // Poll completions once (for both SEND or RECV ops).
    auto completion_info_or_status = completion_queue_manager_->Poll();
    if (ABSL_PREDICT_FALSE(!completion_info_or_status.ok())) {
      return completion_info_or_status.status();
    }

    auto completion_info = completion_info_or_status.value();
    if (ABSL_PREDICT_TRUE(completion_info.num_completions == 0)) {
      continue;
    }

    if (ABSL_PREDICT_TRUE(completion_info.num_completions > 0)) {
      int first_recv_wr = 0;
      int prev_recv_wr = 0;
      int recv_to_post = 0;
      for (int i = 0; i < completion_info.num_completions; ++i) {
        const auto& completion = completion_info.completions[i];
        auto wr_id = completion.wr_id;
        if (completion.status != 0) {
          return absl::InternalError(absl::StrCat(
              "ibv_poll_cq returned an error: ", completion.status,
              ibv_wc_status_str(completion.status), " for WR ", wr_id,
              " after ", total_completed, " ops, outstanding: ", outstanding));
        }
        if (completion.opcode == IBV_WC_RECV) {
          ++total_received;
          ++recv_to_post;
          auto recv_it = recv_wr_finder_.find(wr_id);
          if (ABSL_PREDICT_FALSE(recv_it == recv_wr_finder_.end())) {
            return absl::InternalError(absl::StrCat("Recv not found: ", wr_id));
          }
          recv_it->second->next = nullptr;
          if (prev_recv_wr != 0) {
            // If this is not the first, chain this to the prev.
            recv_wr_finder_[prev_recv_wr]->next = recv_it->second;
          } else {
            first_recv_wr = wr_id;
          }
          prev_recv_wr = wr_id;
          continue;
        }
        // Now process send completions.
        struct BatchTracker* tracker = nullptr;
        auto tracker_it = tracker_finder_.find(wr_id);
        if (signal_only_one_in_batch_) {
          if (ABSL_PREDICT_FALSE(tracker_it == tracker_finder_.end())) {
            LOG(ERROR) << "Didn't find batch tracker for completed wr_id: "
                       << completion_info.completions[i].wr_id;
            LOG(ERROR) << "Completion status for wr_id "
                       << completion_info.completions[i].wr_id << " is "
                       << ibv_wc_status_str(
                              completion_info.completions[i].status);
            continue;
          }
          tracker = tracker_it->second;
          outstanding -= tracker->cnt;
          total_completed += tracker->cnt;
        } else {
          --outstanding;
          ++total_completed;
          if (tracker_it == tracker_finder_.end()) {
            // Skip reporting latency if no corresponding tracker exists.
            continue;
          }
          tracker = tracker_finder_[wr_id];
        }
        latency_[latency_index++] =
            completion_info.completion_after - tracker->time_sent;
        if (latency_index == kTracingLatencyBufferSize) {
          latency_tdigest_ = latency_tdigest_.merge(latency_);
          latency_index = 0;
        }
      }
      // Post recv buffers if needed.
      if (recv_to_post != 0) {
        queue_pair_->PostRecv(recv_wr_finder_[first_recv_wr], &bad_recv_wr);
      }
      if (completion_info.completion_after > end_time) break;
    }
  }
  LOG(INFO) << "Waiting for up to "
            << absl::ToInt64Seconds(utils::kHardCutoffAfterExperiment)
            << " seconds to drain outstanding ops.";

  // Wait extra time to make sure pending ops have time to come back.
  int64_t hard_cutoff_time =
      absl::GetCurrentTimeNanos() +
      absl::ToInt64Nanoseconds(utils::kHardCutoffAfterExperiment);
  while (outstanding > 0 && absl::GetCurrentTimeNanos() <= hard_cutoff_time) {
    // Drain the CQ.
    auto completion_info_or_status = completion_queue_manager_->Poll();
    if (ABSL_PREDICT_FALSE(!completion_info_or_status.ok())) {
      return completion_info_or_status.status();
    }
    auto completion_info = completion_info_or_status.value();
    for (int i = 0; i < completion_info.num_completions; ++i) {
      auto wr_id = completion_info.completions[i].wr_id;
      if (completion_info.completions[i].opcode == IBV_WC_RECV) {
        continue;
      }
      if (signal_only_one_in_batch_) {
        if (tracker_finder_.find(wr_id) == tracker_finder_.end()) {
          LOG(ERROR) << "Received completion for wr_id: " << wr_id
                     << ", op: " << completion_info.completions[i].opcode
                     << " during drain, without a batch tracker.";
          LOG(ERROR) << "Completion status for wr_id " << wr_id << " is "
                     << ibv_wc_status_str(
                            completion_info.completions[i].status);
        } else {
          const auto* tracker = tracker_finder_[wr_id];
          outstanding -= tracker->cnt;
        }
      } else {
        --outstanding;
      }
    }
  }

  return Finish(outstanding, latency_index, begin, total_completed,
                total_received, num_hit_outstanding);
}

absl::Status BandwidthTraffic::Finish(uint64_t outstanding, int k,
                                      uint64_t begin, uint64_t total_completed,
                                      uint64_t total_received,
                                      uint64_t num_hit_outstanding) {
  int64_t end = absl::GetCurrentTimeNanos();
  while (outstanding > 0) {
    // Drain the CQ.
    auto completion_info_or_status = completion_queue_manager_->Poll();
    if (ABSL_PREDICT_FALSE(!completion_info_or_status.ok())) {
      return completion_info_or_status.status();
    }
    auto completion_info = completion_info_or_status.value();
    for (int i = 0; i < completion_info.num_completions; ++i) {
      auto wr_id = completion_info.completions[i].wr_id;
      if (completion_info.completions[i].opcode == IBV_WC_RECV) {
        continue;
      }
      if (signal_only_one_in_batch_) {
        if (tracker_finder_.find(wr_id) == tracker_finder_.end()) {
          LOG(WARNING) << "Received completion for wr_id: " << wr_id
                       << ", op: " << completion_info.completions[i].opcode
                       << " during drain, without a batch tracker.";
        } else {
          const auto* tracker = tracker_finder_[wr_id];
          outstanding -= tracker->cnt;
        }
      } else {
        --outstanding;
      }
    }
    if ((absl::GetCurrentTimeNanos() - end) >
        30 * utils::kSecondInNanoseconds) {
      LOG(ERROR) << "Failed to drain " << outstanding << " ops.";
      break;
    }
  }
  if (k != kTracingLatencyBufferSize) {
    latency_.resize(k);
    latency_tdigest_ = latency_tdigest_.merge(latency_);
  }
  int64_t time = end - begin;
  double total_sec = (time * 1.0) / utils::kSecondInNanoseconds;
  double total_ops = total_completed;
  double ops = (total_completed) / total_sec;
  summary_->mutable_throughput()->set_ops_per_second(ops);
  double bytes_per_second = ops * op_size_;
  summary_->mutable_throughput()->set_bytes_per_second(bytes_per_second);
  summary_->mutable_throughput()->set_gbps((bytes_per_second * 8.0) / 1e9);
  summary_->set_num_latency_obtained(latency_tdigest_.count());
  *summary_->mutable_latency() = utils::LatencyTdigestToProto(latency_tdigest_);
  LOG(INFO) << "Total " << total_ops << " completed in " << total_sec
            << " sec and received " << total_received << ". It is " << ops
            << " ops per sec, outstanding: " << outstanding;
  LOG(INFO) << "Latency (ns) mean: " << latency_tdigest_.mean()
            << " max: " << latency_tdigest_.max()
            << " min: " << latency_tdigest_.min()
            << " median: " << latency_tdigest_.estimateQuantile(0.5);
  LOG(INFO) << "Throughput (gbps): " << summary_->throughput().gbps();
  LOG(INFO) << "Number of times hitting the cap: " << num_hit_outstanding;
  // Fail the test if there were remaining outstanding.
  if (outstanding > 0) {
    return absl::DeadlineExceededError(
        absl::StrCat("Failed to drain the completion queue, with ", outstanding,
                     " remaining outstanding operations and ", total_completed,
                     " successful completions at end time"));
  }
  // Fail the test if total number of completion/receiving is less than intended
  // number of ops. Only check on initiator and when iteration is set.
  if (queue_pair_->IsTarget() || !iterations_) return absl::OkStatus();
  if (total_completed < iterations_) {
    return absl::DeadlineExceededError(absl::StrCat(
        "Failed to complete target of ", iterations_, " iterations in ",
        total_sec, " sec. Completed ", total_completed, " ops."));
  }
  return absl::OkStatus();
}

void SpecialTrafficGenerator::Abort() { abort_.Notify(); }

}  // namespace verbsmarks
