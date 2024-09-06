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

#include "queue_pair.h"

#include <errno.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/flags/flag.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "completion_queue_manager.h"
#include "connection_coordinator.pb.h"
#include "farmhash.h"
#include "folly/Utility.h"
#include "folly/stats/TDigest.h"
#include "ibverbs_utils.h"
#include "infiniband/verbs.h"
#include "throughput_computer.h"
#include "utils.h"
#include "verbsmarks.pb.h"
#include "verbsmarks_binary_flags.h"

namespace verbsmarks {

namespace {
// Access for all operation types.
constexpr int kRemoteAccessAll = IBV_ACCESS_REMOTE_WRITE |
                                 IBV_ACCESS_REMOTE_READ |
                                 IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_MW_BIND;
constexpr int kArbitraryPacketSequenceNumber = 1225;
constexpr int kGidLength = 16;
// In RDMA, any UD queue pair can communicate with any other in the subnet, so
// the Q_Key is a protection mechanism that allows UD queue pairs to specify
// which senders it will receive from.
// More information: https://www.rdmamojo.com/2013/11/29/queue-key-q_key/
// For our tests, we will use a constant Q_Key so that all UD queue pairs have
// access to one another.
constexpr int32_t kQKey = 200;

// The deterministic value that should be at index i of an operation buffer
// when validating buffer data. The buffer will repeat the wr_id of the
// operation.
uint8_t GetBufferVal(const int32_t i, const uint64_t wr_id) {
  return wr_id << (i % 8);
}

static std::string code_to_op(ibv_wc_opcode code) {
  switch (code) {
    case IBV_WC_SEND:
      return "IBV_WC_SEND";
    case IBV_WC_RDMA_WRITE:
      return "IBV_WC_RDMA_WRITE";
    case IBV_WC_RDMA_READ:
      return "IBV_WC_RDMA_READ";
    case IBV_WC_COMP_SWAP:
      return "IBV_WC_COMP_SWAP";
    case IBV_WC_FETCH_ADD:
      return "IBV_WC_FETCH_ADD";
    case IBV_WC_BIND_MW:
      return "IBV_WC_BIND_MW";
    case IBV_WC_LOCAL_INV:
      return "IBV_WC_LOCAL_INV";
    case IBV_WC_TSO:
      return "IBV_WC_TSO";
    case IBV_WC_RECV:
      return "IBV_WC_RECV";
    case IBV_WC_RECV_RDMA_WITH_IMM:
      return "IBV_WC_RECV_RDMA_WITH_IMM";
    case IBV_WC_TM_ADD:
      return "IBV_WC_TM_ADD";
    case IBV_WC_TM_DEL:
      return "IBV_WC_TM_DEL";
    case IBV_WC_TM_SYNC:
      return "IBV_WC_TM_SYNC";
    case IBV_WC_TM_RECV:
      return "IBV_WC_TM_RECV";
    case IBV_WC_TM_NO_TAG:
      return "IBV_WC_TM_NO_TAG";
    case IBV_WC_DRIVER1:
      return "IBV_WC_DRIVER1";
  }
}

absl::StatusOr<ibv_qp_type> ConnectionTypeToQpType(
    const proto::ConnectionType connection_type) {
  switch (connection_type) {
    case proto::CONNECTION_TYPE_RC:
      return IBV_QPT_RC;
    case proto::CONNECTION_TYPE_UD:
      return IBV_QPT_UD;
    case proto::CONNECTION_TYPE_UNKNOWN:
      return absl::InvalidArgumentError(
          "CONNECTION_TYPE_UNKNOWN cannot be converted to a queue pair type.");
    default:
      return absl::UnimplementedError(
          absl::StrCat("Connection type ", connection_type,
                       " cannot be converted to a queue pair type."));
  }
}
}  // namespace

// Populate memory block if needed.
void PopulateMemoryBlock(
    std::optional<MemoryBlockMetadata> memory_block_metadata, char c) {
  if (!memory_block_metadata.has_value()) {
    return;
  }
  char *buffer = reinterpret_cast<char *>(memory_block_metadata->address);
  //
  for (int i = 0; i < memory_block_metadata->size; ++i) {
    buffer[i] = c;
  }
}

void ValidateMemoryBlock(
    std::optional<MemoryBlockMetadata> memory_block_metadata,
    absl::string_view name) {
  if (!memory_block_metadata.has_value()) {
    return;
  }
  LOG(INFO) << "ValidateMemoryBlock: " << name;
  char *buffer = reinterpret_cast<char *>(memory_block_metadata->address);
  LOG(INFO) << "Hash of : " << name << " "
            << farmhash::Fingerprint64(buffer, memory_block_metadata->size);
  // Print out values for manual inspection when the block is small.
  int kTooBigForManualInspection = 100;
  if (memory_block_metadata->size < kTooBigForManualInspection) {
    for (int i = 0; i < memory_block_metadata->size; ++i) {
      LOG(INFO) << "buf: " << i << " value: " << int(buffer[i]);
    }
  }
}

QueuePair::QueuePair(ibv_context *context,
                     const ibverbs_utils::LocalIbverbsAddress local_address,
                     const proto::QueuePairConfig config, bool is_pingpong,
                     CompletionQueueManager *completion_queue_manager)
    : is_pingpong_(is_pingpong),
      state_(State::kUninitialized),
      collect_measurements_(false),
      config_(config),
      context_(context),
      local_address_(local_address),
      next_wr_id_(1),
      should_send_time_stamp_(false),
      should_trace_(false),
      num_completed_operations_(0),
      num_completions_received_(0),
      completion_queue_manager_(completion_queue_manager) {
  // The context is nullptr in some unit tests, e.g., in traffic generator
  // test. We assign 1K to WQE cap for testing purpose only.
  ibv_device_attr dev_attr;
  if (context_ && ibv_query_device(context, &dev_attr) == 0) {
    wqe_cap_ = dev_attr.max_qp_wr;
    max_cqe_ = dev_attr.max_cqe;
  } else {
    wqe_cap_ = 1024;
    max_cqe_ = 1024;
  }
  // Update the max_outstanding_ops and memory_region_size based on the WQE
  // cap.
  if (config_.max_outstanding_ops() > wqe_cap_) {
    LOG(INFO) << "The requested max outstanding is: "
              << config_.max_outstanding_ops()
              << ", which is larger than the WQE cap: " << wqe_cap_
              << ". Use WQE cap instead as the max outstanding.";
    config_.set_max_outstanding_ops(wqe_cap_);
  }
  if (config_.max_outstanding_ops() == 0) {
    config_.set_max_outstanding_ops(std::min(wqe_cap_, max_cqe_ / 2));
    VLOG(2) << "The requested max outstanding is 0. Setting to HW max: "
            << config_.max_outstanding_ops() << ".";
  }

  // The maximum batch size is the maximum number of outstanding operations.
  // Provision double of max outstanding due to receive.
  scatter_gather_entries_ =
      std::vector<ibv_sge>(config_.max_outstanding_ops() * 2);
  work_requests_ = std::vector<ibv_send_wr>(config_.max_outstanding_ops() * 2);

  outstanding_initiated_operations_.reserve(config_.max_outstanding_ops());
  next_wr_pos_ = 0;
  next_wrs_to_process_.reserve(config_.max_outstanding_ops() * 2);

  for (int i = 0; i < kTracingLatencyBufferSize; ++i) {
    before_poll_[i] = 0;
    after_poll_[i] = 0;
    before_post_[i] = 0;
    after_post_[i] = 0;
    timestamp_received_[i] = 0;
  }
}

void QueuePair::StartCollectingMeasurements() {
  for (auto &it : statistics_trackers_) {
    it.second.Reset();
  }
  collect_measurements_ = true;
}

void QueuePair::ResumeCollectingMeasurements() { collect_measurements_ = true; }

void QueuePair::StopCollectingMeasurements() {
  collect_measurements_ = false;
  int64_t end_time = absl::GetCurrentTimeNanos();
  for (auto &it : statistics_trackers_) {
    // Flush any buffered latency samples and end throughput calculation. Must
    // be done before AccessStatistics (which is const).
    it.second.Finish(end_time, outstanding_initiated_operations_.size());
  }
  ValidateOpBuffers();
  if (!should_trace_) return;

  int items = kTracingLatencyBufferSize;
  if (local_controlled_address_space_.has_value()) {
    if (should_trace_) {
      // Print out detailed latency only up to the stop point if tracing is
      // requested.
      items = kTracingStopAt;
    }
    std::string output =
        "i, before post, after post, before poll, after poll, post "
        "took, poll took, after post before poll, e2e latency\n";
    for (int i = 0; i < kTracingStopAt; ++i) {
      absl::StrAppend(&output, i + 1, ",", before_post_[i], ",", after_post_[i],
                      ",", before_poll_[i], ",", after_poll_[i], ",",
                      after_post_[i] - before_post_[i], ",",
                      after_poll_[i] - before_poll_[i], ",",
                      before_poll_[i] - after_post_[i], ",",
                      after_poll_[i] - before_post_[i], "\n");
    }
    LOG(INFO) << "initiator: \n" << output;
  } else {
    std::string output =
        "i, before poll, after poll, received ts, duration, delta\n";
    for (int i = 0; i < kTracingStopAt; ++i) {
      absl::StrAppend(&output, i + 1, ",", before_poll_[i], ",", after_poll_[i],
                      ",", timestamp_received_[i], ",",
                      after_poll_[i] - before_poll_[i], ",",
                      after_poll_[i] - timestamp_received_[i], "\n");
    }
    absl::SleepFor(absl::Seconds(1));
    LOG(INFO) << "target: \n" << output;
  }
}

proto::CurrentStats::QueuePairCurrentStats QueuePair::GetCurrentStatistics() {
  proto::CurrentStats::QueuePairCurrentStats qp_stats;
  qp_stats.set_queue_pair_id(config_.queue_pair_id());

  if (!config_.metrics_collection().enable_per_second_stats() ||
      !config_.is_initiator()) {
    // Real-time stats only available when enabled and when QP is op initiator.
    return qp_stats;
  }

  for (auto &it : statistics_trackers_) {
    // Append current stats from each tracker, if stats are available.
    proto::CurrentStats::PerOpTypeSizeCurrentStats *appended_stats =
        it.second.PopCurrentStatistics(&qp_stats);
    if (appended_stats != nullptr) {
      // Fill in op type/size values given map key <type,size>.
      appended_stats->set_op_type(it.first.first);
      appended_stats->set_op_size(it.first.second);
    }
  }
  return qp_stats;
}

void QueuePair::ValidateOpBuffers() {
  if (!config_.validate_op_buffers()) {
    VLOG(2) << "Not configured for buffer validation";
    return;
  }
  LOG(INFO) << "Validate op buffers";
  ValidateMemoryBlock(remote_controlled_memory_block_metadata_, "remote");
  ValidateMemoryBlock(recv_memory_block_metadata_, "recv");
  ValidateMemoryBlock(local_controlled_memory_block_metadata_, "local");
}

void QueuePair::ProcessFailedCompletion(const ibv_wc &completion) {
  if (auto it = outstanding_recv_operations_.find(completion.wr_id);
      it != outstanding_recv_operations_.end()) {
    outstanding_recv_operations_.erase(it);
    return;
  }
  if (auto it = outstanding_initiated_operations_.find(completion.wr_id);
      it != outstanding_initiated_operations_.end()) {
    outstanding_initiated_operations_.erase(it);
    return;
  }
  LOG(ERROR) << "Failed to clear " << completion.wr_id;
}

absl::StatusOr<uint64_t> QueuePair::ProcessReceiveCompletion(
    const ibv_wc &completion) {
  // Make sure that the completion corresponds to an op issued.
  auto it = outstanding_recv_operations_.find(completion.wr_id);
  if (it == outstanding_recv_operations_.end()) {
    failed_completion_messages_.push_back(
        absl::StrCat("Receive completion with wr_id ", completion.wr_id,
                     " does not correspond to an outstanding operation."));
    return absl::NotFoundError(failed_completion_messages_.back());
  }
  uint64_t addr = it->second;
  outstanding_recv_operations_.erase(it);
  return addr;
}

uint64_t QueuePair::GetNextWrId() {
  if (completion_queue_manager_ != nullptr) {
    return completion_queue_manager_->GetNextWrIdForQueuePair(
        config_.queue_pair_id());
  }
  return next_wr_id_++;
}

absl::Status QueuePair::InitializeResources(
    QueuePairMemoryResourcesView memory_resources_view,
    const uint32_t inline_threshold) {
  if (config_.max_op_size() <= 0) {
    return absl::FailedPreconditionError(
        "QueuePairConfig must have valid max_op_size.");
  }
  uint32_t max_outstanding_ops = config_.max_outstanding_ops();
  if (is_pingpong_ && max_outstanding_ops < kTracingLatencyBufferSize) {
    max_outstanding_ops = kTracingLatencyBufferSize;
  }
  if (config_.override_queue_depth() > 0) {
    max_outstanding_ops = config_.override_queue_depth();
  }
  VLOG(2) << "max_outstanding final: " << max_outstanding_ops;

  // If prepost is required, we need more.
  uint32_t max_recv_ops =
      max_outstanding_ops + config_.num_prepost_receive_ops();
  {
    if (state_ != State::kUninitialized) {
      return absl::AlreadyExistsError(
          "QueuePair can only initialize resources once.");
    }

    bool is_recv_enabled = false;
    protection_domain_ = memory_resources_view.protection_domain;

    if (memory_resources_view.recv_memory_regions.has_value()) {
      recv_lkey_space_.Initialize(
          memory_resources_view.recv_memory_regions.value());
    }
    if (memory_resources_view.local_controlled_memory_regions.has_value()) {
      local_controlled_lkey_space_.Initialize(
          memory_resources_view.local_controlled_memory_regions.value());
    }
    if (memory_resources_view.remote_controlled_memory_regions.has_value()) {
      for (auto memory_region :
           memory_resources_view.remote_controlled_memory_regions.value()) {
        remote_controlled_memory_region_rkeys_.push_back(memory_region->rkey);
      }
    }

    if (memory_resources_view.recv_memory_block_metadata.has_value()) {
      recv_memory_block_metadata_.emplace(
          memory_resources_view.recv_memory_block_metadata.value());
      recv_memory_address_space_.emplace(recv_memory_block_metadata_->address,
                                         recv_memory_block_metadata_->size);
      is_recv_enabled = true;
    }
    if (memory_resources_view.local_controlled_memory_block_metadata
            .has_value()) {
      local_controlled_memory_block_metadata_.emplace(
          memory_resources_view.local_controlled_memory_block_metadata.value());
      local_controlled_address_space_.emplace(
          local_controlled_memory_block_metadata_->address,
          local_controlled_memory_block_metadata_->size);
    }
    if (memory_resources_view.remote_controlled_memory_block_metadata
            .has_value()) {
      remote_controlled_memory_block_metadata_.emplace(
          memory_resources_view.remote_controlled_memory_block_metadata
              .value());
    }

    absl::StatusOr<ibv_qp_type> status_or_qp_type =
        ConnectionTypeToQpType(config_.connection_type());
    ibv_qp_init_attr attr = {
        .srq = nullptr,
        .cap = {.max_send_wr = max_outstanding_ops,
                .max_recv_wr = max_recv_ops,
                // One scatter-gather element per work request.
                .max_send_sge = 1,
                .max_recv_sge = 1},
        .qp_type = status_or_qp_type.value(),
        .sq_sig_all = 0};

    // Get an ibv_cq pointer from CompletionQueueManager.
    if (completion_queue_manager_ != nullptr) {
      auto cq_or_error = completion_queue_manager_->GetCompletionQueue(
          config_.queue_pair_id());
      if (!cq_or_error.ok()) {
        return cq_or_error.status();
      }

      attr.send_cq = cq_or_error.value();
      attr.recv_cq = cq_or_error.value();
    } else {
      return absl::InternalError("CompletionQueueManager is null.");
    }

    attr.cap.max_inline_data = inline_threshold;
    inline_threshold_ = inline_threshold;
    VLOG(2) << "ibv_create_qp";
    queue_pair_ = std::unique_ptr<ibv_qp, ibverbs_utils::QueuePairDeleter>(
        ibv_create_qp(protection_domain_, &attr),
        ibverbs_utils::QueuePairDeleter());

    if (queue_pair_ == nullptr) {
      return absl::InternalError(
          absl::StrCat("Failed to create queue pair with ibv_create_qp: ",
                       std::strerror(errno)));
    }

    state_ = State::kInitialized;
  }

  if (recv_memory_address_space_.has_value() && !is_pingpong_) {
    int to_post =
        config_.num_prepost_receive_ops() + config_.max_outstanding_ops();
    VLOG(2) << "Posting " << to_post << " receive ops before start.";
    // Fill the receive block with receive buffers.
    for (int i = 0; i < to_post; ++i) {
      if (absl::StatusOr<int32_t> status_or_bytes_posted =
              PostRecv(recv_memory_address_space_->GetMemoryAddress());
          !status_or_bytes_posted.ok()) {
        return absl::InternalError(
            absl::StrCat("QueuePair could not initialize resources because "
                         "PostRecv failed: ",
                         status_or_bytes_posted.status().message()));
      }
    }
  }
  if (config_.validate_op_buffers()) {
    PopulateMemoryBlock(remote_controlled_memory_block_metadata_, 'a');
    PopulateMemoryBlock(local_controlled_memory_block_metadata_, 'b');
    PopulateMemoryBlock(recv_memory_block_metadata_, 'c');
  }
  ValidateOpBuffers();

  VLOG(2) << "Initialized resources for queue pair at port "
          << local_address_.port;
  return absl::OkStatus();
}

absl::StatusOr<QueuePair::PingTime> QueuePair::SimplePostSend(uint64_t addr,
                                                              uint32_t length,
                                                              uint32_t wr_id) {
  // Simple post always send inline + signaled.
  unsigned int send_flags = IBV_SEND_SIGNALED;
  if (length <= inline_threshold_) {
    send_flags |= IBV_SEND_INLINE;
  }
  ibv_sge scatter_gather_entry = {
      .addr = addr,
      .length = length,
      .lkey = local_controlled_lkey_space_.GetMemoryKey()};
  ibv_send_wr work_request = {
      .wr_id = wr_id,
      .next = nullptr,
      .sg_list = &scatter_gather_entry,
      .num_sge = 1,
      .opcode = IBV_WR_SEND,
      .send_flags = send_flags,
  };
  ibv_send_wr *bad_wr;
  int64_t before = absl::GetCurrentTimeNanos();
  const int ret = ibv_post_send(queue_pair_.get(), &work_request, &bad_wr);
  if (ret != 0) {
    return absl::InternalError(
        absl::StrCat("ibv_post_send failed: ", ret, " ", std::strerror(errno)));
  }
  VLOG(2) << "ibv_post_send";
  return QueuePair::PingTime{before, absl::GetCurrentTimeNanos()};
}

absl::StatusOr<QueuePair::PingTime> QueuePair::SimplePostReceive(
    uint64_t addr, uint32_t length, uint32_t wr_id) {
  ibv_sge scatter_gather_entry = {
      .addr = addr, .length = length, .lkey = recv_lkey_space_.GetMemoryKey()};
  ibv_recv_wr work_request = {
      .wr_id = wr_id + 200,
      .next = nullptr,
      .sg_list = &scatter_gather_entry,
      .num_sge = 1,
  };
  ibv_recv_wr *bad_wr;
  int64_t before = absl::GetCurrentTimeNanos();
  int ret = ibv_post_recv(queue_pair_.get(), &work_request, &bad_wr);
  if (ret != 0 && errno != 0) {
    return absl::InternalError(
        absl::StrCat("ibv_post_recv failed: ", ret, std::strerror(errno)));
  }
  return QueuePair::PingTime{before, absl::GetCurrentTimeNanos()};
}

absl::StatusOr<QueuePair::PingTime> QueuePair::SimplePostWrite(
    uint64_t addr, uint32_t length, uint32_t wr_id, uint64_t remote_addr) {
  // Simple post always send signaled.
  unsigned int send_flags = IBV_SEND_SIGNALED;
  if (length <= inline_threshold_) {
    // iRDMA will fail if the inline flag is set for payload > inline threshold.
    send_flags |= IBV_SEND_INLINE;
  }
  ibv_sge scatter_gather_entry = {
      .addr = addr,
      .length = length,
      .lkey = local_controlled_lkey_space_.GetMemoryKey()};
  ibv_send_wr work_request = {
      .wr_id = wr_id,
      .next = nullptr,
      .sg_list = &scatter_gather_entry,
      .num_sge = 1,
      .opcode = IBV_WR_RDMA_WRITE,
      .send_flags = send_flags,
  };
  work_request.wr.rdma.remote_addr = remote_addr,
  work_request.wr.rdma.rkey = remote_rkey_space_.GetMemoryKey();
  ibv_send_wr *bad_wr;
  int64_t before = absl::GetCurrentTimeNanos();
  VLOG(2) << "ibv_post_send";
  const int ret = ibv_post_send(queue_pair_.get(), &work_request, &bad_wr);
  if (ret != 0) {
    return absl::InternalError(
        absl::StrCat("ibv_post_send failed: ", ret, std::strerror(errno)));
  }
  return QueuePair::PingTime{before, absl::GetCurrentTimeNanos()};
}

absl::StatusOr<int64_t> QueuePair::PollMemory(int64_t addr) {
  int64_t now = absl::GetCurrentTimeNanos();
  int64_t until = now + 10 * utils::kSecondInNanoseconds;
  uint64_t *ptr = reinterpret_cast<uint64_t *>(addr);
  while (now < until) {
    now = absl::GetCurrentTimeNanos();
    // If the value is not 0, it is received from the peer.
    if (*ptr != 0) {
      // For debugging.
      // reset the value to 0.
      *ptr = 0;
      return now;
    }
  }
  return absl::DeadlineExceededError("Mem poll timed out");
}

absl::Status QueuePair::PopulateRemoteAttributes(
    proto::RemoteQueuePairAttributes &remote_attrs) {
  if (state_ < State::kInitialized) {
    return absl::FailedPreconditionError(
        "Must initialize resources before getting attributes.");
  }
  remote_attrs.set_qp_num(queue_pair_->qp_num);
  remote_attrs.set_gid(std::string(
      reinterpret_cast<const char *>(local_address_.gid.raw), kGidLength));
  if (remote_controlled_memory_block_metadata_.has_value()) {
    remote_attrs.mutable_rkeys()->Add(
        remote_controlled_memory_region_rkeys_.begin(),
        remote_controlled_memory_region_rkeys_.end());
    remote_attrs.set_addr(reinterpret_cast<uint64_t>(
        remote_controlled_memory_block_metadata_->address));
  }
  return absl::OkStatus();
}

void QueuePair::InstallStatsTrackerKey(std::pair<proto::RdmaOp, int> key) {
  const proto::MetricsCollection &metrics = config_.metrics_collection();
  statistics_trackers_.try_emplace(key, metrics.enable_per_second_stats(),
                                   metrics.bypass_tdigest_buffering());
}

absl::StatusOr<int> QueuePair::HandleExternalCompletion(
    const ibv_wc &completion, int64_t before_poll, int64_t after_poll) {
  if (should_trace_) {
    // There are only kTracingLatencyBufferSize items. Overwrite if needed.
    before_poll_[completion.wr_id % kTracingLatencyBufferSize] = before_poll;
    after_poll_[completion.wr_id % kTracingLatencyBufferSize] = after_poll;
  }

  //
  // Make sure operation was successful.
  if (completion.status != IBV_WC_SUCCESS) {
    LOG_EVERY_N(INFO, 10000) << absl::StrCat(
        "Operation with wr_id ", completion.wr_id,
        " with op code: ", code_to_op(completion.opcode),
        " completed with error: ", ibv_wc_status_str(completion.status));
    ProcessFailedCompletion(completion);
    return 0;
  }

  if (completion.opcode == IBV_WC_RECV ||
      completion.opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
    auto addr_or_error = ProcessReceiveCompletion(completion);
    if (!addr_or_error.ok()) {
      return addr_or_error.status();
    }
    uint64_t addr = addr_or_error.value();
    if (should_send_time_stamp_) {
      int iter = completion.wr_id % kTracingLatencyBufferSize;
      before_poll_[iter] = before_poll;
      after_poll_[iter] = after_poll;
      timestamp_received_[iter] = *(reinterpret_cast<int64_t *>(addr));
    }
    // Post a new receive operation at the address at which the operation
    // just completed.
    if (absl::StatusOr<int32_t> status_or_bytes_posted = PostRecv(addr);
        !status_or_bytes_posted.ok()) {
      return absl::Status(
          status_or_bytes_posted.status().code(),
          absl::StrCat("QueuePair halting PollCompletions because "
                       "PostRecv failed: ",
                       status_or_bytes_posted.status().message()));
    }
    // Return 0 since recvs do not count towards load calculation.
    return 0;
  } else {
    ++num_completions_received_;

    int op_count = 0;
    uint64_t wr_id = completion.wr_id;
    while (wr_id != 0) {
      // Process each outstanding op associated with the completion.
      // Multiple ops may be chained together via
      // OutstandingOperation.unsignaled_wr_id_to_process.
      ++op_count;
      auto status_or_next =
          ProcessCompleted(/*check_data_landed=*/false, wr_id, after_poll);
      if (!status_or_next.ok()) {
        return status_or_next.status();
      }
      wr_id = status_or_next.value();
    }
    return op_count;
  }
}

absl::StatusOr<int64_t> QueuePair::ProcessCompleted(
    bool check_data_landed, const int64_t wr_id,
    const int64_t completion_time) {
  auto it = outstanding_initiated_operations_.find(wr_id);
  if (it == outstanding_initiated_operations_.end()) {
    failed_completion_messages_.push_back(
        absl::StrCat("Completion with wr_id ", wr_id,
                     " does not correspond to an outstanding operation."));
    return absl::NotFoundError(failed_completion_messages_.back());
  }

  OutstandingOperation &op = it->second;
  int64_t unsignaled_wr_id_to_process = op.unsignaled_wr_id_to_process;

  // If desired, check that the memory at the initiator and target is the
  // same.
  if (check_data_landed) {
    if (op.target_addr.has_value()) {
      if (config_.validate_op_buffers() &&
          (op.op_type == proto::RdmaOp::RDMA_OP_WRITE ||
           op.op_type == proto::RdmaOp::RDMA_OP_WRITE_IMMEDIATE)) {
        if (should_trace_ && !should_send_time_stamp_) {
          // If tracing is requested, expect the iteration # in the payload.
          int64_t *buffer = reinterpret_cast<int64_t *>(op.target_addr.value());
          if (*buffer != wr_id % kTracingLatencyBufferSize) {
            return absl::InternalError("Unexpected value");
          }
        } else {
          uint8_t *buffer = reinterpret_cast<uint8_t *>(op.target_addr.value());
          for (int32_t i = 0; i < op.length_bytes; ++i) {
            if (buffer[i] != GetBufferVal(i, wr_id)) {
              return absl::InternalError(absl::StrCat(
                  "Data from operation with id ", wr_id, " did not land."));
            }
          }
        }
      } else if (std::memcmp(
                     reinterpret_cast<uint8_t *>(op.initiator_addr),
                     reinterpret_cast<uint8_t *>(op.target_addr.value()),
                     op.length_bytes) != 0) {
        return absl::InternalError(absl::StrCat("Data from operation with id ",
                                                wr_id, " did not land."));
      }
    }
  } else if (config_.validate_op_buffers()) {
    if (op.op_type == proto::RdmaOp::RDMA_OP_READ) {
      char *buffer = reinterpret_cast<char *>(op.initiator_addr);
      LOG(INFO) << "Hash of initiator data: "
                << farmhash::Fingerprint64(buffer, op.length_bytes)
                << " for op " << proto::RdmaOp_Name(op.op_type);
    }
  }

  // Store latency and throughput statistics.
  if (collect_measurements_) {
    VLOG(4) << "QueuePair " << config_.queue_pair_id() << ": operation of type "
            << utils::OpTypeToString(op.op_type) << " completed in "
            << (completion_time - op.sent_at_nanos) << " nanoseconds.";

    auto it = statistics_trackers_.find(std::pair{op.op_type, op.length_bytes});
    if (it != statistics_trackers_.end()) {
      it->second.AddSample(op.sent_at_nanos, completion_time, op.length_bytes,
                           outstanding_initiated_operations_.size());
    } else {
      LOG(ERROR) << "Received unexpected operation type: "
                 << proto::RdmaOp_Name(op.op_type)
                 << ", size: " << op.length_bytes;
    }
  }
  outstanding_initiated_operations_.erase(it);
  ++num_completed_operations_;
  return unsignaled_wr_id_to_process;
}

absl::Status QueuePair::PostOps(const OpTypeSize &op_type_size,
                                const int32_t batch_size,
                                const OpSignalType signal_type,
                                std::optional<ibv_ah *> address_handle) {
  proto::RdmaOp op_type = op_type_size.op_type;
  int32_t op_bytes = op_type_size.op_size;
  auto create_error_message = [op_type, op_bytes,
                               batch_size](absl::string_view message) {
    return absl::StrCat("QueuePair failed to post operation of type ",
                        utils::OpTypeToString(op_type), ", size ", op_bytes,
                        "B, batch_size ", batch_size, " because ", message);
  };

  if (batch_size <= 0) {
    return absl::InvalidArgumentError(
        create_error_message("batch_size must be positive"));
  }

  if (config_.max_outstanding_ops() > 0 &&
      batch_size > config_.max_outstanding_ops()) {
    return absl::InvalidArgumentError(create_error_message(
        "batch_size is larger than the configured max outstanding"));
  }

  if (op_bytes > config_.max_op_size()) {
    return absl::InvalidArgumentError(create_error_message(absl::StrCat(
        "maximum supported size is ", config_.max_op_size(), ". ")));
  }

  if (NumOutstandingOps() >= wqe_cap_) {
    VLOG(2) << "Reach the WQE cap: " << NumOutstandingOps();
    return absl::OutOfRangeError(kWqeCapReached);
  }

  ibv_wr_opcode opcode;
  switch (op_type) {
    case proto::RdmaOp::RDMA_OP_SEND_RECEIVE:
      opcode = IBV_WR_SEND;
      break;
    case proto::RdmaOp::RDMA_OP_READ:
      opcode = IBV_WR_RDMA_READ;
      break;
    case proto::RdmaOp::RDMA_OP_WRITE:
      opcode = IBV_WR_RDMA_WRITE;
      break;
    case proto::RdmaOp::RDMA_OP_SEND_WITH_IMMEDIATE_RECEIVE:
      opcode = IBV_WR_SEND_WITH_IMM;
      break;
    case proto::RdmaOp::RDMA_OP_WRITE_IMMEDIATE:
      opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
      break;
    default:
      return absl::InvalidArgumentError(
          create_error_message("operation type unsupported"));
  }

  if (!local_controlled_address_space_.has_value()) {
    return absl::FailedPreconditionError(create_error_message(
        "this op type requires MemoryRegionType::kLocalControlled to have been "
        "initialized."));
  }

  int64_t send_time_stamp = absl::GetCurrentTimeNanos();
  for (int i = 0; i < batch_size; ++i) {
    std::optional<uint64_t> target_op_addr;
    if (op_type != proto::RDMA_OP_SEND_RECEIVE &&
        op_type != proto::RDMA_OP_SEND_WITH_IMMEDIATE_RECEIVE) {
      if (!remote_attributes_->addr() || remote_attributes_->rkeys().empty()) {
        return absl::FailedPreconditionError(create_error_message(
            "this op type requires a remote address and rkey."));
      }
      target_op_addr = remote_address_space_->GetMemoryAddress();
    }

    uint64_t initiator_op_addr =
        local_controlled_address_space_->GetMemoryAddress();
    uint64_t wr_id = GetNextWrId();
    if (op_type != proto::RdmaOp::RDMA_OP_READ &&
        (config_.populate_op_buffers() || config_.validate_op_buffers())) {
      uint8_t *buffer = reinterpret_cast<uint8_t *>(initiator_op_addr);
      VLOG(2) << "Setting buffer to send for " << wr_id;
      // Fill in the buffer with deterministic data so that it can be
      // validated on separate machines or processes.
      for (int32_t i = 0; i < op_bytes; ++i) {
        buffer[i] = GetBufferVal(i, wr_id);
        VLOG(2) << "i: " << i << " val: " << static_cast<int>(buffer[i]);
      }
    }
    if (should_trace_ && op_bytes >= 8) {
      if (should_send_time_stamp_) {
        // Copy the timestamp to the buffer when send.
        *(reinterpret_cast<int64_t *>(initiator_op_addr)) = send_time_stamp;
      } else {
        // If tracing is requested, put the iteration # in the payload.
        *(reinterpret_cast<int64_t *>(initiator_op_addr)) =
            wr_id % kTracingLatencyBufferSize;
      }
    }

    unsigned int send_flags = IBV_SEND_SIGNALED;
    if (signal_type == OpSignalType::kLastInBatch && i != batch_size - 1) {
      send_flags = 0;
    }

    // Send inline if the size is within the threshold.
    if (op_bytes <= inline_threshold_ &&
        (opcode == IBV_WR_SEND || opcode == IBV_WR_RDMA_WRITE)) {
      // Some document such as https://www.cs.unh.edu/~rdr/rdr-hpcc12.pdf
      // suggests that inline can be used with _IMM while source such as
      // https://linux.die.net/man/3/ibv_post_send says it is only valid with
      // SEND and WRITE. The user manual from mellanox does not say one way or
      // the other. Some providers supports only SEND and WRITE.
      // We stick to SEND and WRITE at this point.
      send_flags = send_flags | IBV_SEND_INLINE;
    }

    scatter_gather_entries_[i] = {
        .addr = initiator_op_addr,
        .length = static_cast<uint32_t>(op_bytes),
        .lkey = local_controlled_lkey_space_.GetMemoryKey()};
    work_requests_[i] = {
        .wr_id = wr_id,
        .next = nullptr,
        .sg_list = &scatter_gather_entries_[i],
        .num_sge = 1,
        .opcode = opcode,
        .send_flags = send_flags,
    };
    if (opcode == IBV_WR_RDMA_WRITE_WITH_IMM ||
        opcode == IBV_WR_SEND_WITH_IMM) {
      // imm_data can be 32bit arbitrary data.
      work_requests_[i].imm_data = htonl(op_bytes);
    }
    if (address_handle.has_value()) {
      work_requests_[i].wr.ud = {
          .ah = *address_handle,
          .remote_qpn = remote_attributes_->qp_num(),
          .remote_qkey = kQKey,
      };
    }
    if (i > 0) {
      work_requests_[i - 1].next = &work_requests_[i];
    }
    if (target_op_addr.has_value()) {
      work_requests_[i].wr.rdma.remote_addr = target_op_addr.value();
      work_requests_[i].wr.rdma.rkey = remote_rkey_space_.GetMemoryKey();
    }
  }

  return PostAndProcess(batch_size, op_type, op_bytes, signal_type,
                        send_time_stamp);
}

absl::Status QueuePair::PostAndProcess(int batch_size, proto::RdmaOp op_type,
                                       int32_t op_bytes,
                                       OpSignalType signal_type,
                                       int64_t before_timestamp_sent) {
  ibv_send_wr *bad_wr;

  // Save the time at which the operation was posted so that we can later
  // compute its latency.
  const int64_t before_post = absl::GetCurrentTimeNanos();
  VLOG(2) << "ibv_post_send";
  const int ret = ibv_post_send(queue_pair_.get(), &work_requests_[0], &bad_wr);
  const int64_t after_post = absl::GetCurrentTimeNanos();
  if (ret != 0 && errno != 0) {
    return absl::InternalError(
        absl::StrCat("ibv_post_send failed with return value: ", ret,
                     ", error: ", std::strerror(errno)));
  }
  bool has_remote_addr = true;
  if (op_type == proto::RDMA_OP_SEND_RECEIVE ||
      op_type == proto::RDMA_OP_SEND_WITH_IMMEDIATE_RECEIVE) {
    // SEND and SEND_WITH_IMM do not have remote address.
    has_remote_addr = false;
  }
  for (int i = 0; i < batch_size; ++i) {
    int64_t next_wr_to_process = 0;
    const ibv_send_wr &wr = work_requests_[i];
    if (should_trace_) {
      // Put the detailed timestamp in the buffer.
      if (should_send_time_stamp_) {
        before_post_[wr.wr_id % kTracingLatencyBufferSize] =
            before_timestamp_sent;
      } else {
        before_post_[wr.wr_id % kTracingLatencyBufferSize] = before_post;
      }
      after_post_[wr.wr_id % kTracingLatencyBufferSize] = after_post;
    }
    if (signal_type == OpSignalType::kLastInBatch && i > 0) {
      next_wr_to_process = work_requests_[i - 1].wr_id;
    }
    outstanding_initiated_operations_[wr.wr_id] = {
        .sent_at_nanos = before_post,
        .initiator_addr = wr.sg_list->addr,
        .target_addr = has_remote_addr
                           ? std::make_optional(wr.wr.rdma.remote_addr)
                           : std::nullopt,
        .length_bytes = op_bytes,
        .op_type = op_type,
        .unsignaled_wr_id_to_process = next_wr_to_process};
  }
  return absl::OkStatus();
}

absl::StatusOr<int32_t> QueuePair::PostRecv(uint64_t addr) {
  uint64_t wr_id = GetNextWrId();
  uint32_t size = static_cast<uint32_t>(config_.max_op_size());
  // UD receive buffers must allow space for the Global Routing Header at the
  // beginning of the buffer.
  if (config_.connection_type() == proto::CONNECTION_TYPE_UD) {
    size += sizeof(ibv_grh);
  }
  ibv_sge sge = {
      .addr = addr, .length = size, .lkey = recv_lkey_space_.GetMemoryKey()};
  ibv_recv_wr recv = {
      .wr_id = wr_id,
      .next = nullptr,
      .sg_list = &sge,
      .num_sge = 1,
  };

  ibv_recv_wr *bad_wr;
  VLOG(2) << "ibv_post_recv";
  int ret = ibv_post_recv(queue_pair_.get(), &recv, &bad_wr);
  if (ret != 0) {
    return absl::InternalError(absl::StrCat(
        "PostRecv failed because ibv_post_recv failed with return value: ", ret,
        ", error: ", std::strerror(errno)));
  }

  SetOutstandingRecvOpAddress(wr_id, addr);
  return size;
}

absl::Status QueuePair::ConnectToRemoteQp(
    const proto::RemoteQueuePairAttributes &remote_attributes) {
  VLOG(2) << "Trying to connect to remote QP: " << remote_attributes;
  if (state_ < State::kInitialized) {
    return absl::FailedPreconditionError(
        "Must initialize resources before connecting to remote.");
  }
  if (state_ >= State::kConnected) {
    return absl::AlreadyExistsError(
        "RcQueuePair is already connected to a remote.");
  }
  remote_attributes_.emplace(remote_attributes);
  if (remote_attributes.addr()) {
    // The remote's memory region will be equal in size to the local.
    remote_address_space_.emplace(
        remote_attributes.addr(),
        absl::GetFlag(FLAGS_qp_memory_space_slots) * config_.max_op_size());
    remote_rkey_space_.Initialize(remote_attributes.rkeys());
  }

  // 1. Transition to Init state.
  ibv_qp_attr mod_init = {
      .qp_state = IBV_QPS_INIT,
      .qp_access_flags = kRemoteAccessAll,
      // Primary P_Key index. The value of the entry in the P_Key table that
      // outgoing packets from this QP will be sent with and incoming packets
      // to this QP will be verified within the Primary path.
      .pkey_index = 0,
      .port_num = local_address_.port,
  };
  constexpr int kInitMask =
      IBV_QP_STATE | IBV_QP_ACCESS_FLAGS | IBV_QP_PORT | IBV_QP_PKEY_INDEX;

  VLOG(2) << "ibv_modify_qp Transition to Init state.";
  if (int ret = ibv_modify_qp(queue_pair_.get(), &mod_init, kInitMask);
      ret != 0) {
    return absl::InternalError(
        absl::StrCat("Modify Qp (init) failed with return value: ", ret, " : ",
                     std::strerror(errno)));
  }

  // 2. Transition to Ready to Receive state.
  ibv_qp_attr mod_rtr = {
      .qp_state = IBV_QPS_RTR,
      .path_mtu = local_address_.mtu,
      .rq_psn = kArbitraryPacketSequenceNumber,
      .dest_qp_num = remote_attributes_->qp_num(),
      .ah_attr = {.grh =
                      {
                          // Global routing header
                          .sgid_index = local_address_.gid_index,
                          .hop_limit = 127,  // The maximum number of hops
                                             // outside of the local subnet.
                          .traffic_class =
                              static_cast<uint8_t>(config_.traffic_class()),
                      },
                  .is_global = 1,  // Sets grh field to valid
                  .port_num = local_address_.port},
      // The maximum number of RDMA reads/atomics that can be handled by this
      // QP as destination.
      .max_dest_rd_atomic =
          static_cast<uint8_t>(absl::GetFlag(FLAGS_max_dest_rd_atomic))};
  memcpy(mod_rtr.ah_attr.grh.dgid.raw, remote_attributes_->gid().c_str(),
         kGidLength);
  mod_rtr.ah_attr.sl = 5;
  // Requires for RC RTR: Minimum RNR NAK Timer Field Value. 1 means 1us, 12
  // means 64 us, 26 means 82us, and 31 means 491us for RoCE.
  if (config_.min_rnr_timer()) {
    mod_rtr.min_rnr_timer = config_.min_rnr_timer();
  } else {
    mod_rtr.min_rnr_timer = 31;
  }

  constexpr int kRtrMask = IBV_QP_STATE | IBV_QP_PATH_MTU | IBV_QP_RQ_PSN |
                           IBV_QP_DEST_QPN | IBV_QP_AV |
                           IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;

  int modify_qp_ret = 0;
  VLOG(2) << "ibv_modify_qp Transition to Ready to Receive state.";
  modify_qp_ret = ibv_modify_qp(queue_pair_.get(), &mod_rtr, kRtrMask);

  if (modify_qp_ret != 0) {
    return absl::InternalError(absl::StrCat(
        "Modify QP (RtR) failed with return value: ", modify_qp_ret, " : ",
        std::strerror(errno)));
  }

  // 3. Transition to Ready to Send state.
  ibv_qp_attr mod_rts = {.qp_state = IBV_QPS_RTS,
                         .sq_psn = kArbitraryPacketSequenceNumber,
                         // The maximum number of RDMA reads/atomics that can
                         // be handled by this QP as initiator.
                         .max_rd_atomic = static_cast<uint8_t>(
                             absl::GetFlag(FLAGS_max_rd_atomic))};
  // RC RTR->RTS requires the following attributes:
  // The minimum timeout that a QP waits for ACK/NACK from remote QP before
  // retransmitting the packet. The value zero is special value which means wait
  // an infinite time for the ACK/NACK (useful for debugging). For any other
  // value of timeout, the time calculation is: 4.096*2^{timeout} usec.
  // For RoCE, 0~31 are valid. For Falcon, 0~15 are valid. Higher bits will be
  // masked out and won't cause failure.
  if (config_.timeout()) {
    mod_rts.timeout = config_.timeout();
  } else {
    mod_rts.timeout = 23;  // 34.3 sec
  }
  // A 3 bits value of the total number of times that the QP will try to
  // resend the packets when an RNR NACK was sent by the remote QP before
  // reporting an error. The value 7 is special and specify to retry infinite
  // times in case of RNR
  mod_rts.rnr_retry = 7;
  // A 3 bits value of the total number of times that the QP will try to
  // resend the packets before reporting an error because the remote side
  // doesn't answer in the primary path
  mod_rts.retry_cnt = 7;
  constexpr int kRtsMask = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_SQ_PSN |
                           IBV_QP_MAX_QP_RD_ATOMIC | IBV_QP_RETRY_CNT |
                           IBV_QP_RNR_RETRY;
  VLOG(2) << "ibv_modify_qp Transition to Ready to Send state. QP: "
          << queue_pair_->qp_num;
  if (int ret = ibv_modify_qp(queue_pair_.get(), &mod_rts, kRtsMask);
      ret != 0) {
    return absl::InternalError(
        absl::StrCat("Modify QP (RtS) failed with return value: ", ret, " : ",
                     std::strerror(errno)));
  }

  state_ = State::kConnected;
  return absl::OkStatus();
}

absl::Status QueuePair::ConnectToRemoteAh(
    const proto::RemoteQueuePairAttributes &remote_attributes) {
  if (state_ < State::kInitialized) {
    return absl::FailedPreconditionError(
        "Must initialize resources before connecting to remote.");
  }
  if (state_ >= State::kConnected) {
    return absl::AlreadyExistsError(
        "UdQueuePair is already connected to a remote.");
  }
  remote_attributes_.emplace(remote_attributes);

  // Create an address handle to the remote.
  ibv_ah_attr ah_attr = {
      .grh = {.sgid_index = local_address_.gid_index,  // Global routing header
              .hop_limit = 127,
              .traffic_class = static_cast<uint8_t>(config_.traffic_class())},
      .is_global = 1,
      .port_num = local_address_.port};
  memcpy(ah_attr.grh.dgid.raw, remote_attributes.gid().c_str(), kGidLength);
  address_handle_ =
      std::unique_ptr<ibv_ah, ibverbs_utils::AddressHandleDeleter>(
          ibv_create_ah(protection_domain_, &ah_attr),
          ibverbs_utils::AddressHandleDeleter());

  // 1. Transition to Init state.
  ibv_qp_attr mod_init = {.qp_state = IBV_QPS_INIT,
                          .qkey = kQKey,
                          .pkey_index = 0,
                          .port_num = local_address_.port};
  constexpr int kInitMask =
      IBV_QP_STATE | IBV_QP_QKEY | IBV_QP_PKEY_INDEX | IBV_QP_PORT;
  VLOG(2) << "ibv_modify_qp Transition to Init state. QP: "
          << queue_pair_->qp_num;
  if (int ret = ibv_modify_qp(queue_pair_.get(), &mod_init, kInitMask);
      ret != 0) {
    return absl::InternalError(
        absl::StrCat("Modify Qp (init) failed with return value: ", ret, " : ",
                     std::strerror(errno)));
  }

  // 2. Transition to Ready to Receive state.
  ibv_qp_attr mod_rtr = {.qp_state = IBV_QPS_RTR};
  constexpr int kRtrMask = IBV_QP_STATE;
  VLOG(2) << "ibv_modify_qp Transition to Ready to Receive state. QP: "
          << queue_pair_->qp_num;
  if (int ret = ibv_modify_qp(queue_pair_.get(), &mod_rtr, kRtrMask);
      ret != 0) {
    return absl::InternalError(
        absl::StrCat("Modify QP (RtR) failed with return value: ", ret, " : ",
                     std::strerror(errno)));
  }

  // 3. Transition to Ready to Send state.
  ibv_qp_attr mod_rts = {
      .qp_state = IBV_QPS_RTS,
      .sq_psn = kArbitraryPacketSequenceNumber,
  };
  constexpr int kRtsMask = IBV_QP_STATE | IBV_QP_SQ_PSN;
  VLOG(2) << "ibv_modify_qp Transition to Ready to Send state. QP: "
          << queue_pair_->qp_num;
  if (int ret = ibv_modify_qp(queue_pair_.get(), &mod_rts, kRtsMask);
      ret != 0) {
    return absl::InternalError(
        absl::StrCat("Modify QP (RtS) failed with return value: ", ret, " : ",
                     std::strerror(errno)));
  }

  state_ = State::kConnected;
  return absl::OkStatus();
}

void QueuePair::StatisticsTracker::Flush() {
  if (latency_buffer_count_ == 0) {
    return;
  }

  int full_buffer_size = latency_buffer_.size();
  if (latency_buffer_count_ < full_buffer_size) {
    // We must temporarily resize when merging fewer samples than vector size.
    // TDigest::merge API expects a full vector.
    latency_buffer_.resize(latency_buffer_count_);
  }
  // Sort before merging to avoid doing it twice.
  std::sort(latency_buffer_.begin(), latency_buffer_.end());
  latency_tdigest_ =
      latency_tdigest_.merge(folly::sorted_equivalent, latency_buffer_);
  if (enable_per_second_stats_) {
    latency_tdigest_current_second_ = latency_tdigest_current_second_.merge(
        folly::sorted_equivalent, latency_buffer_);
  }

  // Restore original size if buffer was resized for merge.
  latency_buffer_.resize(full_buffer_size);
  latency_buffer_count_ = 0;
}

void QueuePair::StatisticsTracker::AppendPerSecondStatistics(
    const std::vector<proto::ThroughputResult> &throughput_intervals,
    int outstanding_op_count) {
  absl::MutexLock lock(&statistics_per_second_mutex_);
  for (const auto &tput : throughput_intervals) {
    proto::Statistics *stats_for_last_second =
        statistics_per_second_.add_statistics();
    // Append measurements from the finished interval to the proto.
    *stats_for_last_second->mutable_throughput() = tput;
    *stats_for_last_second->mutable_latency() =
        utils::LatencyTdigestToProto(latency_tdigest_current_second_);
    stats_for_last_second->set_outstanding_ops(outstanding_op_count);
    // Reset the per-second latency tdigest.
    latency_tdigest_current_second_ = folly::TDigest();
  }
}

void QueuePair::StatisticsTracker::AddSample(int64_t started_ns,
                                             int64_t completed_ns,
                                             int32_t op_bytes,
                                             int outstanding_op_count) {
  std::vector<proto::ThroughputResult> finished_tput_intervals =
      throughput_computer_.AddMeasurement(completed_ns, op_bytes);

  if (ABSL_PREDICT_FALSE(enable_per_second_stats_ &&
                         !finished_tput_intervals.empty())) {
    // We have entered a new second interval, so we must first merge any values
    // in the latency buffer into the current per-second tdigest, then start a
    // new one.
    Flush();
    AppendPerSecondStatistics(finished_tput_intervals, outstanding_op_count);
  }

  latency_buffer_[latency_buffer_count_++] = (completed_ns - started_ns);
  if (latency_buffer_count_ == latency_buffer_.size()) {
    Flush();
  }
}

void QueuePair::StatisticsTracker::Finish(int64_t end_ns,
                                          int outstanding_op_count) {
  if (latency_buffer_count_ == 0 && latency_tdigest_.count() == 0) {
    return;
  }

  std::vector<proto::ThroughputResult> finished_tput_intervals =
      throughput_computer_.FinishMeasurements(end_ns);

  Flush();
  if (enable_per_second_stats_) {
    AppendPerSecondStatistics(finished_tput_intervals, outstanding_op_count);
  }
}

void QueuePair::StatisticsTracker::Reset() {
  absl::MutexLock lock(&statistics_per_second_mutex_);
  latency_buffer_count_ = 0;
  latency_tdigest_ = folly::TDigest();
  latency_tdigest_current_second_ = folly::TDigest();
  throughput_computer_ = ThroughputComputer();
  statistics_per_second_.clear_statistics();
}

proto::PerTrafficResult::PerOpTypeSizeResult
QueuePair::StatisticsTracker::GetFinalResults() const {
  absl::MutexLock lock(&statistics_per_second_mutex_);
  proto::PerTrafficResult::PerOpTypeSizeResult result;
  int op_count = latency_tdigest_.count();
  result.set_num_successful_ops(op_count);
  *result.mutable_latency() = utils::LatencyTdigestToProto(latency_tdigest_);
  if (op_count > 0) {
    *result.mutable_throughput() = throughput_computer_.GetAverageResult();
    *result.mutable_statistics_per_second() = statistics_per_second_;
  }
  return result;
}

proto::CurrentStats::PerOpTypeSizeCurrentStats *
QueuePair::StatisticsTracker::PopCurrentStatistics(
    proto::CurrentStats::QueuePairCurrentStats *qp_stats_output) {
  absl::MutexLock lock(&statistics_per_second_mutex_);
  if (statistics_per_second_.statistics_size() == 0) {
    // Skip if no second intervals have completed yet.
    return nullptr;
  }

  proto::CurrentStats::PerOpTypeSizeCurrentStats *appended_stats =
      qp_stats_output->add_per_op_type_size_current_stats();

  *appended_stats->mutable_stats_per_second() = statistics_per_second_;
  statistics_per_second_.clear_statistics();

  return appended_stats;
}

}  // namespace verbsmarks
