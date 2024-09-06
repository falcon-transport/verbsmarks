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

#ifndef VERBSMARKS_QUEUE_PAIR_H_
#define VERBSMARKS_QUEUE_PAIR_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/flags/flag.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/optional.h"
#include "completion_queue_manager.h"
#include "connection_coordinator.pb.h"
#include "folly/stats/TDigest.h"
#include "ibverbs_utils.h"
#include "infiniband/verbs.h"
#include "throughput_computer.h"
#include "utils.h"
#include "verbsmarks.pb.h"
#include "verbsmarks_binary_flags.h"

// The size of the buffer to keep detailed latency in tracing mode.
const int kTracingLatencyBufferSize = 128;
// Number of latency samples to buffer before merging into tdigest.
// Evaluation of tdigest merge performance vs. buffer depth is at
const int kLatencyTdigestBufferCount = 1024;
const int kTracingPauseAt = 20;
const int kTracingStopAt = kTracingPauseAt + 4;
const char kWqeCapReached[] = "Reached the WQE cap.";

namespace verbsmarks {

struct OpTypeSize {
  proto::RdmaOp op_type;
  int32_t op_size;
};

struct MemoryBlockMetadata {
  std::uintptr_t address;
  std::size_t size;

  MemoryBlockMetadata(std::uintptr_t address, std::size_t size)
      : address(address), size(size) {}
};

// The view describes the resources belonging to a queue pair but does not bear
// ownership of any object.
struct QueuePairMemoryResourcesView {
  ibv_pd *protection_domain;
  // A type of memory might not exist. The external resource allocator (e.g.,
  // MemoryManager) determines and only allocates the necessary types of memory
  // regions and blocks for the traffic characteristics that a QueuePair runs.
  std::optional<std::vector<ibv_mr *>> recv_memory_regions;
  std::optional<std::vector<ibv_mr *>> local_controlled_memory_regions;
  std::optional<std::vector<ibv_mr *>> remote_controlled_memory_regions;
  std::optional<MemoryBlockMetadata> recv_memory_block_metadata;
  std::optional<MemoryBlockMetadata> local_controlled_memory_block_metadata;
  std::optional<MemoryBlockMetadata> remote_controlled_memory_block_metadata;
};

// A simple memory space that manages the pre-allocated space address in an
// array-based manner. The allocated address is reused even it is not released,
// i.e., we do not have memory resource limitation.
// Specifically, given the memory region starting address, size and number of
// memory chunks, we use an array to store the address of each memory chunk.
// When calling the function GetMemoryAddress(), it will return the address of
// current memory chunk, and then advance to the next memory chunk. If reaching
// the end of the array, we start over again.
struct ArrayMemorySpace {
  // The array stores the starting address of each memory chunk.
  std::vector<uint64_t> memory_chunk_addresses;
  // Index of current memory address for allocation.
  uint32_t index;

  ArrayMemorySpace(uint64_t memory_region_address,
                   int32_t memory_region_size_bytes) {
    int32_t number_of_memory_blocks =
        absl::GetFlag(FLAGS_qp_memory_space_slots);
    uint32_t memory_chunk_size_bytes =
        memory_region_size_bytes / number_of_memory_blocks;
    for (int i = 0; i < number_of_memory_blocks; ++i) {
      memory_chunk_addresses.push_back(memory_region_address);
      memory_region_address += memory_chunk_size_bytes;
    }
    index = 0;
  }

  // Returns the address in the memory space of the current item in the array,
  // and moves the index pointing to the next item.
  inline uint64_t GetMemoryAddress() {
    uint64_t allocated_address = memory_chunk_addresses[index];
    index = index + 1 == memory_chunk_addresses.size() ? 0 : index + 1;
    return allocated_address;
  }
};

// A simple memory key space that manages the keys of multiple memory regions in
// an array-based manner. When calling the function GetMemoryKey(), it will
// return the key of current memory region, and then advance to the next memory
// region. If reaching the end of the array, we start over again.
class ArrayMemoryKeySpace {
 public:
  // Initialize from rkeys in remote attributes.
  void Initialize(const google::protobuf::RepeatedField<uint32_t> &rkeys) {
    memory_keys_ = {rkeys.begin(), rkeys.end()};
    index_ = 0;
  }

  // Initialize from lkeys in local memory regions.
  void Initialize(const std::vector<ibv_mr *> &mrs) {
    for (auto mr : mrs) {
      memory_keys_.push_back(mr->lkey);
    }
    index_ = 0;
  }

  // Returns the current key in the array and moves the index pointing to the
  // next item.
  inline uint32_t GetMemoryKey() {
    int32_t allocated_key = memory_keys_[index_];
    index_ = (index_ + 1 == memory_keys_.size()) ? 0 : index_ + 1;
    return allocated_key;
  }

 private:
  // The array stores the key (lkey or rkey depending on constructor) of each
  // memory region.
  std::vector<uint32_t> memory_keys_;
  // Index of current memory address for allocation.
  uint32_t index_;
};

// Prints out the hash of the memory block for validation.
void ValidateMemoryBlock(
    std::optional<MemoryBlockMetadata> memory_block_metadata,
    absl::string_view name);

// The QueuePair class encapsulates the state necessary to issue
// ibverbs traffic on a queue pair, including the memory region and completion
// queue. The class keeps tracks of the operations it has issued and reports
// their latencies and timestamps. This class is thread compatible.
class QueuePair {
 public:
  // Creates a QueuePair that will issue traffic on the context and with the
  // address provided. The caller owns the context pointer. By default, the
  // QueuePair does not collect measurements from operations.
  QueuePair(ibv_context *context,
            const ibverbs_utils::LocalIbverbsAddress local_address,
            const proto::QueuePairConfig config, bool is_pingpong = false,
            CompletionQueueManager *completion_queue_manager = nullptr);

  // Makes class abstract, to be subclassed by different queue pair modes.
  virtual ~QueuePair() = default;

  // Destroys the queue pair explicitly. If not called, the destructor will
  // clean up.
  void Destroy() {
    if (queue_pair_ != nullptr) {
      auto qp = std::move(queue_pair_);
      queue_pair_ = nullptr;
      int ret = ibv_destroy_qp(qp.get());
      if (ret != 0) {
        LOG(FATAL) << "Failed to destroy QP";  // crash ok
      }
      qp.release();
    }
  }

  // Returns WQE cap.
  virtual int GetWqeCap() const { return wqe_cap_; }

  // The different kinds of memory regions that a queue pair might need. kRecv
  // will hold receive buffers. kLocalControlled will hold initiated send, read,
  // or write buffers. kRemoteControlled can be read from or written to by the
  enum class MemoryRegionType { kRecv, kLocalControlled, kRemoteControlled };

  // Uses protection domain, memory blocks, and regions from
  // QueuePairMemoryResourcesView. Creates a completion queue internally, or
  // retrieves an existing shared completion queue from CompletionQueueManager
  // if provided to the QueuePair constructor.
  // If `inline_threshold` is bigger than zero, try
  // to set max_inline_data with it and returns an error upon failure.
  virtual absl::Status InitializeResources(
      QueuePairMemoryResourcesView memory_resources_view,
      uint32_t inline_threshold);

  // Returns the internal ibverbs qp_num which appears on completions which
  // originated from this QP. Must be called after InitializeResources.
  virtual uint32_t GetIbvQpNum() const { return queue_pair_->qp_num; }

  // Sets the fields of `remote_attrs` to the attributes of this queue pair.
  // `InitializeResources` must be called before this method. Returns an error
  // if this queue pair's resources are not initialized, as some attributes are
  // properties of those resources.
  virtual absl::Status PopulateRemoteAttributes(
      proto::RemoteQueuePairAttributes &remote_attrs);

  // Implementations connect this queue pair to its remote counterpart.
  // Transitions the queue pair state from Reset -> Init -> Ready to Receive ->
  // Ready to Send. `InitializeResources` must be called before this method, and
  // this method must be called before posting operations. Returns an error if
  // this queue pair does not have its resources initialized or is already
  // connected to a remote.
  virtual absl::Status ConnectToRemote(
      const proto::RemoteQueuePairAttributes &remote_attributes) = 0;

  enum class OpSignalType { kSignalAll, kLastInBatch, kOnlyRecv };
  // Issues `batch_size` work requests with one operation each of the type and
  // size provided on the underlying ibv_qp. Implementations must assign the
  // `wr_id` field of each operation to `next_wr_id_`, increment it, and save
  // the start time of the operation in `outstanding_operations_`. If
  // `signal_only_last` is true, only the last one will be signaled and the rest
  // will be unsignaled. Because only one WR will generate a completion, work
  // requests in the batch will be chained via next_wr_to_process and be
  // processed when the signaled work request is processed.
  virtual absl::Status PostOps(const OpTypeSize &op_type_size,
                               int32_t batch_size, OpSignalType signal_type) {
    return PostOps(op_type_size, batch_size, signal_type, std::nullopt);
  }

  struct PingTime {
    int64_t before;
    int64_t after;
  };

  // Processes an ibv_wc completion by updating outstanding operation metadata,
  // and for recv completions, posting new recv buffers. Caller provides
  // timestamps for the polling call, from CompletionInfo. Returns change in
  // completed op count (either 1 or 0, since recvs are not counted), which
  // enables updating the load generator's load calculation.
  virtual absl::StatusOr<int> HandleExternalCompletion(const ibv_wc &completion,
                                                       int64_t before_poll,
                                                       int64_t after_poll);

  // Add the statistics from future operation completions to the statistics
  // trackers. Resets the statistics trackers so that they do not include
  // measurements from before `StopCollectingMeasurements` was called.
  virtual void StartCollectingMeasurements();

  // Initializes a statistics tracker for a given key. The traffic generator
  // shall call this upfront. If not called, the stats are not guaranteed but it
  // does not cause a crash.
  virtual void InstallStatsTrackerKey(std::pair<proto::RdmaOp, int> key);

  // Resume metrics collection if stopped, without resetting any existing
  // measurements.
  virtual void ResumeCollectingMeasurements();

  // Do not add the statistics from future operation completions to the
  // statistics trackers.
  virtual void StopCollectingMeasurements();

  // Prints out fingerprint of the buffers.
  virtual void ValidateOpBuffers();

  // Returns the number of operations that have been issued for which
  // completions have not yet been received.
  virtual const int inline NumOutstandingOps() const {
    return outstanding_initiated_operations_.size();
  }

  // Returns the total number of initiated operations that have successfully
  // completed, including those for which we did not collect statistics.
  virtual const int inline NumCompletedOperations() const {
    return num_completed_operations_;
  }

  // Returns the total number of work request completions received.
  virtual const int inline NumCompetionReceived() const {
    return num_completions_received_;
  }

  // Clears a failed completion from the tracking data structure.
  virtual void ProcessFailedCompletion(const ibv_wc &completion);

  virtual absl::StatusOr<uint64_t> ProcessReceiveCompletion(
      const ibv_wc &completion);

  // Processes internal tracking of a completed work request with `wr_id`.
  // Unsignaled work requests do not generate completion but they should be
  // processed when the completed work request in the batch is completed. Work
  // requests that should be processed together are chained in
  // `unsignaled_wr_id_to_process`. This function returns
  // `unsignaled_wr_id_to_process` at the end so we can clear tracking of
  // unsignaled work requests.
  virtual absl::StatusOr<int64_t> ProcessCompleted(bool check_data_landed,
                                                   int64_t wr_id,
                                                   int64_t completion_time);

  virtual absl::Status PostAndProcess(int batch_size, proto::RdmaOp op_type,
                                      int32_t op_bytes,
                                      OpSignalType signal_type,
                                      int64_t before_timestamp_sent);

  virtual void SetOutstandingRecvOpAddress(const uint64_t wr_id,
                                           const uint64_t addr) {
    outstanding_recv_operations_[wr_id] = addr;
  }

  // Throughput and latency tracker for QP operations. Intended use case is to
  // allocate one tracker per operation type/size combination, and call
  // AddSample() for each completed operation. When the workload is finished,
  // Finish() will process any buffered samples and GetFinalResults() will write
  // a PerOpTypeSizeResult with all calculated throughput/latency values.
  class StatisticsTracker {
   public:
    // Creates a StatisticsTracker with default options.
    StatisticsTracker() : StatisticsTracker(false, false) {}

    // Creates a StatisticsTracker given per-second and buffering options.
    // If enable_per_second_stats is set, throughput and latency will be
    // measured for each second interval of the experiment, in addition to
    // cumulative measurements. If bypass_tdigest_buffering is set, each
    // individual sample will be merged into the tdigest, reducing buffer
    // memory utilization, but also increasing CPU cycles spent on calculation.
    StatisticsTracker(bool enable_per_second_stats,
                      bool bypass_tdigest_buffering)
        : enable_per_second_stats_(enable_per_second_stats),
          latency_buffer_(
              bypass_tdigest_buffering ? 1 : kLatencyTdigestBufferCount),
          latency_buffer_count_(0) {}

    // Adds a sample given start and completion times (nanos) and operation
    // size, along with current outstanding op count. This will append the
    // latency value to the StatisticsTracker buffer and flush when full.
    void AddSample(int64_t started_ns, int64_t completed_ns, int op_bytes,
                   int outstanding_op_count);

    // Finishes measurement by ending throughput interval and flushing any
    // buffered latency samples.
    void Finish(int64_t end_ns, int outstanding_op_count);

    // Resets all stored measurements, preserving tracking options.
    void Reset();

    // Returns a result proto with final calculated throughput and latency.
    // Note that PerOpTypeSizeResult op type and size values are not set.
    // These values form the StatisticsTracker map key within TrafficGenerator,
    // so to minimize memory utilization they are not also maintained inside of
    // StatisticsTracker.
    proto::PerTrafficResult::PerOpTypeSizeResult GetFinalResults() const;

    // Writes per-second results into the provided proto, for any intervals
    // which have elapsed since the experiment start or previous call to this
    // function. Clears the popped stats from statistics_per_second_. Returns
    // the PerOpTypeSize proto if stats were available and written, nullptr
    // otherwise. Intended to be called during the experiment runtime by a
    // separate thread.
    proto::CurrentStats::PerOpTypeSizeCurrentStats *PopCurrentStatistics(
        proto::CurrentStats::QueuePairCurrentStats *qp_stats_output);

    // Returns the tracker's cumulative latency tdigest, allowing the caller to
    // calculate an aggregate tdigest.
    const folly::TDigest &GetLatencyTdigest() const { return latency_tdigest_; }

    // Move constructor. Necessary for correctness of per-second stats mutex
    // when StatisticsTrackers are emplaced directly as flat_hash_map values.
    StatisticsTracker(StatisticsTracker &&old) {
      absl::MutexLock old_lock(&old.statistics_per_second_mutex_);
      absl::MutexLock new_lock(&statistics_per_second_mutex_);
      enable_per_second_stats_ = old.enable_per_second_stats_;
      latency_buffer_ = old.latency_buffer_;
      latency_buffer_count_ = old.latency_buffer_count_;
      latency_tdigest_ = old.latency_tdigest_;
      latency_tdigest_current_second_ = old.latency_tdigest_current_second_;
      throughput_computer_ = old.throughput_computer_;
      statistics_per_second_ = old.statistics_per_second_;
    }

   private:
    // Flushes any samples from latency_buffer_ into cumulative and (if enabled)
    // per-second latency tdigests.
    void Flush();

    // Appends per-second statistics to statistics_per_second_ when one or more
    // intervals has elapsed (non-empty return from) throughput_computer_
    // AddMeasurement/FinishMeasurements. Saves the associated outstanding op
    // count into each interval record.
    void AppendPerSecondStatistics(
        const std::vector<proto::ThroughputResult> &throughput_intervals,
        int outstanding_op_count);

    bool enable_per_second_stats_;
    std::vector<double> latency_buffer_;
    int latency_buffer_count_;
    folly::TDigest latency_tdigest_;
    folly::TDigest latency_tdigest_current_second_;
    ThroughputComputer throughput_computer_;
    proto::StatisticsOverTime statistics_per_second_
        ABSL_GUARDED_BY(statistics_per_second_mutex_);
    mutable absl::Mutex statistics_per_second_mutex_;
  };

  // Access and process statistics of completed operations. The provided
  // function will be called with a map from operation type to statistics for
  // the operations of that type that have completed, and well as average
  // latencies trackers for segments of the posting and polling code. Note
  // that until `QueuePair::Finish` is called, the latency statistics will
  // include information about *all* completed operations, while the
  // throughput statistics will omit those operations in the most recent
  // second of the experiment.
  virtual void AccessStatistics(
      std::function<
          void(const absl::flat_hash_map<std::pair<proto::RdmaOp, int32_t>,
                                         StatisticsTracker> &,
               const utils::LatencyTracker &, const utils::LatencyTracker &)>
          fn) {
    fn(statistics_trackers_, post_latency_tracker_, poll_latency_tracker_);
  }

  // Retrieves current per-second statistics from each StatisticsTracker which
  // has been installed. Returns an empty result if per-second statistics are
  // not enabled in QueuePairConfig, or if a full second interval has not
  // finished since experiment start or a previous call to this function. Safe
  // to call from a separate thread (e.g., follower main thread).
  virtual proto::CurrentStats::QueuePairCurrentStats GetCurrentStatistics();

  bool is_pingpong_;
  // Primitive write / send / receive for special traffic patterns, such as
  // pingpong. These functions provide a simple ibv operations and do not track
  // states internally. The special traffic pattern should keep the state in the
  // traffic generator.
  virtual absl::StatusOr<QueuePair::PingTime> SimplePostWrite(
      uint64_t addr, uint32_t length, uint32_t wr_id, uint64_t remote_addr);
  virtual absl::StatusOr<QueuePair::PingTime> SimplePostSend(uint64_t addr,
                                                             uint32_t length,
                                                             uint32_t wr_id);
  virtual absl::StatusOr<QueuePair::PingTime> SimplePostReceive(uint64_t addr,
                                                                uint32_t length,
                                                                uint32_t wr_id);

  // Traffic generator can construct work request and post directly using this
  // function.
  virtual int PostSend(ibv_send_wr *wr, ibv_send_wr **bad_wr) {
    return ibv_post_send(queue_pair_.get(), wr, bad_wr);
  }

  // Traffic generator can construct recv work request and post directly using
  // this function.
  virtual int PostRecv(ibv_recv_wr *wr, ibv_recv_wr **bad_wr) {
    return ibv_post_recv(queue_pair_.get(), wr, bad_wr);
  }

  // When the value of the pointed address is non 0, it returns the timestamp.
  // And set the value back to 0.
  virtual absl::StatusOr<int64_t> PollMemory(int64_t addr);

  // The following functions give information to construct work requests.
  // Base memory addresses for each memory region.
  uint64_t GetLocallyControlledMemoryBaseAddr() const {
    return reinterpret_cast<uint64_t>(
        local_controlled_memory_block_metadata_->address);
  }
  uint64_t GetRemoteControlledMemoryBaseAddr() const {
    return reinterpret_cast<uint64_t>(
        remote_controlled_memory_block_metadata_->address);
  }
  uint64_t GetReceiveMemoryBaseAddr() const {
    return reinterpret_cast<uint64_t>(recv_memory_block_metadata_->address);
  }
  uint64_t GetRemoteMemoryBaseAddr() const {
    return reinterpret_cast<uint64_t>(remote_attributes_->addr());
  }
  virtual struct ibv_mr *GetLocalMemoryRegion() {
    return own_local_controlled_memory_region_.get();
  }
  virtual struct ibv_mr *GetRemoteMemoryRegion() {
    return own_remote_controlled_memory_region_.get();
  }
  // We do not check if key space is empty for better performance. Accessing an
  // empty key space itself is unexpected behavior and should fail.
  uint32_t GetReceiveKey() { return recv_lkey_space_.GetMemoryKey(); }
  uint32_t GetLocalKey() { return local_controlled_lkey_space_.GetMemoryKey(); }
  uint32_t GetRemoteKey() { return remote_rkey_space_.GetMemoryKey(); }
  virtual bool IsInitiator() { return config_.is_initiator(); }
  virtual bool IsTarget() { return config_.is_target(); }

  // Call to enable tracing.
  void EnableTracing(bool should_trace) { should_trace_ = should_trace; }

  // QueuePair is moveable but not copyable
  QueuePair(QueuePair &&other) = default;
  QueuePair &operator=(QueuePair &&other) = default;

 protected:
  absl::Status ConnectToRemoteQp(
      const proto::RemoteQueuePairAttributes &remote_attributes);
  absl::Status ConnectToRemoteAh(
      const proto::RemoteQueuePairAttributes &remote_attributes);

  enum class State {
    // Ibverbs resources are uninitialized.
    kUninitialized,
    // Ibverbs resources (protection domain, queue pair, etc.) are initialized,
    // but not yet ready to send and receive.
    kInitialized,
    // Connected to the remote queue pair and ready to send and receive data.
    kConnected,
  };
  State state_;

  // When true, add the statistics for completed operations to
  // `statistics_trackers_`. When false, throw away the latency and throughput
  // measurements of operations that complete.
  bool collect_measurements_;

  // Const protos are inherently thread-safe.
  proto::QueuePairConfig config_;
  ibv_context *context_;
  const ibverbs_utils::LocalIbverbsAddress local_address_;
  int wqe_cap_;
  int max_cqe_;

  // The unique identifier for the next operation that will be posted.
  uint64_t next_wr_id_;

  // Returns the next unused work request id. If this QP was created using
  // CompletionQueueManager to assign a completion queue, the
  // CompletionQueueManager provides the wr_id. Otherwise, an internal
  // `next_wr_id_` counter is incremented to generate the id.
  uint64_t GetNextWrId();

  // If true, copy the time stamp value to the buffer using the send_time_stamp.
  bool should_send_time_stamp_;
  // If true, set data for tracing.
  bool should_trace_;

  // The total number of operations that have successfully completed (batched
  // ops are counted individually).
  uint64_t num_completed_operations_;

  // The total number of work request completion received (batched ops are
  // counted as one).
  uint64_t num_completions_received_;

  // See comment on public PostOps method for interface details. This method
  // additionally takes in an optional address handle. When provided, `ud`
  // struct in the work request is populated.
  virtual absl::Status PostOps(const OpTypeSize &op_type_size,
                               int32_t batch_size, OpSignalType signal_type,
                               std::optional<ibv_ah *> address_handle);

  // Posts a receive operation at `addr`. Receive buffers are the size of the
  // largest possible operation. For UD queue pairs, this includes space for
  // the GRH header at the beginning of the buffer. Returns the size (in
  // bytes) of the receive buffer that was posted, or an error if the buffer
  // could not be posted.
  virtual absl::StatusOr<int32_t> PostRecv(uint64_t addr);

  // Information relevant to an operation that has been posted but not
  // completed.
  // When a batch includes unsignaled WRs, we need to process them when a
  // signaled WR is completed. To be able to track unsignaled WRs in the batch,
  // we use `unsignaled_wr_id_to_process`.
  // Example) a batch has wr_id 1,2,3 and only 3 is signaled.
  // We set out internal data structure in the following way:
  //  outstanding_initiated_operations_[3].unsignaled_wr_id_to_process=2
  //  outstanding_initiated_operations_[2].unsignaled_wr_id_to_process=1
  //  outstanding_initiated_operations_[1].unsignaled_wr_id_to_process=0

  // When wr 3 is completed, we only get a completion that says wr 3 is
  // successfully completed.

  // We look up outstanding_initiated_operations_[3] and process state. Then we
  // notice there's `unsignaled_wr_id_to_process`, which is 2. So we process 2.
  // Again, it will tell us to process 1. Then we're done.
  struct OutstandingOperation {
    int64_t sent_at_nanos;
    uint64_t initiator_addr;
    std::optional<uint64_t> target_addr;
    int length_bytes;
    proto::RdmaOp op_type;
    int64_t unsignaled_wr_id_to_process;
  };

  // A map from operation identifier (wr_id) to information about that
  // operation.
  absl::flat_hash_map<uint64_t, OutstandingOperation>
      outstanding_initiated_operations_;

  struct OutstandingRecvOperation {
    std::optional<uint64_t> target_addr;
    int64_t unsignaled_outstanding_wr_id_to_process;
    int64_t unsignaled_recv_wr_id_to_process;
  };

  // A map from operation identifier (wr_id) to the address at which the
  // receive buffer begins.
  absl::flat_hash_map<uint64_t, uint64_t> outstanding_recv_operations_;

  // A map from operation type to the statistics for operations of type and size
  // that have completed.
  absl::flat_hash_map<std::pair<proto::RdmaOp, int32_t>, StatisticsTracker>
      statistics_trackers_;

  // The metadata of segments of memory allocated for RDMA operations on this
  // queue pair. All operations on the memory blocks should obtain the addresses
  // and sizes here. They must be filled whether the queue pair is initialized
  // with externally allocated resources, or allocates its own blocks for
  // compatibility.
  std::optional<MemoryBlockMetadata> recv_memory_block_metadata_ = std::nullopt;
  std::optional<MemoryBlockMetadata> local_controlled_memory_block_metadata_ =
      std::nullopt;
  std::optional<MemoryBlockMetadata> remote_controlled_memory_block_metadata_ =
      std::nullopt;

  // Keeping these for backward compatibility: memory blocks allocated by this
  // queue pair. These fields store the actual memory blocks, but operations
  // on memory blocks should not directly use these fields. Rather, addresses
  // and sizes should be obtained from the *_memory_block_metadata_ fields.
  std::optional<std::vector<uint8_t>> recv_memory_block_ = std::nullopt;
  std::optional<std::vector<uint8_t>> local_controlled_memory_block_ =
      std::nullopt;
  std::optional<std::vector<uint8_t>> remote_controlled_memory_block_ =
      std::nullopt;

  // Manages the addresses in `recv_memory_block_`.
  std::optional<ArrayMemorySpace> recv_memory_address_space_ = std::nullopt;

  // Manages the addresses in `local_controlled_memory_block_`.
  std::optional<ArrayMemorySpace> local_controlled_address_space_ =
      std::nullopt;

  // Manages the addresses on the remote, for READ and WRITE operations.
  std::optional<ArrayMemorySpace> remote_address_space_ = std::nullopt;

  // Manages the lkeys of `recv_memory_regions_`.
  ArrayMemoryKeySpace recv_lkey_space_;
  // Manages the lkeys of `local_controlled_memory_block_`.
  ArrayMemoryKeySpace local_controlled_lkey_space_;
  // Manages the rkeys on the remote, for READ and WRITE operations.
  ArrayMemoryKeySpace remote_rkey_space_;

  // Ibverbs functions are themselves thread safe and so ibverbs structs do not
  // need to be protected with a mutex.

  std::unique_ptr<ibv_pd, ibverbs_utils::ProtectionDomainDeleter>
      own_protection_domain_;
  std::unique_ptr<ibv_mr, ibverbs_utils::MemoryRegionDeleter>
      own_recv_memory_region_;
  std::unique_ptr<ibv_mr, ibverbs_utils::MemoryRegionDeleter>
      own_local_controlled_memory_region_;
  std::unique_ptr<ibv_mr, ibverbs_utils::MemoryRegionDeleter>
      own_remote_controlled_memory_region_;
  std::unique_ptr<ibv_cq, ibverbs_utils::CompletionQueueDeleter>
      completion_queue_;
  std::unique_ptr<ibv_qp, ibverbs_utils::QueuePairDeleter> queue_pair_;

  // Pointers to protection domain and memory regions. All operations on these
  // objects must use these pointers. Therefore they must be filled even if the
  // queue pair allocates these objects for backward compatibility.
  ibv_pd *protection_domain_;

  // Saves remote-controlled MRs allocated by local follower, to generate
  // attributes for access from remote
  std::vector<uint32_t> remote_controlled_memory_region_rkeys_;

  // Trackers for the latencies of chunks of code. Separate the trackers for
  // polling and posting so that they do not need to be protected with locks.
  utils::LatencyTracker post_latency_tracker_;
  utils::LatencyTracker poll_latency_tracker_;

  // Elements that will be submitted in calls to `ibv_post_send`. These are
  // kept as instance variables so that they do not need to be allocated on
  // each call to `PostOps`.
  std::vector<ibv_sge> scatter_gather_entries_;
  std::vector<ibv_send_wr> work_requests_;

  // When non-null, represents the counterpart of the connection and contains
  // information necessary to send to or receive data from the remote.
  std::optional<const proto::RemoteQueuePairAttributes> remote_attributes_;

  // Pointer to traffic generator owned CompletionQueueManager. Needed for
  // QueuePair to receive wr_ids and initialize QueuePair using its assigned
  // ibv_cq.
  CompletionQueueManager *completion_queue_manager_;

  // Info for each failed completion which originated from this QueuePair.
  std::vector<std::string> failed_completion_messages_;

  // When exists, this `address_handle_` is a connection to the remote. For UD
  // operations, queue pairs are not associated with a connection, so an
  // address handle must be provided in send work requests to indicate to
  // which peer queue pair the work request is destined.
  std::unique_ptr<ibv_ah, ibverbs_utils::AddressHandleDeleter> address_handle_;

  // If the data to send is within the threshold, inline the data.
  uint32_t inline_threshold_;

  int64_t next_wr_pos_;
  std::vector<int64_t> next_wrs_to_process_;

  // For tracing, track detailed timestamps.
  std::array<int64_t, kTracingLatencyBufferSize> before_post_, after_post_,
      before_poll_, after_poll_, timestamp_received_;
};

// Encapsulates state of a queue pair that uses Reliable Connected mode. An RC
// queue pair maps 1:1 between the local and remote.
class RcQueuePair : public QueuePair {
 public:
  using QueuePair::QueuePair;
  ~RcQueuePair() override = default;

  // See comment in QueuePair for interface details.
  absl::Status ConnectToRemote(
      const proto::RemoteQueuePairAttributes &remote_attributes) override {
    return QueuePair::ConnectToRemoteQp(remote_attributes);
  }
};

// Encapsulates state of a queue pair that uses Unreliable Datagram mode.
// Although UD queue pairs are not required to map 1:1 between hosts, this
// implementation maps 1:1 for simplicity.
class UdQueuePair : public QueuePair {
 public:
  using QueuePair::QueuePair;
  ~UdQueuePair() override = default;

  // See comment in QueuePair for interface details. Additionally, this
  // implementation creates an address handle to the remote.
  absl::Status ConnectToRemote(
      const proto::RemoteQueuePairAttributes &remote_attributes) override {
    return QueuePair::ConnectToRemoteAh(remote_attributes);
  }

  // See comment in QueuePair for interface details.
  absl::Status PostOps(const OpTypeSize &op_type_size, int32_t batch_size,
                       OpSignalType signal_type) override {
    const auto op_type = op_type_size.op_type;
    if (op_type != proto::RDMA_OP_SEND_RECEIVE &&
        op_type != proto::RDMA_OP_SEND_WITH_IMMEDIATE_RECEIVE) {
      return absl::InvalidArgumentError(
          absl::StrCat("UD queue pairs can only post send/recv operations. Op "
                       "type requested: ",
                       utils::OpTypeToString(op_type)));
    }
    return QueuePair::PostOps(op_type_size, batch_size, signal_type,
                              address_handle_.get());
  }
};

}  // namespace verbsmarks

#endif  // VERBSMARKS_QUEUE_PAIR_H_
