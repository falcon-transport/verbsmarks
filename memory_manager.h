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

#ifndef VERBSMARKS_MEMORY_MANAGER_H_
#define VERBSMARKS_MEMORY_MANAGER_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/optional.h"
#include "google/protobuf/repeated_ptr_field.h"
#include "ibverbs_utils.h"
#include "infiniband/verbs.h"
#include "queue_pair.h"
#include "verbsmarks.pb.h"

namespace verbsmarks {

// This is a auxiliary class mainly to be used by MemoryManager to conveniently
// organize the memory resources (i.e., protection domain, memory regions and
// blocks) that belong to a follower, traffic pattern or queue pair. It stores
// the ownership of the PD and MRs, as well as memory block addresses and sizes.

// All resources may or may not exist in the same instance. E.g., when the
// protection domain is to be allocated follower-wide, no traffic pattern or
// queue pair can own a protection domain.

// This class also provides a series of calls regarding memory block sizes and
// address for MemoryManager, which needs to calculate the total memory block
// sizes (to support traffic pattern- or follower-level memory regions) by
// iterating through queue pairs and to set the offset for each queue pair
// within the allocated block.

// This class is thread-unsafe, because it is only mutated when MemoryManager
// initializes resources, is happens during experiment initialization where
// multithreading is not used.
class MemoryResources {
 public:
  absl::Status AllocateProtectionDomain(ibv_context *verbs_context);

  ibv_pd *GetProtectionDomain() const { return protection_domain_.get(); }
  std::uintptr_t GetRecvMemoryAddress() const { return recv_memory_address_; }
  std::uintptr_t GetLocalControlledMemoryAddress() const {
    return local_controlled_memory_address_;
  }
  std::uintptr_t GetRemoteControlledMemoryAddress() const {
    return remote_controlled_memory_address_;
  }
  std::size_t GetRecvMemoryBlockSize() const { return recv_memory_block_size_; }
  std::size_t GetLocalControlledMemoryBlockSize() const {
    return local_controlled_memory_block_size_;
  }
  std::size_t GetRemoteControlledMemoryBlockSize() const {
    return remote_controlled_memory_block_size_;
  }
  const std::vector<
      std::unique_ptr<ibv_mr, ibverbs_utils::MemoryRegionDeleter>> &
  GetRecvMemoryRegions() const {
    return recv_memory_regions_;
  }
  const std::vector<
      std::unique_ptr<ibv_mr, ibverbs_utils::MemoryRegionDeleter>> &
  GetLocalControlledMemoryRegions() const {
    return local_controlled_memory_regions_;
  }
  const std::vector<
      std::unique_ptr<ibv_mr, ibverbs_utils::MemoryRegionDeleter>> &
  GetRemoteControlledMemoryRegions() const {
    return remote_controlled_memory_regions_;
  }

  void SetRecvMemoryAddress(std::uintptr_t address) {
    recv_memory_address_ = address;
  }
  void SetLocalControlledMemoryAddress(std::uintptr_t address) {
    local_controlled_memory_address_ = address;
  }
  void SetRemoteControlledMemoryAddress(std::uintptr_t address) {
    remote_controlled_memory_address_ = address;
  }
  void SetRecvMemoryBlockSize(std::size_t size) {
    recv_memory_block_size_ = size;
  }
  void SetLocalControlledMemoryBlockSize(std::size_t size) {
    local_controlled_memory_block_size_ = size;
  }
  void SetRemoteControlledMemoryBlockSize(std::size_t size) {
    remote_controlled_memory_block_size_ = size;
  }

  // Copies the *_size fields in another instance to the corresponding *_address
  // in this one.
  void CopyMemoryBlockSizesAsAddressesFrom(
      const MemoryResources &memory_resources) {
    recv_memory_address_ =
        static_cast<std::uintptr_t>(memory_resources.recv_memory_block_size_);
    local_controlled_memory_address_ = static_cast<std::uintptr_t>(
        memory_resources.local_controlled_memory_block_size_);
    remote_controlled_memory_address_ = static_cast<std::uintptr_t>(
        memory_resources.remote_controlled_memory_block_size_);
  }

  // Increments *_size fields by the corresponding *_size fields from another
  // instance.
  void IncrementMemoryBlockSizesFrom(const MemoryResources &memory_resources) {
    recv_memory_block_size_ += memory_resources.recv_memory_block_size_;
    local_controlled_memory_block_size_ +=
        memory_resources.local_controlled_memory_block_size_;
    remote_controlled_memory_block_size_ +=
        memory_resources.remote_controlled_memory_block_size_;
  }

  // Increments *_address fields by the corresponding *_address fields from
  // another instance.
  void IncrementMemoryAddressesFrom(const MemoryResources &memory_resources) {
    recv_memory_address_ += memory_resources.recv_memory_address_;
    local_controlled_memory_address_ +=
        memory_resources.local_controlled_memory_address_;
    remote_controlled_memory_address_ +=
        memory_resources.remote_controlled_memory_address_;
  }

  // Create memory regions according to sizes and addresses in this struct.
  // The protection domain passed in the argument is used instead of the one in
  // this instance , because PD might not be allocated at this level.
  absl::Status CreateMemoryRegions(ibv_pd *protection_domain,
                                   int num_memory_regions);

 private:
  std::unique_ptr<ibv_pd, ibverbs_utils::ProtectionDomainDeleter>
      protection_domain_;
  std::vector<std::unique_ptr<ibv_mr, ibverbs_utils::MemoryRegionDeleter>>
      recv_memory_regions_;
  std::vector<std::unique_ptr<ibv_mr, ibverbs_utils::MemoryRegionDeleter>>
      local_controlled_memory_regions_;
  std::vector<std::unique_ptr<ibv_mr, ibverbs_utils::MemoryRegionDeleter>>
      remote_controlled_memory_regions_;
  std::uintptr_t recv_memory_address_;
  std::uintptr_t local_controlled_memory_address_;
  std::uintptr_t remote_controlled_memory_address_;
  std::size_t recv_memory_block_size_;
  std::size_t local_controlled_memory_block_size_;
  std::size_t remote_controlled_memory_block_size_;
};

// Manages all the protection domains, memory regions and memory blocks within
// a follower and maintains the mappings between these objects and the follower,
// traffic patterns and queue pairs. Only one instance should be created within
// a follower, and initialization and queries should happen before an experiment
// begins.

// This class is thread-unsafe, because resource initialization should only
// happen during experiment initialization where multithreading is not used.
class MemoryManager {
 public:
  MemoryManager() = default;
  MemoryManager(MemoryManager &&) = default;
  virtual ~MemoryManager() = default;

  // Initializes all the managed resources. This function should be called only
  // once for a follower.
  virtual absl::Status InitializeResources(
      ibv_context *verbs_context,
      const proto::MemoryResourcePolicy &memory_resource_policy,
      const google::protobuf::RepeatedPtrField<proto::PerFollowerTrafficPattern>
          &per_follower_traffic_patterns);

  // Find out protection domain, memory regions and memory blocks corresponding
  // to a queue pair.
  virtual absl::StatusOr<QueuePairMemoryResourcesView>
  GetQueuePairMemoryResources(int32_t traffic_pattern_id,
                              int32_t queue_pair_id) const;

  // Gets per-queue pair memory resources struct.
  // Returns nullptr if it is not found.
  const MemoryResources *GetMemoryResources(int32_t traffic_pattern_id,
                                            int32_t queue_pair_id) const {
    if (!queue_pair_resources_.contains(traffic_pattern_id) ||
        !queue_pair_resources_.at(traffic_pattern_id).contains(queue_pair_id)) {
      return nullptr;
    }
    return &queue_pair_resources_.at(traffic_pattern_id).at(queue_pair_id);
  }

  // Gets per-traffic pattern memory resources struct.
  // Returns nullptr if it is not found.
  const MemoryResources *GetMemoryResources(int32_t traffic_pattern_id) const {
    if (!traffic_pattern_resources_.contains(traffic_pattern_id)) {
      return nullptr;
    }
    return &traffic_pattern_resources_.at(traffic_pattern_id);
  }

  // Gets per-follower memory resources struct. Always returns a valid pointer.
  const MemoryResources *GetMemoryResources() const {
    return &follower_resources_;
  }

  const std::vector<uint8_t> *GetRecvMemoryBlock() const {
    if (!recv_memory_block_.has_value()) {
      return nullptr;
    }
    return &recv_memory_block_.value();
  }

  const std::vector<uint8_t> *GetLocalControlledMemoryBlock() const {
    if (!local_controlled_memory_block_.has_value()) {
      return nullptr;
    }
    return &local_controlled_memory_block_.value();
  }

  const std::vector<uint8_t> *GetRemoteControlledMemoryBlock() const {
    if (!remote_controlled_memory_block_.has_value()) {
      return nullptr;
    }
    return &remote_controlled_memory_block_.value();
  }

 private:
  ibv_context *verbs_context_;
  proto::MemoryResourcePolicy memory_resource_policy_;

  // Below are follower-, traffic pattern- and queue pair-level resources. All
  // MemoryResources objects are allocated when initializing resources, while
  // not all MemoryResources objects bear all types of resources, depending on
  // the memory resource policy.
  MemoryResources follower_resources_ = {};
  // Mapping: global traffic pattern ID --> traffic pattern-level resources.
  absl::flat_hash_map<int32_t, MemoryResources> traffic_pattern_resources_;
  // Mappings: global traffic pattern ID --> (queue pair ID --> queue pair-level
  // resources).
  absl::flat_hash_map<int32_t, absl::flat_hash_map<int32_t, MemoryResources>>
      queue_pair_resources_;

  std::optional<std::vector<uint8_t>> recv_memory_block_ = std::nullopt;
  std::optional<std::vector<uint8_t>> local_controlled_memory_block_ =
      std::nullopt;
  std::optional<std::vector<uint8_t>> remote_controlled_memory_block_ =
      std::nullopt;
};

}  // namespace verbsmarks

#endif  // VERBSMARKS_MEMORY_MANAGER_H_
