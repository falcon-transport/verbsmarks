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

#include "memory_manager.h"

#include <errno.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/flags/flag.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "google/protobuf/repeated_ptr_field.h"
#include "ibverbs_utils.h"
#include "infiniband/verbs.h"
#include "queue_pair.h"
#include "verbsmarks.pb.h"
#include "verbsmarks_binary_flags.h"

namespace verbsmarks {

namespace {
using MemoryRegionType = QueuePair::MemoryRegionType;
constexpr int kMemoryRegionAccessAll =
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
    IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_MW_BIND;
}  // namespace

absl::Status MemoryResources::AllocateProtectionDomain(
    ibv_context *verbs_context) {
  protection_domain_ =
      std::unique_ptr<ibv_pd, ibverbs_utils::ProtectionDomainDeleter>(
          ibv_alloc_pd(verbs_context),
          ibverbs_utils::ProtectionDomainDeleter());
  if (!protection_domain_) {
    return absl::InternalError(
        absl::StrCat("Failed to allocate follower protection domain: ",
                     std::strerror(errno)));
  }
  return absl::OkStatus();
}

absl::Status MemoryResources::CreateMemoryRegions(ibv_pd *protection_domain,
                                                  int num_memory_regions) {
  for (int i = 0; i < num_memory_regions; ++i) {
    if (recv_memory_block_size_ > 0) {
      auto recv_memory_region =
          std::unique_ptr<ibv_mr, ibverbs_utils::MemoryRegionDeleter>(
              ibv_reg_mr(protection_domain,
                         reinterpret_cast<void *>(recv_memory_address_),
                         recv_memory_block_size_, kMemoryRegionAccessAll),
              ibverbs_utils::MemoryRegionDeleter());
      if (!recv_memory_region) {
        return absl::InternalError(absl::StrCat(
            "Failed to allocate recv memory region: ", std::strerror(errno)));
      }
      recv_memory_regions_.push_back(std::move(recv_memory_region));
    }

    if (local_controlled_memory_block_size_ > 0) {
      auto local_controlled_memory_region =
          std::unique_ptr<ibv_mr, ibverbs_utils::MemoryRegionDeleter>(
              ibv_reg_mr(
                  protection_domain,
                  reinterpret_cast<void *>(local_controlled_memory_address_),
                  local_controlled_memory_block_size_, kMemoryRegionAccessAll),
              ibverbs_utils::MemoryRegionDeleter());
      if (!local_controlled_memory_region) {
        return absl::InternalError(
            absl::StrCat("Failed to allocate local controlled memory region: ",
                         std::strerror(errno)));
      }
      VLOG(2) << "Created local controlled memory region size: "
              << local_controlled_memory_block_size_;
      local_controlled_memory_regions_.push_back(
          std::move(local_controlled_memory_region));
    }

    if (remote_controlled_memory_block_size_ > 0) {
      auto remote_controlled_memory_region =
          std::unique_ptr<ibv_mr, ibverbs_utils::MemoryRegionDeleter>(
              ibv_reg_mr(
                  protection_domain,
                  reinterpret_cast<void *>(remote_controlled_memory_address_),
                  remote_controlled_memory_block_size_, kMemoryRegionAccessAll),
              ibverbs_utils::MemoryRegionDeleter());
      if (!remote_controlled_memory_region) {
        return absl::InternalError(
            absl::StrCat("Failed to allocate remote controlled memory region: ",
                         std::strerror(errno)));
      }
      VLOG(2) << "Created remote controlled memory region size: "
              << remote_controlled_memory_block_size_;
      remote_controlled_memory_regions_.push_back(
          std::move(remote_controlled_memory_region));
    }
  }
  return absl::OkStatus();
}

absl::Status MemoryManager::InitializeResources(
    ibv_context *verbs_context,
    const proto::MemoryResourcePolicy &memory_resource_policy,
    const google::protobuf::RepeatedPtrField<proto::PerFollowerTrafficPattern>
        &per_follower_traffic_patterns) {
  verbs_context_ = verbs_context;
  memory_resource_policy_ = memory_resource_policy;

  // Reject invalid configs.
  if (memory_resource_policy_.pd_allocation_policy() ==
      proto::PD_ALLOCATION_POLICY_UNKNOWN) {
    return absl::InvalidArgumentError("pd_allocation_policy must be specified");
  }
  if (memory_resource_policy_.qp_mr_mapping() == proto::QP_MR_MAPPING_UNKNOWN) {
    return absl::InvalidArgumentError("qp_mr_mapping must be specified");
  }
  if (memory_resource_policy_.qp_mr_mapping() == proto::QP_HAS_DEDICATED_MRS &&
      memory_resource_policy_.num_mrs_per_qp() <= 0) {
    return absl::InvalidArgumentError(absl::Substitute(
        "Invalid num_mrs_per_qp $0", memory_resource_policy_.num_mrs_per_qp()));
  }
  if (memory_resource_policy_.qp_mr_mapping() == proto::QP_USES_MRS_IN_PD &&
      memory_resource_policy_.num_mrs_per_pd() <= 0) {
    return absl::InvalidArgumentError(absl::Substitute(
        "Invalid num_mrs_per_pd $0", memory_resource_policy_.num_mrs_per_pd()));
  }

  int qp_memory_space_slots = absl::GetFlag(FLAGS_qp_memory_space_slots);

  // Memory manager allocates only one block of memory for each of recv,
  // remote-controlled and local-controlled, therefore needs to store where
  // each traffic pattern and queue pair's blocks begin and how large they
  // are.
  for (const auto &traffic_pattern : per_follower_traffic_patterns) {
    auto traffic_pattern_id = traffic_pattern.global_traffic_pattern_id();
    traffic_pattern_resources_[traffic_pattern_id] = {};
    auto &current_traffic_pattern_resources =
        traffic_pattern_resources_[traffic_pattern_id];
    // Record the offsets of this traffic pattern in each memory block.
    // follower_resources_ records the cumulative sizes hence can be used as
    // the starting offset of the traffic pattern's memory block.
    current_traffic_pattern_resources.CopyMemoryBlockSizesAsAddressesFrom(
        follower_resources_);
    for (const proto::QueuePairConfig &queue_pair_config :
         traffic_pattern.queue_pairs()) {
      auto queue_pair_id = queue_pair_config.queue_pair_id();
      queue_pair_resources_[traffic_pattern_id][queue_pair_id] = {};
      auto &current_queue_pair_resources =
          queue_pair_resources_[traffic_pattern_id][queue_pair_id];
      // Record the offsets of this queue pair in each type of memory block.
      // follower_resources_ records the cumulative sizes hence can be used as
      // the starting offset of the queue pair's memory block.
      current_queue_pair_resources.CopyMemoryBlockSizesAsAddressesFrom(
          follower_resources_);

      // Set the memory region types for the QueuePair based on the operation
      // types that the QueuePair is the initiator or target for.
      absl::flat_hash_set<MemoryRegionType> memory_region_types;
      if (queue_pair_config.is_initiator()) {
        memory_region_types.insert(MemoryRegionType::kLocalControlled);
      }
      if (queue_pair_config.is_target()) {
        for (const proto::RdmaOpRatio &op_ratio :
             traffic_pattern.traffic_characteristics().op_ratio()) {
          if (op_ratio.op_code() == proto::RDMA_OP_WRITE ||
              op_ratio.op_code() == proto::RDMA_OP_READ) {
            memory_region_types.insert(MemoryRegionType::kRemoteControlled);
          } else if (op_ratio.op_code() == proto::RDMA_OP_SEND_RECEIVE ||
                     op_ratio.op_code() ==
                         proto::RDMA_OP_SEND_WITH_IMMEDIATE_RECEIVE) {
            memory_region_types.insert(MemoryRegionType::kRecv);
          } else if (op_ratio.op_code() == proto::RDMA_OP_WRITE_IMMEDIATE) {
            memory_region_types.insert(MemoryRegionType::kRemoteControlled);
            // WRITE IMMEDIATE consumes receive request on the remote QP.
            memory_region_types.insert(MemoryRegionType::kRecv);
          }
        }
      }

      if (memory_region_types.contains(MemoryRegionType::kRecv)) {
        std::size_t recv_memory_block_size =
            qp_memory_space_slots * queue_pair_config.max_op_size();
        if (queue_pair_config.connection_type() == proto::CONNECTION_TYPE_UD) {
          // UD receive buffers must allocate additional space for the Falcon
          // header at the beginning of the receive buffer.
          recv_memory_block_size += qp_memory_space_slots * sizeof(ibv_grh);
        }
        current_queue_pair_resources.SetRecvMemoryBlockSize(
            recv_memory_block_size);
      }
      if (memory_region_types.contains(MemoryRegionType::kLocalControlled)) {
        current_queue_pair_resources.SetLocalControlledMemoryBlockSize(
            qp_memory_space_slots * queue_pair_config.max_op_size());
      }
      if (memory_region_types.contains(MemoryRegionType::kRemoteControlled)) {
        current_queue_pair_resources.SetRemoteControlledMemoryBlockSize(
            qp_memory_space_slots * queue_pair_config.max_op_size());
      }

      // Update follower- and traffic pattern-level memory block sizes to
      // reflect the cumulative block sizes in this follower/traffic pattern.
      follower_resources_.IncrementMemoryBlockSizesFrom(
          current_queue_pair_resources);
      current_traffic_pattern_resources.IncrementMemoryBlockSizesFrom(
          current_queue_pair_resources);
    }
  }

  // Allocate memory block
  if (auto recv_memory_block_size =
          follower_resources_.GetRecvMemoryBlockSize();
      recv_memory_block_size > 0) {
    VLOG(2) << "Total recv memory block size: " << recv_memory_block_size;
    recv_memory_block_.emplace(recv_memory_block_size);
    std::generate_n(recv_memory_block_->data(), recv_memory_block_->size(),
                    std::ref(random));
  }
  if (auto local_controlled_memory_block_size =
          follower_resources_.GetLocalControlledMemoryBlockSize();
      local_controlled_memory_block_size > 0) {
    VLOG(2) << "Total local controlled memory block size: "
            << local_controlled_memory_block_size;
    local_controlled_memory_block_.emplace(local_controlled_memory_block_size);
    std::generate_n(local_controlled_memory_block_->data(),
                    local_controlled_memory_block_->size(), std::ref(random));
  }
  if (auto remote_controlled_memory_block_size =
          follower_resources_.GetRemoteControlledMemoryBlockSize();
      remote_controlled_memory_block_size > 0) {
    VLOG(2) << "Total remote controlled memory block size: "
            << remote_controlled_memory_block_size;
    remote_controlled_memory_block_.emplace(
        remote_controlled_memory_block_size);
    std::generate_n(remote_controlled_memory_block_->data(),
                    remote_controlled_memory_block_->size(), std::ref(random));
  }

  // Add the allocated follower-level memory block addresses to the recorded
  // per-traffic pattern and per-queue pair offsets.
  if (recv_memory_block_.has_value()) {
    follower_resources_.SetRecvMemoryAddress(
        reinterpret_cast<std::uintptr_t>(recv_memory_block_->data()));
  }
  if (local_controlled_memory_block_.has_value()) {
    follower_resources_.SetLocalControlledMemoryAddress(
        reinterpret_cast<std::uintptr_t>(
            local_controlled_memory_block_->data()));
  }
  if (remote_controlled_memory_block_.has_value()) {
    follower_resources_.SetRemoteControlledMemoryAddress(
        reinterpret_cast<std::uintptr_t>(
            remote_controlled_memory_block_->data()));
  }
  for (auto &traffic_pattern_id_and_memory_resources :
       traffic_pattern_resources_) {
    traffic_pattern_id_and_memory_resources.second.IncrementMemoryAddressesFrom(
        follower_resources_);
  }
  for (auto &traffic_pattern_id_to_queue_pair_resources :
       queue_pair_resources_) {
    for (auto &queue_pair_id_and_memory_resources :
         traffic_pattern_id_to_queue_pair_resources.second) {
      queue_pair_id_and_memory_resources.second.IncrementMemoryAddressesFrom(
          follower_resources_);
    }
  }

  absl::Status status;
  // Allocate protection domains.
  switch (memory_resource_policy_.pd_allocation_policy()) {
    case proto::PD_PER_FOLLOWER: {
      // Allocate follower-level protection domain.
      VLOG(2) << "Allocating protection domain: follower-level";
      status = follower_resources_.AllocateProtectionDomain(verbs_context_);
      if (!status.ok()) {
        return status;
      }
    } break;
    case proto::PD_PER_TRAFFIC_PATTERN: {
      // Allocate traffic pattern-level protection domain.
      for (auto &traffic_pattern_id_and_memory_resources :
           traffic_pattern_resources_) {
        VLOG(2) << "Allocating protection domain: traffic pattern "
                << traffic_pattern_id_and_memory_resources.first;
        status = traffic_pattern_id_and_memory_resources.second
                     .AllocateProtectionDomain(verbs_context_);
        if (!status.ok()) {
          return status;
        }
      }
    } break;
    case proto::PD_PER_QP: {
      // Allocate queue pair-level protection domain.
      for (auto &traffic_pattern_id_to_queue_pair_resources :
           queue_pair_resources_) {
        for (auto &queue_pair_id_and_memory_resources :
             traffic_pattern_id_to_queue_pair_resources.second) {
          VLOG(2) << "Allocating protection domain: traffic pattern "
                  << traffic_pattern_id_to_queue_pair_resources.first
                  << ", queue pair "
                  << queue_pair_id_and_memory_resources.first;
          status = queue_pair_id_and_memory_resources.second
                       .AllocateProtectionDomain(verbs_context_);
          if (!status.ok()) {
            return status;
          }
        }
      }
    } break;
    default:
      return absl::InvalidArgumentError(
          absl::Substitute("Unsupported pd_allocation_policy $0!",
                           memory_resource_policy_.pd_allocation_policy()));
  }

  // Allocate memory regions
  if (memory_resource_policy_.qp_mr_mapping() == proto::QP_USES_MRS_IN_PD) {
    switch (memory_resource_policy_.pd_allocation_policy()) {
      case proto::PD_PER_FOLLOWER:
        VLOG(2) << "Registering follower-level memory regions";
        status = follower_resources_.CreateMemoryRegions(
            follower_resources_.GetProtectionDomain(),
            memory_resource_policy_.num_mrs_per_pd());
        return status;
      case proto::PD_PER_TRAFFIC_PATTERN:
        for (auto &traffic_pattern_id_and_memory_resources :
             traffic_pattern_resources_) {
          VLOG(2) << "Registering memory regions: traffic pattern "
                  << traffic_pattern_id_and_memory_resources.first;
          status = traffic_pattern_id_and_memory_resources.second
                       .CreateMemoryRegions(
                           traffic_pattern_id_and_memory_resources.second
                               .GetProtectionDomain(),
                           memory_resource_policy_.num_mrs_per_pd());
          if (!status.ok()) {
            return status;
          }
        }
        return absl::OkStatus();
      case proto::PD_PER_QP:
        for (auto &traffic_pattern_id_to_queue_pair_resources :
             queue_pair_resources_) {
          for (auto &queue_pair_id_and_memory_resources :
               traffic_pattern_id_to_queue_pair_resources.second) {
            DLOG(INFO) << "Registering memory regions: traffic pattern "
                       << traffic_pattern_id_to_queue_pair_resources.first
                       << ", queue pair "
                       << queue_pair_id_and_memory_resources.first;
            status =
                queue_pair_id_and_memory_resources.second.CreateMemoryRegions(
                    queue_pair_id_and_memory_resources.second
                        .GetProtectionDomain(),
                    memory_resource_policy_.num_mrs_per_pd());
            if (!status.ok()) {
              return status;
            }
          }
        }
        return absl::OkStatus();
      default:
        return absl::InvalidArgumentError(
            absl::Substitute("Unsupported pd_allocation_policy $0!",
                             memory_resource_policy_.pd_allocation_policy()));
    }
  }

  if (memory_resource_policy_.qp_mr_mapping() == proto::QP_HAS_DEDICATED_MRS) {
    auto protection_domain = follower_resources_.GetProtectionDomain();
    for (auto &traffic_pattern_id_to_queue_pair_resources :
         queue_pair_resources_) {
      auto traffic_pattern_id =
          traffic_pattern_id_to_queue_pair_resources.first;
      if (memory_resource_policy_.pd_allocation_policy() ==
          proto::PD_PER_TRAFFIC_PATTERN) {
        protection_domain = traffic_pattern_resources_[traffic_pattern_id]
                                .GetProtectionDomain();
      }
      for (auto &queue_pair_id_and_memory_resources :
           traffic_pattern_id_to_queue_pair_resources.second) {
        if (memory_resource_policy_.pd_allocation_policy() ==
            proto::PD_PER_QP) {
          protection_domain =
              queue_pair_id_and_memory_resources.second.GetProtectionDomain();
        }
        DLOG(INFO) << "Registering memory regions: traffic pattern "
                   << traffic_pattern_id_to_queue_pair_resources.first
                   << ", queue pair "
                   << queue_pair_id_and_memory_resources.first;
        status = queue_pair_id_and_memory_resources.second.CreateMemoryRegions(
            protection_domain, memory_resource_policy_.num_mrs_per_qp());
        if (!status.ok()) {
          return status;
        }
      }
    }
    return absl::OkStatus();
  }

  return absl::InvalidArgumentError(
      absl::Substitute("Unsupported qp_mr_mapping $0!",
                       memory_resource_policy_.qp_mr_mapping()));
}

absl::StatusOr<QueuePairMemoryResourcesView>
MemoryManager::GetQueuePairMemoryResources(int32_t traffic_pattern_id,
                                           int32_t queue_pair_id) const {
  // Look up for the MemoryResources object that contains the protection domain.
  const MemoryResources *memory_resources_with_protection_domain = nullptr;
  switch (memory_resource_policy_.pd_allocation_policy()) {
    case proto::PD_PER_FOLLOWER:
      memory_resources_with_protection_domain = &follower_resources_;
      break;
    case proto::PD_PER_TRAFFIC_PATTERN: {
      if (!traffic_pattern_resources_.contains(traffic_pattern_id)) {
        return absl::InternalError(absl::Substitute(
            "Memory resources for traffic pattern $0 not found.",
            traffic_pattern_id));
      }
      memory_resources_with_protection_domain =
          &traffic_pattern_resources_.at(traffic_pattern_id);
    } break;
    case proto::PD_PER_QP: {
      if (!queue_pair_resources_.contains(traffic_pattern_id) ||
          !queue_pair_resources_.at(traffic_pattern_id)
               .contains(queue_pair_id)) {
        return absl::InternalError(absl::Substitute(
            "Memory resources for traffic pattern $0 queue pair $1 not found.",
            traffic_pattern_id, queue_pair_id));
      }
      memory_resources_with_protection_domain =
          &queue_pair_resources_.at(traffic_pattern_id).at(queue_pair_id);
    } break;
    default:
      return absl::InvalidArgumentError(
          absl::Substitute("Unsupported pd_allocation_policy $0!",
                           memory_resource_policy_.pd_allocation_policy()));
  }

  auto protection_domain =
      memory_resources_with_protection_domain->GetProtectionDomain();
  if (protection_domain == nullptr) {
    return absl::InvalidArgumentError(
        absl::Substitute("Did not find correct protection domain for  $0!",
                         memory_resource_policy_.pd_allocation_policy()));
  }

  // MemoryResources object of the queried queue pair itself.
  const MemoryResources *queue_pair_memory_resources = nullptr;
  if (!queue_pair_resources_.contains(traffic_pattern_id) ||
      !queue_pair_resources_.at(traffic_pattern_id).contains(queue_pair_id)) {
    return absl::InternalError(absl::Substitute(
        "Memory resources for traffic pattern $0 queue pair $1 not found.",
        traffic_pattern_id, queue_pair_id));
  }
  queue_pair_memory_resources =
      &queue_pair_resources_.at(traffic_pattern_id).at(queue_pair_id);
  if (!queue_pair_memory_resources) {
    return absl::InternalError(
        absl::Substitute("Failed to find memory blocks and memory regions for "
                         "traffic pattern $0 queue pair $1.",
                         traffic_pattern_id, queue_pair_id));
  }

  const MemoryResources *memory_resources_with_memory_regions =
      memory_resource_policy_.qp_mr_mapping() == proto::QP_USES_MRS_IN_PD
          ? memory_resources_with_protection_domain
          : queue_pair_memory_resources;

  // Generate the memory resource view for the queue pair.
  QueuePairMemoryResourcesView queue_pair_resources_view = {
      .protection_domain = protection_domain,
  };

  if (auto recv_memory_block_size =
          queue_pair_memory_resources->GetRecvMemoryBlockSize();
      recv_memory_block_size > 0) {
    queue_pair_resources_view.recv_memory_block_metadata.emplace(
        queue_pair_memory_resources->GetRecvMemoryAddress(),
        recv_memory_block_size);
    queue_pair_resources_view.recv_memory_regions.emplace();
    for (auto &recv_memory_region :
         memory_resources_with_memory_regions->GetRecvMemoryRegions()) {
      queue_pair_resources_view.recv_memory_regions->push_back(
          recv_memory_region.get());
    }
  }
  if (auto local_controlled_memory_block_size =
          queue_pair_memory_resources->GetLocalControlledMemoryBlockSize();
      local_controlled_memory_block_size > 0) {
    queue_pair_resources_view.local_controlled_memory_block_metadata.emplace(
        queue_pair_memory_resources->GetLocalControlledMemoryAddress(),
        local_controlled_memory_block_size);
    queue_pair_resources_view.local_controlled_memory_regions.emplace();
    for (auto &recv_memory_region : memory_resources_with_memory_regions
                                        ->GetLocalControlledMemoryRegions()) {
      queue_pair_resources_view.local_controlled_memory_regions->push_back(
          recv_memory_region.get());
    }
  }
  if (auto remote_controlled_memory_block_size =
          queue_pair_memory_resources->GetRemoteControlledMemoryBlockSize();
      remote_controlled_memory_block_size > 0) {
    queue_pair_resources_view.remote_controlled_memory_block_metadata.emplace(
        queue_pair_memory_resources->GetRemoteControlledMemoryAddress(),
        remote_controlled_memory_block_size);
    queue_pair_resources_view.remote_controlled_memory_regions.emplace();
    for (auto &recv_memory_region : memory_resources_with_memory_regions
                                        ->GetRemoteControlledMemoryRegions()) {
      queue_pair_resources_view.remote_controlled_memory_regions->push_back(
          recv_memory_region.get());
    }
  }
  return queue_pair_resources_view;
}

}  // namespace verbsmarks
