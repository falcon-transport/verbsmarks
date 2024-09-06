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

// A collection of functions that encompass common patterns for interacting with
// the ibverbs API.
#ifndef VERBSMARKS_IBVERBS_UTILS_H_
#define VERBSMARKS_IBVERBS_UTILS_H_

#include <netinet/in.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <vector>

#include "absl/flags/declare.h"
#include "absl/flags/flag.h"
#include "absl/log/log.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "infiniband/verbs.h"

ABSL_DECLARE_FLAG(int, ibverb_gid_override);

namespace verbsmarks {
namespace ibverbs_utils {

// These structs are custom deleters for ibverbs pointers so that they can be
// used with std::unique_ptr. They destroy the resource, logging the error if it
// fails. If the resource is null, they do nothing.
struct ContextDeleter {
  void operator()(ibv_context *context) {
    if (context != nullptr) {
      DLOG(INFO) << "ibv_close_device ";
      if (int ret = ibv_close_device(context); ret != 0) {
        LOG(FATAL) << "Fail to close device " << context->device->name
                   << " @error: " << ret;  // Crash ok
      }
    }
  }
};
struct QueuePairDeleter {
  void operator()(ibv_qp *qp) {
    if (qp != nullptr) {
      LOG_EVERY_N(INFO, 100000) << "ibv_destroy_qp " << qp->qp_num;
      DLOG(INFO) << "ibv_destroy_qp " << qp->qp_num;
      if (int ret = ibv_destroy_qp(qp); ret != 0) {
        LOG(FATAL) << "Fail to destroy QP " << qp->qp_num
                   << " @error: " << ret;  // Crash ok
      }
    }
  }
};
struct CompletionQueueDeleter {
  void operator()(ibv_cq *cq) {
    if (cq != nullptr) {
      DLOG(INFO) << "ibv_destroy_cp " << cq;
      if (int ret = ibv_destroy_cq(cq); ret != 0) {
        LOG(FATAL) << "Fail to destroy CQ @error: " << ret;  // Crash ok
      }
    }
  }
};
struct MemoryRegionDeleter {
  void operator()(ibv_mr *mr) {
    if (mr != nullptr) {
      DLOG(INFO) << "ibv_dreg_mr ";
      if (int ret = ibv_dereg_mr(mr); ret != 0) {
        LOG(FATAL) << "Fail to deregister memory region @error: "
                   << ret;  // Crash ok
      }
    }
  }
};
struct ProtectionDomainDeleter {
  void operator()(ibv_pd *pd) {
    if (pd != nullptr) {
      DLOG(INFO) << "ibv_dealloc_pd";
      if (int ret = ibv_dealloc_pd(pd); ret != 0) {
        LOG(FATAL) << "Fail to dealloc protection domain @error: "
                   << ret;  // Crash ok
      }
    }
  }
};
struct AddressHandleDeleter {
  void operator()(ibv_ah *ah) {
    if (ah != nullptr) {
      DLOG(INFO) << "ibv_destroy_ah";
      if (int ret = ibv_destroy_ah(ah); ret != 0) {
        LOG(FATAL) << "Fail to destroy address handle @error: "
                   << ret;  // Crash ok
      }
    }
  }
};
struct CompChannelDeleter {
  void operator()(ibv_comp_channel *ch) {
    if (ch != nullptr) {
      DLOG(INFO) << "ibv_destroy_comp_channel " << ch;
      if (int ret = ibv_destroy_comp_channel(ch); ret != 0) {
        LOG(FATAL) << "Fail to destroy comp channel @error: "
                   << ret;  // Crash ok
      }
    }
  }
};

// Returns true if there are one or more ibverbs devices available on this
// machine. Returns false otherwise.
bool HasDevice();

// Returns a list of available ibverbs device names, regardless if their ports
// are up or down. If no RDMAable devices are found, it will return a "Not Found
// Error".
absl::StatusOr<std::vector<std::string>> ListRdmaDeviceNames();

// Opens and returns the ibverbs device associated with the provided name, if
// one is available. If no name is provided, the first available ibverbs
// device is returned, if there is one.
absl::StatusOr<std::unique_ptr<ibv_context, ContextDeleter>> OpenDevice(
    absl::string_view device_name = "");

// Opens and returns the ibverbs device associated with the provided name, or
// the first available ibverbs device given the empty string. Crashes if the
// device cannot be opened or if no device is available.
std::unique_ptr<ibv_context, ContextDeleter> OpenDeviceOrDie(
    absl::string_view device_name = "");

// Identifying information of source address from which ibverbs traffic is sent.
struct LocalIbverbsAddress {
  uint8_t port;      // Port out of which traffic should be sent.
  enum ibv_mtu mtu;  // The maximum MTU of RC/UD ops on this port.
  ibv_gid gid;  // Global address for sending packets between different subnets.
  uint8_t gid_index;    // Index in the GID table corresponding to `gid`.
  std::string ip_addr;  // The string format of the IP used.
};

// Returns the first valid address of the provided context, if there is one.
// Prefers IPv6 addresses to IPv4 addresses.
absl::StatusOr<LocalIbverbsAddress> GetLocalAddress(ibv_context *context);

// Returns the max_qp_wr from ibv_query_device, which is the maximum number of
// outstanding WR on any work queue.
absl::StatusOr<int> GetMaxQpWr(ibv_context *context);

// Wait the async event in a blocking way and ack it. The function should run in
// a separate thread for handling AEs.
void WaitAndAckAsyncEvents(absl::string_view device_name);

// Returns the first valid address of the provided context. Crashes if no valid
// address is available.
LocalIbverbsAddress GetLocalAddressOrDie(ibv_context *context);

// Converts an `in6_addr` into a string. If V4 mapped, returns an IPv4 string.
std::string IpToString(in6_addr *addr6);

}  // namespace ibverbs_utils
}  // namespace verbsmarks

#endif  // VERBSMARKS_IBVERBS_UTILS_H_
