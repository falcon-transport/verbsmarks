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

#include "ibverbs_utils.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <stdint.h>
#include <string.h>

#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/cleanup/cleanup.h"
#include "absl/flags/flag.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "infiniband/verbs.h"

const int kGidOverrideInvalid = -1;

ABSL_FLAG(int, ibverb_mtu_override, 4096,
          "If not 0 and a valid value, overrides mtu. Valid values are:256, "
          "512, 1024, 2048, and 4096.");
ABSL_FLAG(bool, validate_device_info, true,
          "If true, validate if the device has open ports and non-empty gids "
          "during device open.");
ABSL_FLAG(int, ibverb_gid_override, kGidOverrideInvalid,
          "If set to a valid value, force use this GID.");

namespace verbsmarks {
namespace ibverbs_utils {

bool HasDevice() {
  ibv_device** devices = nullptr;
  absl::Cleanup free_list = [&devices]() {
    if (devices) {
      DLOG(INFO) << "ibv_free_device_list";
      ibv_free_device_list(devices);
    }
  };

  int num_devices = 0;
  DLOG(INFO) << "ibv_get_device_list";
  devices = ibv_get_device_list(&num_devices);
  return num_devices > 0;
}

absl::StatusOr<std::vector<std::string>> ListRdmaDeviceNames() {
  ibv_device** devices = nullptr;
  absl::Cleanup free_list = [&devices]() {
    if (devices) {
      DLOG(INFO) << "ibv_free_device_list";
      ibv_free_device_list(devices);
    }
  };

  int num_devices = 0;
  DLOG(INFO) << "ibv_get_device_list";
  devices = ibv_get_device_list(&num_devices);
  if (num_devices == 0) {
    return absl::NotFoundError(
        "No devices found. Please check if RDMA driver is loaded correctly.");
  }
  std::vector<std::string> device_names;
  device_names.reserve(num_devices);
  for (int i = 0; i < num_devices; ++i) {
    device_names.push_back(std::string(devices[i]->name));
  }
  return device_names;
}

std::string PortStatusValue(enum ibv_port_state state) {
  switch (state) {
    case IBV_PORT_NOP:
      return "IBV_PORT_NOP";
    case IBV_PORT_DOWN:
      return "IBV_PORT_DOWN";
    case IBV_PORT_INIT:
      return "IBV_PORT_INIT";
    case IBV_PORT_ARMED:
      return "IBV_PORT_ARMED";
    case IBV_PORT_ACTIVE:
      return "IBV_PORT_ACTIVE";
    case IBV_PORT_ACTIVE_DEFER:
      return "IBV_PORT_ACTIVE_DEFER";
    default:
      return "Unknown";
  }
}

absl::Status CheckIfDeviceLooksGood(ibv_context* context) {
  if (context == nullptr) {
    return absl::InvalidArgumentError("No device to print information.");
  }

  ibv_device_attr dev_attr;
  if (int query_result = ibv_query_device(context, &dev_attr);
      query_result != 0) {
    return absl::InternalError(absl::StrCat(
        "ibv_query_device failed with return value: ", query_result));
  }

  // Find the first valid port. libibverbs port numbers start at 1.
  std::optional<LocalIbverbsAddress> ipv4_address;
  std::optional<LocalIbverbsAddress> invalid_address;
  bool active_port_found = false;
  bool gid_found = false;
  for (uint8_t port_idx = 1; port_idx <= dev_attr.phys_port_cnt; ++port_idx) {
    ibv_port_attr port_attr;
    if (int query_result = ibv_query_port(context, port_idx, &port_attr);
        query_result != 0) {
      return absl::InternalError(absl::StrCat(
          "ibv_query_port failed with return value: ", query_result));
    }
    LOG(INFO) << "Port " << port_idx << ": "
              << PortStatusValue(port_attr.state);
    if (port_attr.state != IBV_PORT_ACTIVE) {
      continue;
    }
    active_port_found = true;
    LOG(INFO) << "\tmax MTU: " << int(128 << port_attr.max_mtu);
    LOG(INFO) << "\tactive MTU: " << int(128 << port_attr.active_mtu);

    ibv_gid gid;
    // gid_tbl_len can be bigger than UINT8_MAX as it's integer. To prevent
    // infinite loop, cap it at UINT8_MAX.
    uint8_t max_gid_index =
        (port_attr.gid_tbl_len > UINT8_MAX) ? UINT8_MAX : port_attr.gid_tbl_len;
    for (uint8_t gid_index = 0; gid_index < max_gid_index; ++gid_index) {
      if (int query_result = ibv_query_gid(context, port_idx, gid_index, &gid);
          query_result != 0) {
        return absl::InternalError(absl::StrCat(
            "ibv_query_gid failed with return value: ", query_result));
      }
      in6_addr addr;
      const int8_t kEmptyGid[16] = {0};
      if (memcmp(&gid.raw, kEmptyGid, sizeof(kEmptyGid)) == 0) {
        // Skip empty GIDs.
        continue;
      }
      gid_found = true;
      memcpy(&addr, gid.raw, sizeof(addr));
      LOG(INFO) << "GID " << int(gid_index) << ": " << IpToString(&addr);
    }
  }
  if (!active_port_found) {
    return absl::NotFoundError("No active port found.");
  }
  if (!gid_found) {
    return absl::NotFoundError("No non-empty GID found.");
  }
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<ibv_context, ContextDeleter>> OpenDevice(
    const absl::string_view device_name) {
  ibv_device** devices = nullptr;
  absl::Cleanup free_list = [&devices]() {
    if (devices) {
      DLOG(INFO) << "ibv_free_device_list";
      ibv_free_device_list(devices);
    }
  };

  int num_devices = 0;
  DLOG(INFO) << "ibv_get_device_list";
  devices = ibv_get_device_list(&num_devices);
  if (num_devices == 0) {
    return absl::InternalError(
        "No devices found. Check if RDMA driver is loaded correctly.");
  }

  ibv_device* device = nullptr;
  if (device_name.empty()) {
    // If the caller did not provide a device name, use the first device.
    device = devices[0];
  } else {
    // Find the device that corresponds to the name provided by the caller.
    for (int i = 0; i < num_devices; ++i) {
      if (devices[i]->name == device_name) {
        device = devices[i];
        break;
      }
    }
    if (!device) {
      return absl::InternalError(
          absl::StrCat("Device with name ", device_name, " not found."));
    }
  }

  LOG(INFO) << "Opening " << device->name;
  auto context = std::unique_ptr<ibv_context, ContextDeleter>(
      ibv_open_device(device), ContextDeleter());
  if (context == nullptr) {
    return absl::InternalError(
        absl::StrCat("Failed to open a context for ", device->name,
                     ". Check if the driver is correct."));
  }
  if (absl::GetFlag(FLAGS_validate_device_info)) {
    auto status = CheckIfDeviceLooksGood(context.get());
    if (!status.ok()) {
      return status;
    }
  }

  return context;
}

void WaitAndAckAsyncEvents(absl::string_view device_name) {
  VLOG(2) << "Start the AE handle thread.";
  ibv_async_event event{};
  int ret;
  auto verbs_context = OpenDeviceOrDie(device_name);
  while (true) {
    // Wait for the async event.

    DLOG(INFO) << "ibv_get_async_event";
    ret = ibv_get_async_event(verbs_context.get(), &event);
    if (ret) {
      LOG(ERROR) << "Fail to get AE.";
      return;
    }

    // Print the event type and handle it.
    LOG(WARNING) << "Receive AE: " << ibv_event_type_str(event.event_type);
    // Finish immediately if DEVICE_FATAL is detected, as the behavior for
    // continuing is undefined and may cause problems.
    if (event.event_type == IBV_EVENT_DEVICE_FATAL) {
      LOG(ERROR) << "Fatal error event for device " << device_name
                 << ". Stop waiting for async event.";
      return;
    }
    DLOG(INFO) << "ibv_ack_async_event";
    ibv_ack_async_event(&event);
  }
}

std::unique_ptr<ibv_context, ContextDeleter> OpenDeviceOrDie(
    const absl::string_view device_name) {
  absl::StatusOr<std::unique_ptr<ibv_context, ContextDeleter>>
      status_or_context = OpenDevice(device_name);
  CHECK_OK(status_or_context.status());  // Crash OK
  return *std::move(status_or_context);
}

std::string IpToString(in6_addr* addr6) {
  if (addr6 == nullptr) {
    return "invalid addres: null";
  }
  if (IN6_IS_ADDR_V4MAPPED(addr6)) {
    u_char* p = addr6->s6_addr;
    in_addr_t inaddr;
    inaddr = p[12] << 24;
    inaddr += p[13] << 16;
    inaddr += p[14] << 8;
    inaddr += p[15];
    inaddr = ntohl(inaddr);

    char buffer[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &inaddr, buffer, INET6_ADDRSTRLEN);
    return std::string(buffer);
  }
  char buffer[INET6_ADDRSTRLEN];
  inet_ntop(AF_INET6, addr6, buffer, INET6_ADDRSTRLEN);
  return std::string(buffer);
}

absl::StatusOr<LocalIbverbsAddress> GetLocalAddress(ibv_context* context) {
  if (context == nullptr) {
    return absl::FailedPreconditionError("ibv_context cannot be null.");
  }

  ibv_device_attr dev_attr;

  if (int query_result = ibv_query_device(context, &dev_attr);
      query_result != 0) {
    return absl::InternalError(absl::StrCat(
        "ibv_query_device failed with return value: ", query_result));
  }

  // Find the first valid port. libibverbs port numbers start at 1.
  std::optional<LocalIbverbsAddress> ipv4_address;
  std::optional<LocalIbverbsAddress> invalid_address;
  for (uint8_t port_idx = 1; port_idx <= dev_attr.phys_port_cnt; ++port_idx) {
    ibv_port_attr port_attr;
    DLOG(INFO) << "ibv_query_port";
    if (int query_result = ibv_query_port(context, port_idx, &port_attr);
        query_result != 0) {
      return absl::InternalError(absl::StrCat(
          "ibv_query_port failed with return value: ", query_result));
    }
    if (port_attr.state != IBV_PORT_ACTIVE) {
      continue;
    }
    enum ibv_mtu active_mtu = port_attr.active_mtu;
    LOG(INFO) << "Verbs MTU detected: " << active_mtu;
    if (absl::GetFlag(FLAGS_ibverb_mtu_override) > 0) {
      int requested_mtu = absl::GetFlag(FLAGS_ibverb_mtu_override);
      switch (requested_mtu) {
        case 256:
          active_mtu = IBV_MTU_256;
          break;
        case 512:
          active_mtu = IBV_MTU_512;
          break;
        case 1024:
          active_mtu = IBV_MTU_1024;
          break;
        case 2048:
          active_mtu = IBV_MTU_2048;
          break;
        case 4096:
          active_mtu = IBV_MTU_4096;
          break;
        default:
          LOG(ERROR) << "Requested MTU " << requested_mtu
                     << " is not a valid value.";
      }
    }
    LOG(INFO) << "Verbs MTU used: " << active_mtu;

    ibv_gid gid;
    int gid_index_override = absl::GetFlag(FLAGS_ibverb_gid_override);
    // Override flag is set.
    if (gid_index_override != kGidOverrideInvalid) {
      if (gid_index_override < 0 || gid_index_override > UINT8_MAX) {
        return absl::InternalError(
            absl::StrCat("Invalid GID index: ", gid_index_override));
      }
      uint8_t gid_index = static_cast<uint8_t>(gid_index_override);
      DLOG(INFO) << "ibv_query_gid " << int(gid_index);
      if (int query_result = ibv_query_gid(context, port_idx, gid_index, &gid);
          query_result != 0) {
        return absl::InternalError(absl::StrCat(
            "ibv_query_gid failed with return value: ", query_result));
      }

      in6_addr addr;
      memcpy(&addr, gid.raw, sizeof(addr));
      LOG(INFO) << "GID to use: port " << int(port_idx)
                << " index: " << int(gid_index)
                << " addr: " << IpToString(&addr);
      return LocalIbverbsAddress{.port = port_idx,
                                 .mtu = active_mtu,
                                 .gid = gid,
                                 .gid_index = gid_index,
                                 .ip_addr = IpToString(&addr)};
    }
    // gid_tbl_len can be bigger than UINT8_MAX as it's integer. To prevent
    // infinite loop, cap it at UINT8_MAX.
    uint8_t max_gid_index =
        (port_attr.gid_tbl_len > UINT8_MAX) ? UINT8_MAX : port_attr.gid_tbl_len;
    for (uint8_t gid_index = 0; gid_index < max_gid_index; ++gid_index) {
      DLOG(INFO) << "ibv_query_gid " << int(gid_index);
      if (int query_result = ibv_query_gid(context, port_idx, gid_index, &gid);
          query_result != 0) {
        return absl::InternalError(absl::StrCat(
            "ibv_query_gid failed with return value: ", query_result));
      }

      in6_addr addr;
      memcpy(&addr, gid.raw, sizeof(addr));
      // Can't use this IP address.
      if (IN6_IS_ADDR_LINKLOCAL(&addr) || IN6_IS_ADDR_UNSPECIFIED(&addr) ||
          IN6_IS_ADDR_LOOPBACK(&addr)) {
        LOG(INFO) << "A local ipv6 gid: " << int(gid_index)
                  << " found. Will prefer other gid if exists.";
        invalid_address.emplace(
            LocalIbverbsAddress{.port = port_idx,
                                .mtu = active_mtu,
                                .gid = gid,
                                .gid_index = gid_index,
                                .ip_addr = IpToString(&addr)});
        continue;
      }

      // Prefer IPv6 addresses to IPv4 addresses, but if no IPv6 address is
      // available, use the first valid IPv4 address.
      if (IN6_IS_ADDR_V4MAPPED(&addr) && !ipv4_address.has_value()) {
        ipv4_address.emplace(LocalIbverbsAddress{.port = port_idx,
                                                 .mtu = active_mtu,
                                                 .gid = gid,
                                                 .gid_index = gid_index,
                                                 .ip_addr = IpToString(&addr)});
        continue;
      }

      LOG(INFO) << "GID to use: port " << int(port_idx)
                << " index: " << int(gid_index)
                << " addr: " << IpToString(&addr);
      // Use the first valid IPv6 address.
      return LocalIbverbsAddress{.port = port_idx,
                                 .mtu = active_mtu,
                                 .gid = gid,
                                 .gid_index = gid_index,
                                 .ip_addr = IpToString(&addr)};
    }
  }

  if (ipv4_address.has_value()) {
    LOG(INFO) << "Did not find IPv6 address. Using IPv4 address.";
    // We didn't find a valid IPv6 address, use the valid IPv4 address.
    return ipv4_address.value();
  } else {
    // If all addresses were invalid but a successful query result exists, use
    // it. This can help loopback.
    if (invalid_address.has_value()) {
      return invalid_address.value();
    }
    return absl::InternalError("No active ports detected.");
  }
}

absl::StatusOr<int> GetMaxQpWr(ibv_context* context) {
  if (context == nullptr) {
    return absl::FailedPreconditionError("ibv_context cannot be null.");
  }

  ibv_device_attr dev_attr;
  DLOG(INFO) << "ibv_query_device ";
  if (int query_result = ibv_query_device(context, &dev_attr);
      query_result != 0) {
    return absl::InternalError(absl::StrCat(
        "ibv_query_device failed with return value: ", query_result));
  }

  return dev_attr.max_qp_wr;
}

LocalIbverbsAddress GetLocalAddressOrDie(ibv_context* context) {
  absl::StatusOr<LocalIbverbsAddress> status_or_address =
      GetLocalAddress(context);
  if (!status_or_address.ok()) {
    std::cout << status_or_address.status() << '\n';
  }
  CHECK_OK(status_or_address.status());  // Crash OK
  return *status_or_address;
}

}  // namespace ibverbs_utils
}  // namespace verbsmarks
