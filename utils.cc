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

#include "utils.h"

#include <pthread.h>
#include <sched.h>
#include <string.h>
#include <sys/types.h>

#include <cerrno>
#include <cstdint>
#include <fstream>
#include <ios>
#include <iostream>
#include <iterator>
#include <memory>
#include <string>
#include <string_view>

#include "absl/flags/flag.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/time/time.h"
#include "folly/stats/TDigest.h"
#include "google/protobuf/duration.pb.h"
#include "google/protobuf/text_format.h"
#include "grpc/grpc_security_constants.h"
#include "grpcpp/security/credentials.h"
#include "grpcpp/security/server_credentials.h"
#include "verbsmarks.pb.h"
#include "verbsmarks_binary_flags.h"

namespace verbsmarks {
namespace utils {

absl::StatusOr<proto::LeaderConfig> ReadVerbsmarksLeaderConfig() {
  proto::LeaderConfig leader_config;
  const std::string leader_config_filename =
      absl::StrCat(absl::GetFlag(FLAGS_leader_config_file_path), "/",
                   absl::GetFlag(FLAGS_leader_config_file));
  std::ifstream leader_config_file(leader_config_filename);
  if (!leader_config_file.is_open()) {
    return absl::InternalError("Failed to open leader config file");
  }
  std::string textproto_data(
      (std::istreambuf_iterator<char>(leader_config_file)),
      std::istreambuf_iterator<char>());
  if (!google::protobuf::TextFormat::ParseFromString(textproto_data,
                                                     &leader_config)) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Failed to parse leader config file: ", leader_config_filename));
  }
  return leader_config;
}

std::string HostPortString(absl::string_view host, int port) {
  if (!host.empty() && host[0] != '[' && absl::StrContains(host, ':')) {
    // IPv6 and all potential future IP address versions string
    // representations are defined to be enclosed within "[" and "]".
    return absl::StrCat("[", host, "]:", port);
  } else {
    return absl::StrCat(host, ":", port);
  }
}

absl::Status SaveToFile(absl::string_view filename, absl::string_view content,
                        bool append) {
  std::ofstream file_to_save;
  std::ios_base::openmode mode = std::ios::out;
  if (append) {
    mode |= std::ios_base::app;
  } else {
    mode |= std::ios_base::trunc;
  }
  file_to_save.open(std::string(filename), mode);
  if (!file_to_save.is_open()) {
    return absl::InternalError("Failed to save file");
  }
  file_to_save << content;
  file_to_save.close();
  return absl::OkStatus();
}

google::protobuf::Timestamp TimeToProto(absl::Time d) {
  google::protobuf::Timestamp proto;
  const int64_t s = absl::ToUnixSeconds(d);
  proto.set_seconds(s);
  proto.set_nanos((d - absl::FromUnixSeconds(s)) / absl::Nanoseconds(1));
  return proto;
}

void DurationToProto(absl::Duration d, google::protobuf::Duration& proto) {
  // s and n may both be negative, per the Duration proto spec.
  const int64_t s = absl::IDivDuration(d, absl::Seconds(1), &d);
  const int64_t n = absl::IDivDuration(d, absl::Nanoseconds(1), &d);
  proto.set_seconds(s);
  proto.set_nanos(n);
}

absl::Duration ProtoToDuration(const google::protobuf::Duration& proto) {
  return absl::Seconds(proto.seconds()) + absl::Nanoseconds(proto.nanos());
}

std::string_view OpTypeToString(const proto::RdmaOp op_type) {
  switch (op_type) {
    case proto::RdmaOp::RDMA_OP_SEND_RECEIVE:
      return "SEND_RECEIVE";
    case proto::RdmaOp::RDMA_OP_READ:
      return "READ";
    case proto::RdmaOp::RDMA_OP_WRITE:
      return "WRITE";
    case proto::RdmaOp::RDMA_OP_SEND_WITH_IMMEDIATE_RECEIVE:
      return "RDMA_OP_SEND_WITH_IMMEDIATE_RECEIVE";
    case proto::RdmaOp::RDMA_OP_WRITE_IMMEDIATE:
      return "RDMA_OP_WRITE_IMMEDIATE";
    default:
      return "UNKNOWN";
  }
}

proto::ThroughputResult MakeThroughputResult(double ops_per_second,
                                             double bytes_per_second,
                                             int32_t seconds_from_start) {
  proto::ThroughputResult result;
  result.set_ops_per_second(ops_per_second);
  result.set_bytes_per_second(bytes_per_second);
  result.set_gbps((bytes_per_second * 8.0) / (1e9));
  if (seconds_from_start >= 0) {
    result.set_seconds_from_start(seconds_from_start);
  }
  return result;
}

void AppendThroughputResult(proto::ThroughputResult& result,
                            const proto::ThroughputResult& append) {
  result.set_ops_per_second(result.ops_per_second() + append.ops_per_second());
  result.set_bytes_per_second(result.bytes_per_second() +
                              append.bytes_per_second());
  result.set_gbps(result.gbps() + append.gbps());
}

proto::LatencyResult LatencyTdigestToProto(const folly::TDigest& tdigest) {
  proto::LatencyResult proto;
  utils::DurationToProto(absl::Nanoseconds(tdigest.mean()),
                         *proto.mutable_average_latency());
  utils::DurationToProto(absl::Nanoseconds(tdigest.estimateQuantile(0.5)),
                         *proto.mutable_median_latency());
  utils::DurationToProto(absl::Nanoseconds(tdigest.estimateQuantile(0.99)),
                         *proto.mutable_p99_latency());
  utils::DurationToProto(absl::Nanoseconds(tdigest.estimateQuantile(0.999)),
                         *proto.mutable_p999_latency());
  utils::DurationToProto(absl::Nanoseconds(tdigest.estimateQuantile(0.9999)),
                         *proto.mutable_p9999_latency());
  utils::DurationToProto(absl::Nanoseconds(tdigest.min()),
                         *proto.mutable_min_latency());
  return proto;
}

std::shared_ptr<grpc::ServerCredentials> GetGrpcServerCredentials() {
  std::string cred = absl::GetFlag(FLAGS_grpc_creds);
  if (cred == "GRPC_CRED_SSL") {
    return grpc::SslServerCredentials(grpc::SslServerCredentialsOptions());
  }
  if (cred == "GRPC_CRED_LOCAL") {
    return grpc::experimental::LocalServerCredentials(LOCAL_TCP);
  }
  if (cred == "GRPC_CRED_INSECURE") {
    return grpc::InsecureServerCredentials();
  }
  LOG(FATAL) << "Unknown credential config.";
  return nullptr;
}

std::shared_ptr<grpc::ChannelCredentials> GetGrpcChannelCredentials() {
  std::string cred = absl::GetFlag(FLAGS_grpc_creds);
  if (cred == "GRPC_CRED_SSL") {
    return grpc::SslCredentials(grpc::SslCredentialsOptions());
  }
  if (cred == "GRPC_CRED_LOCAL") {
    return grpc::experimental::LocalCredentials(LOCAL_TCP);
  }
  if (cred == "GRPC_CRED_INSECURE") {
    return grpc::InsecureChannelCredentials();
  }
  LOG(FATAL) << "Unknown credential config.";
  return nullptr;
}

int GetCpuCount() { return sysconf(_SC_NPROCESSORS_ONLN); }

int AssignableCpu(int desired_cpu) {
  int cpu_cnt = GetCpuCount();
  if (desired_cpu > cpu_cnt) {
    desired_cpu = desired_cpu % cpu_cnt;
  }
  return desired_cpu;
}

void SetThreadAffinity(int desired_cpu, pthread_t thread) {
  cpu_set_t cpuset;

  // Set affinity mask to include only the desired CPU.
  CPU_ZERO(&cpuset);
  CPU_SET(desired_cpu, &cpuset);
  int ret = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
  if (ret != 0) {
    LOG(ERROR) << "pthread_setaffinity_np failed: " << ret << ": " << errno;
  }
}

absl::StatusOr<int> GetThreadAffinity(pthread_t thread) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  // Check the affinity mask assigned to the thread
  int ret = pthread_getaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
  if (ret != 0) {
    return absl::InternalError(
        absl::StrCat("pthread_getaffinity_np failed", strerror(errno)));
  }

  int num_cpu = sysconf(_SC_NPROCESSORS_ONLN);
  for (int i = 0; i < num_cpu; ++i) {
    if (CPU_ISSET(i, &cpuset)) {
      return i;
    }
  }
  return absl::NotFoundError(
      absl::StrCat("Could not found any CPU for thread: ", thread));
}

bool SetThreadAffinityAndLog(int desired_cpu, pthread_t thread) {
  SetThreadAffinity(desired_cpu, thread);
  auto status_or_cpu = GetThreadAffinity(thread);
  if (status_or_cpu.ok()) {
    int used_cpu = status_or_cpu.value();
    if (used_cpu != desired_cpu) {
      LOG(ERROR) << "Requested to pin thread " << thread << " on CPU "
                 << desired_cpu << " and running on " << used_cpu;
      return false;
    }
    VLOG(2) << "Thread " << thread << " pinned on " << used_cpu
            << " as requested " << desired_cpu;
    return true;
  }
  LOG(ERROR) << "Failed to set thread affinity for thread " << thread << ": "
             << status_or_cpu.status();

  return false;
}

}  // namespace utils
}  // namespace verbsmarks
