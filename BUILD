# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

load("@com_github_grpc_grpc//bazel:cc_grpc_library.bzl", "cc_grpc_library")
load("@rules_license//rules:license.bzl", "license")

exports_files(
    ["LICENSE"],
    ["//visibility:public"],
)

proto_library(
    name = "verbsmarks_proto",
    srcs = ["verbsmarks.proto"],
    visibility = [":__subpackages__"],
    deps = [
        "@googleapis//google/rpc:status_proto",
        "@protobuf//:duration_proto",
        "@protobuf//:timestamp_proto",
    ],
)

cc_proto_library(
    name = "grpc_code_cc_proto",
    deps = ["@googleapis//google/rpc:code_proto"],
)

cc_proto_library(
    name = "grpc_status_cc_proto",
    deps = ["@googleapis//google/rpc:status_proto"],
)

cc_proto_library(
    name = "verbsmarks_cc_proto",
    visibility = [":__subpackages__"],
    deps = [":verbsmarks_proto"],
)

cc_grpc_library(
    name = "verbsmarks_cc_grpc",
    srcs = [":verbsmarks_proto"],
    generate_mocks = True,
    grpc_only = True,
    deps = [":verbsmarks_cc_proto"],
)

cc_library(
    name = "verbsmarks_follower",
    srcs = ["verbsmarks_follower.cc"],
    hdrs = ["verbsmarks_follower.h"],
    deps = [
        ":connection_coordinator",
        ":connection_coordinator_cc_grpc",
        ":connection_coordinator_cc_proto",
        ":grpc_code_cc_proto",
        ":grpc_status_cc_proto",
        ":ibverbs_utils",
        ":memory_manager",
        ":traffic_generator",
        ":utils",
        ":verbsmarks_binary_flags",
        ":verbsmarks_cc_grpc",
        ":verbsmarks_cc_proto",
        "@com_github_grpc_grpc//:grpc++",
        "@com_github_grpc_grpc//:grpc++_public_hdrs",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@libibverbs",
    ],
)

cc_library(
    name = "traffic_pattern_translator",
    srcs = ["traffic_pattern_translator.cc"],
    hdrs = ["traffic_pattern_translator.h"],
    deps = [
        ":utils",
        ":verbsmarks_cc_proto",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "verbsmarks_leader",
    srcs = ["verbsmarks_leader.cc"],
    hdrs = ["verbsmarks_leader.h"],
    deps = [
        ":grpc_code_cc_proto",
        ":grpc_status_cc_proto",
        ":traffic_pattern_translator",
        ":utils",
        ":verbsmarks_binary_flags",
        ":verbsmarks_cc_proto",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@protobuf",
    ],
)

cc_library(
    name = "verbsmarks_leader_service",
    srcs = ["verbsmarks_leader_service.cc"],
    hdrs = ["verbsmarks_leader_service.h"],
    deps = [
        ":verbsmarks_cc_grpc",
        ":verbsmarks_cc_proto",
        ":verbsmarks_leader",
        "@com_github_grpc_grpc//:gpr",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/functional:bind_front",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "ibverbs_utils",
    srcs = ["ibverbs_utils.cc"],
    hdrs = ["ibverbs_utils.h"],
    deps = [
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@libibverbs",
    ],
)

cc_library(
    name = "utils",
    srcs = ["utils.cc"],
    hdrs = ["utils.h"],
    visibility = [":__subpackages__"],
    deps = [
        ":connection_coordinator_cc_proto",
        ":verbsmarks_binary_flags",
        ":verbsmarks_cc_proto",
        "@com_github_grpc_grpc//:grpc++",
        "@com_github_grpc_grpc//:grpc++_public_hdrs",
        "@com_github_grpc_grpc//:grpc_security_base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@folly",
        "@protobuf",
        "@protobuf//:duration_cc_proto",
        "@protobuf//:protobuf_lite",
        "@protobuf//:timestamp_cc_proto",
    ],
)

proto_library(
    name = "connection_coordinator_proto",
    srcs = ["connection_coordinator.proto"],
)

cc_proto_library(
    name = "connection_coordinator_cc_proto",
    deps = [":connection_coordinator_proto"],
)

cc_grpc_library(
    name = "connection_coordinator_cc_grpc",
    srcs = [":connection_coordinator_proto"],
    generate_mocks = True,
    grpc_only = True,
    deps = [":connection_coordinator_cc_proto"],
)

cc_library(
    name = "connection_coordinator",
    srcs = ["connection_coordinator.cc"],
    hdrs = ["connection_coordinator.h"],
    deps = [
        ":connection_coordinator_cc_grpc",
        ":connection_coordinator_cc_proto",
        ":utils",
        ":verbsmarks_cc_proto",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_library(
    name = "queue_pair",
    srcs = ["queue_pair.cc"],
    hdrs = ["queue_pair.h"],
    tags = [
        "notsan",  # grpc provider race on incoming RPC
    ],
    deps = [
        ":completion_queue_manager",
        ":connection_coordinator_cc_proto",
        ":ibverbs_utils",
        ":throughput_computer",
        ":utils",
        ":verbsmarks_binary_flags",
        ":verbsmarks_cc_proto",
        "@com_github_google_farmhash//:farmhash",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:optional",
        "@folly",
        "@libibverbs",
    ],
)

cc_library(
    name = "completion_queue_manager",
    srcs = ["completion_queue_manager.cc"],
    hdrs = ["completion_queue_manager.h"],
    deps = [
        ":ibverbs_utils",
        ":utils",
        ":verbsmarks_cc_proto",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@libibverbs",
    ],
)

cc_library(
    name = "load_generator",
    srcs = ["load_generator.cc"],
    hdrs = ["load_generator.h"],
    deps = [
        ":utils",
        ":verbsmarks_cc_proto",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:distributions",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "memory_manager",
    srcs = ["memory_manager.cc"],
    hdrs = ["memory_manager.h"],
    deps = [
        ":ibverbs_utils",
        ":queue_pair",
        ":verbsmarks_binary_flags",
        ":verbsmarks_cc_proto",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@libibverbs",
        "@protobuf//:protobuf_lite",
    ],
)

cc_library(
    name = "special_traffic_generator",
    srcs = ["special_traffic_generator.cc"],
    hdrs = [
        "special_traffic_generator.h",
    ],
    deps = [
        ":completion_queue_manager",
        ":queue_pair",
        ":utils",
        ":verbsmarks_cc_proto",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@folly",
        "@libibverbs",
    ],
)

cc_library(
    name = "traffic_generator",
    srcs = ["traffic_generator.cc"],
    hdrs = ["traffic_generator.h"],
    deps = [
        ":completion_queue_manager",
        ":connection_coordinator",
        ":connection_coordinator_cc_proto",
        ":ibverbs_utils",
        ":load_generator",
        ":memory_manager",
        ":queue_pair",
        ":special_traffic_generator",
        ":throughput_computer",
        ":utils",
        ":verbsmarks_binary_flags",
        ":verbsmarks_cc_proto",
        "@com_github_grpc_grpc//:grpc++",
        "@com_github_grpc_grpc//:grpc++_public_hdrs",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:distributions",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@folly",
        "@libibverbs",
        "@protobuf//:duration_cc_proto",
        "@protobuf//:protobuf_lite",
    ],
)

cc_library(
    name = "verbsmarks_binary",
    srcs = ["verbsmarks_binary.cc"],
    hdrs = ["verbsmarks_binary.h"],
    deps = [
        ":utils",
        ":verbsmarks_binary_flags",
        ":verbsmarks_cc_grpc",
        ":verbsmarks_cc_proto",
        ":verbsmarks_follower",
        ":verbsmarks_leader",
        ":verbsmarks_leader_service",
        "@com_github_grpc_grpc//:grpc++",
        "@com_github_grpc_grpc//:grpc++_public_hdrs",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
    ],
)

cc_binary(
    name = "verbsmarks_main",
    srcs = [
        "verbsmarks_main.cc",
    ],
    deps = [
        ":ibverbs_utils",
        ":utils",
        ":verbsmarks_binary",
        ":verbsmarks_binary_flags",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/flags:parse",
        "@com_google_absl//absl/log",
    ],
)

cc_library(
    name = "throughput_computer",
    srcs = ["throughput_computer.cc"],
    hdrs = ["throughput_computer.h"],
    deps = [
        ":utils",
        ":verbsmarks_cc_proto",
        "@com_google_absl//absl/log",
    ],
)

cc_library(
    name = "verbsmarks_binary_flags",
    srcs = ["verbsmarks_binary_flags.cc"],
    hdrs = ["verbsmarks_binary_flags.h"],
    deps = [
        "@com_google_absl//absl/flags:flag",
    ],
)
