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

proto_library(
    name = "experiment_config_proto",
    srcs = ["experiment_config.proto"],
    deps = [
        ":experiment_host_proto",
        "//:verbsmarks_proto",
    ],
)

cc_proto_library(
    name = "experiment_config_cc_proto",
    deps = [":experiment_config_proto"],
)

proto_library(
    name = "experiment_host_proto",
    srcs = ["experiment_host.proto"],
)

cc_proto_library(
    name = "experiment_host_cc_proto",
    deps = [":experiment_host_proto"],
)

cc_library(
    name = "experiment_sweep_parameter_util",
    srcs = [
        "experiment_sweep_parameter_util.cc",
    ],
    hdrs = [
        "experiment_sweep_parameter_util.h",
    ],
    deps = [
        ":config_utils",
        ":experiment_config_cc_proto",
        "//:verbsmarks_cc_proto",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "@protobuf",
        "@protobuf//:duration_cc_proto",
    ],
)

cc_library(
    name = "config_utils",
    srcs = ["config_utils.cc"],
    hdrs = ["config_utils.h"],
    deps = [
        "//:verbsmarks_cc_proto",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/strings",
        "@protobuf",
    ],
)

cc_binary(
    name = "simple_sweep_main",
    srcs = [
        "simple_sweep_main.cc",
    ],
    deps = [
        ":experiment_config_cc_proto",
        ":experiment_sweep_parameter_util",
        "//:utils",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/flags:parse",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@protobuf",
    ],
)
