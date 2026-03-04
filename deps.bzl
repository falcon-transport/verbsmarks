# Copyright 2025 Google LLC
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

"""Verbsmarks dependencies."""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:local.bzl", "new_local_repository")

# Base directory for libibverbs and libnuma.
USR_LIB_DIR = "/usr/lib/x86_64-linux-gnu"
LIBVERBS_DIR = USR_LIB_DIR
LIBNUMA_DIR = USR_LIB_DIR

def _deps_extension_impl(_ctx):
    http_archive(
        name = "farmhash",
        build_file_content = """
cc_library(
    name = "farmhash",
    srcs = ["farmhash.cc"],
    hdrs = ["farmhash.h"],
    includes = ["src/."],
    visibility = ["//visibility:public"],
    defines=["NAMESPACE_FOR_HASH_FUNCTIONS=farmhash"],
)
""",
        integrity = "sha256-GDks8HNuHWLsu41pXDFJa2UHhZ6MdVQdetC6CS3FIRU=",
        strip_prefix = "farmhash-0d859a811870d10f53a594927d0d0b97573ad06d/src",
        urls = ["https://github.com/google/farmhash/archive/0d859a811870d10f53a594927d0d0b97573ad06d.tar.gz"],
    )

    new_local_repository(
        name = "libibverbs",
        build_file_content = """
cc_library(
    name = "libibverbs",
    srcs = [
        "libibverbs.so",
        "libnl-3.so.200",
        "libnl-route-3.so.200",
    ],
    visibility = ["//visibility:public"],
)
""",
        path = LIBVERBS_DIR,
    )

    new_local_repository(
        name = "libnuma",
        build_file_content = """
cc_library(
    name = "libnuma",
    srcs = [
        "libnuma.so",
    ],
    visibility = ["//visibility:public"],
)
""",
        path = LIBNUMA_DIR,
    )

deps_extension = module_extension(implementation = _deps_extension_impl)
