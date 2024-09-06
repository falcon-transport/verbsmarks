# Verbsmarks

Verbsmarks is a synthetic ibverbs traffic generator to enable at-scale
performance testing of RDMA networks. Verbsmarks supports workloads with a large
number of hosts and queue pairs, providing a range of traffic pattern
specifications and granular performance statistics.

## Verbsmarks Overview
A deployment of Verbsmarks consists of one leader instance and multiple
followers. The leader handles experiment coordination, signaling to followers to
start an experiment, and collecting results. Each follower process performs the
experiment workload by managing one or more traffic generators. Each traffic
generator submits ibverbs operations and polls completions in one thread, with
one or more queue pairs. Additional special traffic generator implementations
support pingpong and bandwidth-optimized workloads.

The leader process is run with a configuration protobuf defining the experiment
workload. The leader translates the global config into a per-follower config,
and waits for the followers to initialize. Then, the leader triggers the
experiment start and collects measurements from the followers on a periodic
basis. When the experiment finishes, the leader generates a combined result
including all metrics.

See `verbsmarks.md` for a description of the main components of Verbsmarks, and
the input/output file specifications.

## Building Verbsmarks
Verbsmarks can be built using `bazel 7.1.1`. Run from the repository root:
```
bazel build verbsmarks_main
bazel build tools:simple_sweep_main
```

### Build Troubleshooting
* If Bazel is not available as a native package (e.g., Rocky linux), build it
  from source, following the directions at https://github.com/bazelbuild/bazel/.
* If libibverbs is installed in a path other than `/usr/lib/x86_64-linux-gnu`
  (default for Ubuntu), update the location of libibverbs in `WORKSPACE.bazel`
  to point to your local installation, for instance:
  `sed -i 's/\/usr\/lib\/x86_64-linux-gnu/\/usr\/lib64/g' WORKSPACE.bazel`
* Prevent Bazel from overriding dependency URLs with internal mirrors if you
  encounter errors downloading dependencies. Run `touch bazel_downloader.cfg`
  in the project root directory.
* Define any needed compiler flags in `.bazelrc`, or specify flags using
  `bazel build --action_env=BAZEL_CXXOPTS="-std=c++17" verbsmarks_main`.
  Verbsmarks must be built with `-std=c++17` or greater.


## Running Verbsmarks
After building Verbsmarks as above, the verbsmarks_main binary (generated in
`./bazel-bin/verbsmarks_main`) should be run for each leader and follower
instance. In addition to the LeaderConfig defining the experiment workflow,
there are a range of runtime flags (see `./verbsmarks_main --help`).

In the `examples` directory, there are many example LeaderConfig files for
various workloads. To run the experiment by manually launching Verbsmarks processes, use the following
commands:

1. Run leader on host1

```
./verbsmarks_main --is_leader=true --leader_config_file_path=${config_dir} \\
    --leader_config_file=${config_filename} --leader_port=9000 \\
    --description="example" --result_file_name=verbsmarks_result.textpb
```

2. Run follower0 on host1

```
./verbsmarks_main --is_leader=false --leader_address=${ip_address_host1} \\
    --leader_port=9000 --follower_port=9001 \\
    --follower_address=${ip_address_host1} --follower_alias=follower0
```

3. Run follower1 on host2

```
./verbsmarks_main --is_leader=false --leader_address=${ip_address_host1} \\
    --leader_port=9000 --follower_port=9002 \\
    --follower_address=${ip_address_host2} --follower_alias=follower1
```


## Parameter Sweep Experiments
Verbsmarks uses the `ExperimentConfig` protobuf to define parameter sweep
experiments. This protobuf is translated into a series of `LeaderConfig` files
by the `tools/simple_sweep_main` binary. Each ExperimentConfig references a
base LeaderConfig proto containing constant settings, and applies sweep values
to this base config. Refer to the inline documentation in
`tools/experiment_config.proto` and the ExperimentConfgis listed below for
examples of sweep functionality.

To generate LeaderConfigs from an ExperimentConfig sweep specification:
```
./tools/simple_sweep_main --experiment_config=${experiment_config_textpb} \
    --output_dir=${leader_config_dir}
```

This will generate a sequence of numbered `LeaderConfig` textpb files named as
`${sweep_index}_${sweep_parameter_1}_..._${sweep_parameter_N}.textpb` in the
output directory. Once the sweep outputs are written, the ExperimentConfig and
base LeaderConfig protos are no longer needed; simply launch `verbsmarks_main`
with one of the generated LeaderConfigs.


## Example Experiment Configurations
The `examples` directory contains a range of configurations for various
workloads and parameter sweep experiments. Inline comments describe the fields
within LeaderConfig and ExperimentConfig protobufs which control Verbsmarks
functionality (in addition to the documentation within `verbsmarks.proto`).

#### Bandwidth-oriented workloads (`examples/bandwidth_sweep/`)
BandwidthTraffic is a traffic pattern that is optimized to push the NIC further
by reducing computation during experiments as much as possible. This is suitable for measuring op rate or bandwidth. Below examples are provided
in this category.

* `bandwidth_base_config.textpb` 2-node, unidirectional bandwidth workload
  with 4096B WRITE ops.
* `sweep_bandwidth_over_msg_size.textpb` Parameter sweep for 2-node
  unidirectional bandwidth, with 1KB-128KB op sizes and READ/WRITE/SEND_RECEIVE
  op types.
* `bandwidth_bidi_base_config.textpb` 2-node, bidirectional bandwidth workload
  with 4096B WRITE ops.
* `sweep_bandwidth_bidi_over_msg_size.textpb` Parameter sweep for 2-node
  bidirectional bandwidth, with 1KB-128KB op sizes and READ/WRITE/SEND_RECEIVE
  op types.

#### Latency-oriented workloads (`examples/latency_sweep/`)
Verbsmarks' default traffic generator provides detailed latency measurements.
Here are examples of latency focused examples.

* `latency_base_config.textpb` 2-node, unidirectional latency workload with 1B
  SEND_RECEIVE ops. Closed-loop with 1 outstanding operation.
* `sweep_closedloop_latency_over_msg_size.textpb` Parameter sweep for 2-node
  closed-loop WRITE latency, with 1B-2MB op sizes.
* `sweep_openloop_latency_over_msg_size.textpb` Parameter sweep for 2-node open-
  loop WRITE latency, with 1B-2MB op sizes, and 0.005Gbps offered load, 100
  threads.
* `sweep_latency_over_qp_count.textpb` Parameter sweep for 2-node open-loop
  WRITE latency, varying QP count, with 0.005Gbps offered load, 100-10000 QPs,
  and 100 threads.

#### Pingpong workloads (`examples/pingpong_sweep/`)
* `pingpong_base_config.textpb` 2-node pingpong workload with 1B SEND_RECEIVE
  ops.
* `sweep_pingpong_over_msg_size.textpb` Parameter sweep for 2-node pingpong
  latency, with 1B-2MB op sizes, and SEND_RECEIVE and WRITE op types.

#### Incast, outcast workloads (`examples/incast_outcast/`)
* `incast_5to1_config.textpb` Incast workload with 1000KB WRITE ops from 5
  initiator nodes to 1 target node.
* `outcast_5to1_config.textpb` Outcast workload with 1000KB WRITE ops from 1
  initiator node to 5 targets.

#### Uniform random workloads (`examples/uniform_random/`)
* `uniform_random_config.textpb` Uniform random traffic with 1 initiator sending
  to 5 total nodes, with mixed 4B READ/WRITE/SEND_RECEIVE ops, with fixed
  arrival distribution (open-loop).

#### Mixed operations workloads (`examples/mixed_ops/`)
* `mixed_op_sizes_config.textpb` 2-node unidirectional traffic with a mixture of
  8B, 1B, and 4KB READ/WRITE operations and open-loop load.
* `mixed_op_types_config.textpb` 2-node unidirectional traffic with operation
  type and size distributions defined on a per-follower basis.
