# Verbsmarks Design
This document details the components of Verbsmarks: the leader/follower roles,
traffic generation abstractions, and input/output protobuf types.

## Leader and Followers
The Verbsmarks binary (`verbsmarks_main`) may be run as a leader or follower. A
deployment of Verbsmarks consists of one leader instance and multiple followers.
The leader serves as a control-plane experiment coordinator, while followers are
participants in the RDMA workload. The leader parses the globally-defined
traffic pattern and generates individual workloads for each follower. The leader
awaits follower startup and triggers the start of the workload, and collects
final measurement results from all followers.

An optional health check mechanism allows the leader and follower to perform
periodic status checks via heartbeat messages.

Leader-follower communication uses gRPC, and has the following phases:
* *Form Quorum*: followers send a request to the leader upon initialization.
* *Ready*: once all followers join quorum, the leader sends a traffic pattern
  specification to each follower and facilitates connection between followers.
* *Start Experiment*: once all followers are connected, the leader signals to
  start generating traffic.
* *During Experiment*: followers send regular heartbeats to the leader,
  including workload metrics and any errors, and check for leader response. The
  experiment will abort upon an error or heartbeat timeout.
* *Finish*: once all followers report final results to the leader, the leader
  saves a final result file and prompts follower connection cleanup.


## Traffic Generation
Verbsmarks workloads are defined using a LeaderConfig protobuf (see below),
which includes one or more traffic patterns involving multiple followers. The
leader translates these global traffic patterns into per-follower traffic
patterns, containing the necessary information for each follower to generate
traffic. Based on the per-follower traffic pattern, a follower instantiates one
or more TrafficGenerators. A TrafficGenerator is a thread which posts and polls
RDMA operations via one or more queue pairs.

Verbsmarks includes multiple TrafficGenerator variants to support a range of
workloads. The default TrafficGenerator supports open-loop and closed-loop load
generation, with explicitly-defined or uniform random flows. Two additional
TrafficGenerator implementations provide specialized functionality:

* *BandwidthTraffic* is optimized for closed-loop throughput-oriented workloads.
  Traffic may be unidirectional or bidirectional. Some detailed measurements are
  excluded to increase per-thread performance.
* *PingpongTraffic* supports WRITE and SEND pingpong workloads, with either
  poll-mode or event-driven completions.

Verbsmarks supports WRITE, WRITE_WITH_IMM, SEND, SEND_WITH_IMM, and READ.

## LeaderConfig, ExperimentConfig Protobufs
To assist exploring performance over different parameters, such as latency over
a range of message sizes, Verbsmarks supports parameter sweep experiments. This
is done by generating a sweep of configurations, representing all combinations
of parameters, and running the individual configurations. The ExperimentConfig
proto defines a parameter sweep, and the LeaderConfig proto describes a single
invocation of Verbsmarks.

* *LeaderConfig* specifies a single invocation of Verbsmarks. The proto filename
  is passed as an argument upon starting the leader process. The workload is
  defined as one or more GlobalTrafficPattern objects, along with additional
  fields specifying memory registration and thread affinity options. The leader
  translates the LeaderConfig into per-follower traffic patterns and
  configuration settings.
* *ExperimentConfig* specifies a parameter sweep. It is used to generate a set
  of LeaderConfig files with all combinations of sweep parameters. This proto
  references a base LeaderConfig file, and applies parameter combinations over
  the base LeaderConfig. Sweep parameters are defined using the
  VerbsmarksSweepParameterConfig field, where sweep parameters are specified as
  repeated values. ExperimentConfigs are input to a separate binary
  (simple_sweep_main), which generates a cartesian product of all repeated sweep
  parameters, outputting a LeaderConfig file for each combination.


## Result Protobuf
The Verbsmarks leader process saves a result protobuf file when a workload ends
successfully. The result includes a summary of performance metrics, as well as
granular statistics. Overall performance results are provided on a
per-traffic-pattern basis:
* Completion and failure counts
* Latency percentiles calculated via tdigest
* Cumulative throughput

The same statistics are also collected for each QP, and for each operation type
and size combination in the traffic pattern. Optionally, per-second throughput
and latency statistics are reported for each 1 second interval of experiment
runtime. This is enabled in the LeaderConfig’s MetricsCollection field.
