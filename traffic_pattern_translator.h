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

#ifndef VERBSMARKS_TRAFFIC_PATTERN_TRANSLATOR_H_
#define VERBSMARKS_TRAFFIC_PATTERN_TRANSLATOR_H_

#include <memory>
#include <utility>
#include <variant>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/time/time.h"
#include "verbsmarks.pb.h"

namespace verbsmarks {

using ThreadAffinityParamType =
    std::variant<std::monostate, proto::ThreadAffinityParameter,
                 proto::ExplicitThreadAffinityParameter>;

// The TrafficPatternTranslator translates a  global traffic pattern into per
// follower traffic patterns. Common fields are populated by the base class and
// actions that are specific to certain traffic pattern type are implemented
// individually in derived classes.
class TrafficPatternTranslator {
 public:
  // A factory method to build a traffic pattern translator.
  // It takes group size and translate participants into specific participants
  // before passing it to the constructor so the remaining code can work on
  // specific participants.
  static absl::StatusOr<std::unique_ptr<TrafficPatternTranslator>> Build(
      proto::GlobalTrafficPattern global,
      const proto::ParticipantList& all_followers,
      proto::ThreadAffinityType affinity_type,
      ThreadAffinityParamType affinity_param = std::monostate{});

  virtual ~TrafficPatternTranslator() = default;

  // A helper that returns the participants list.
  virtual verbsmarks::proto::ParticipantList GetParticipants();

  // Adds PerFollowerTrafficPattern to the ReadyResponse for the given
  // participant. It 1) if the ready response already has the traffic pattern,
  // returns an error. 2) returns OK if it doesn't belong to the traffic. 3)
  // adds a per follower traffic pattern, fills in global traffic id and
  // follower id, and passes it to the specific translator of the traffic.
  absl::Status AddToResponse(int participant,
                             verbsmarks::proto::ReadyResponse* response);

 protected:
  explicit TrafficPatternTranslator(
      const verbsmarks::proto::GlobalTrafficPattern& global,
      proto::ThreadAffinityType affinity_type,
      ThreadAffinityParamType affinity_param);

 private:
  // FillTrafficPattern fills in the per follower traffic pattern for the
  // specific traffic pattern.
  virtual absl::Status FillTrafficPattern(
      int participant,
      verbsmarks::proto::PerFollowerTrafficPattern* per_follower_traffic) = 0;

  const verbsmarks::proto::GlobalTrafficPattern global_;
  const proto::ThreadAffinityType affinity_type_;
  const ThreadAffinityParamType affinity_param_;
  absl::flat_hash_set<int> participants_;
};

// Handles explicit traffic pattern.
class ExplicitTrafficPatternTranslator : public TrafficPatternTranslator {
 public:
  explicit ExplicitTrafficPatternTranslator(
      const verbsmarks::proto::GlobalTrafficPattern& global,
      const proto::ThreadAffinityType affinity_type,
      ThreadAffinityParamType affinity_param)
      : TrafficPatternTranslator(global, affinity_type, affinity_param),
        global_(global) {}

  absl::Status FillTrafficPattern(int participant,
                                  verbsmarks::proto::PerFollowerTrafficPattern*
                                      per_follower_traffic) override;

 private:
  const verbsmarks::proto::GlobalTrafficPattern global_;
};

class PingPongTrafficPatternTranslator : public TrafficPatternTranslator {
 public:
  explicit PingPongTrafficPatternTranslator(
      const verbsmarks::proto::GlobalTrafficPattern& global,
      const proto::ThreadAffinityType affinity_type,
      ThreadAffinityParamType affinity_param)
      : TrafficPatternTranslator(global, affinity_type, affinity_param),
        global_(global) {}

  absl::Status FillTrafficPattern(int participant,
                                  verbsmarks::proto::PerFollowerTrafficPattern*
                                      per_follower_traffic) override;

 private:
  const verbsmarks::proto::GlobalTrafficPattern global_;
};

class BandwidthTrafficPatternTranslator : public TrafficPatternTranslator {
 public:
  explicit BandwidthTrafficPatternTranslator(
      const verbsmarks::proto::GlobalTrafficPattern& global,
      const proto::ThreadAffinityType affinity_type,
      ThreadAffinityParamType affinity_param)
      : TrafficPatternTranslator(global, affinity_type, affinity_param),
        global_(global) {}

  absl::Status FillTrafficPattern(int participant,
                                  verbsmarks::proto::PerFollowerTrafficPattern*
                                      per_follower_traffic) override;

 private:
  const verbsmarks::proto::GlobalTrafficPattern global_;
};

// Handles incast traffic pattern.
class IncastTrafficPatternTranslator : public TrafficPatternTranslator {
 public:
  explicit IncastTrafficPatternTranslator(
      const verbsmarks::proto::GlobalTrafficPattern& global,
      proto::ThreadAffinityType affinity_type,
      ThreadAffinityParamType affinity_param);

  absl::Status FillTrafficPattern(int participant,
                                  verbsmarks::proto::PerFollowerTrafficPattern*
                                      per_follower_traffic) override;

 private:
  const verbsmarks::proto::GlobalTrafficPattern global_;
  // Follower IDs of the sinks in this traffic.
  std::vector<int32_t> sinks_;
  // Follower IDs of the sources in this traffic.
  absl::flat_hash_set<int32_t> sources_;
  // Follower IDs of all the participants.
  absl::flat_hash_set<int32_t> participants_;
  // QP IDs indexed by initiator_id, target_id pair.
  absl::flat_hash_map<std::pair<int32_t, int32_t>, int32_t> qp_ids_;
};

// Handles uniform random traffic pattern. Builds a set of initiators and
// targets upon construction, and generates QueuePair IDs for them.
// FillTrafficPattern adds peers and QueuePair configs using this information.
class UniformRandomTrafficPatternTranslator : public TrafficPatternTranslator {
 public:
  explicit UniformRandomTrafficPatternTranslator(
      const verbsmarks::proto::GlobalTrafficPattern& global,
      proto::ThreadAffinityType affinity_type,
      ThreadAffinityParamType affinity_param);

  absl::Status FillTrafficPattern(int participant,
                                  verbsmarks::proto::PerFollowerTrafficPattern*
                                      per_follower_traffic) override;

 private:
  const verbsmarks::proto::GlobalTrafficPattern global_;
  // Follower IDs of initiators in this traffic pattern.
  absl::flat_hash_set<int32_t> initiators_;
  // Follower IDs of targets in this traffic pattern.
  absl::flat_hash_set<int32_t> targets_;
  // QP IDs indexed by initiator_id, target_id pair.
  absl::flat_hash_map<std::pair<int32_t, int32_t>, absl::flat_hash_set<int32_t>>
      qp_ids_;
};

}  // namespace verbsmarks

#endif  // VERBSMARKS_TRAFFIC_PATTERN_TRANSLATOR_H_
