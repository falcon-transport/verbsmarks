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

#include "traffic_pattern_translator.h"

#include <sys/types.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <utility>
#include <variant>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "utils.h"
#include "verbsmarks.pb.h"

namespace verbsmarks {

namespace {
// `proto::Participants` field allow specifying 1) all, 2) any_subset(N), or 3)
// list of specific participants. This function translates proto::Participants
// all or any_subset into specific lists of followers so the remaining code can
// assume list of specific participants.
absl::Status TranslateParticipants(const proto::ParticipantList& followers,
                                   proto::Participants* participants) {
  // Translate participants expressions into specific_followers.
  if (participants->has_all()) {
    for (const int32_t follower_id : followers.participant()) {
      participants->mutable_specific_followers()->add_participant(follower_id);
    }
  }
  if (participants->has_any_subset()) {
    int subset_num = participants->any_subset();
    LOG(INFO) << "subset: " << subset_num
              << " in group size: " << followers.participant_size();
    if (subset_num > followers.participant_size()) {
      return absl::InvalidArgumentError("group size smaller than any_subset");
    }
    auto* specific_followers = participants->mutable_specific_followers();
    //
    for (int i = 0; i < subset_num; ++i) {
      specific_followers->add_participant(followers.participant(i));
    }
  }
  return absl::OkStatus();
}

void TrafficCharacteristicsToQueuePair(
    const proto::TrafficCharacteristics& traffic_characteristics,
    proto::QueuePairConfig* queue_pair) {
  queue_pair->set_traffic_class(
      static_cast<uint8_t>(traffic_characteristics.traffic_class()));

  queue_pair->set_min_rnr_timer(traffic_characteristics.min_rnr_timer());
  queue_pair->set_timeout(traffic_characteristics.timeout());
}

}  // namespace

verbsmarks::proto::ParticipantList TrafficPatternTranslator::GetParticipants() {
  return global_.participants().specific_followers();
}

absl::StatusOr<std::unique_ptr<TrafficPatternTranslator>>
TrafficPatternTranslator::Build(verbsmarks::proto::GlobalTrafficPattern global,
                                const proto::ParticipantList& all_followers,
                                const proto::ThreadAffinityType affinity_type,
                                ThreadAffinityParamType affinity_param) {
  if (absl::Status status =
          TranslateParticipants(all_followers, global.mutable_participants());
      !status.ok()) {
    return status;
  }
  if (global.participants().specific_followers().participant().empty()) {
    return absl::InvalidArgumentError("Traffic does not have any participant");
  }
  // If specific initiators are requested for ops, make sure they are valid
  // participants.
  absl::flat_hash_set<int> participatns_set;
  for (const auto& follower :
       global.participants().specific_followers().participant()) {
    participatns_set.insert(follower);
  }
  for (const auto& op_ratio : global.traffic_characteristics().op_ratio()) {
    for (const auto& initiator : op_ratio.initiators()) {
      if (!participatns_set.contains(initiator)) {
        return absl::InvalidArgumentError(absl::StrCat(
            "Initiator requested in ", op_ratio,
            " not found in participants: ", global.participants()));
      }
    }
  }

  // Make appropriate translator.
  if (global.has_explicit_traffic()) {
    return std::make_unique<ExplicitTrafficPatternTranslator>(
        global, affinity_type, affinity_param);
  }
  if (global.has_pingpong_traffic()) {
    return std::make_unique<PingPongTrafficPatternTranslator>(
        global, affinity_type, affinity_param);
  }
  if (global.has_bandwidth_traffic()) {
    return std::make_unique<BandwidthTrafficPatternTranslator>(
        global, affinity_type, affinity_param);
  }
  if (global.has_incast_traffic()) {
    // Before starting the translator, translate participants into specific
    // list.
    if (absl::Status status = TranslateParticipants(
            global.participants().specific_followers(),
            global.mutable_incast_traffic()->mutable_sources());
        !status.ok()) {
      return status;
    }
    return std::make_unique<IncastTrafficPatternTranslator>(
        global, affinity_type, affinity_param);
  }
  if (global.has_uniform_random_traffic()) {
    // If initiators and targets are not provided, all of them are selected.
    if (!global.uniform_random_traffic().has_initiators()) {
      global.mutable_uniform_random_traffic()->mutable_initiators()->set_all(
          true);
    }
    if (!global.uniform_random_traffic().has_targets()) {
      global.mutable_uniform_random_traffic()->mutable_targets()->set_all(true);
    }
    // Before starting the translator, translate participants into specific
    // list.
    if (absl::Status status = TranslateParticipants(
            global.participants().specific_followers(),
            global.mutable_uniform_random_traffic()->mutable_initiators());
        !status.ok()) {
      return status;
    }
    if (absl::Status status = TranslateParticipants(
            global.participants().specific_followers(),
            global.mutable_uniform_random_traffic()->mutable_targets());
        !status.ok()) {
      return status;
    }
    return std::make_unique<UniformRandomTrafficPatternTranslator>(
        global, affinity_type, affinity_param);
  }

  return absl::InvalidArgumentError("Unsupported translator requested");
}

TrafficPatternTranslator::TrafficPatternTranslator(
    const verbsmarks::proto::GlobalTrafficPattern& global,
    const proto::ThreadAffinityType affinity_type,
    ThreadAffinityParamType affinity_param)
    : global_(global),
      affinity_type_(affinity_type),
      affinity_param_(affinity_param) {
  // Now everything is translated into specific_followers. Put them in a set for
  // easy access.
  for (const int32_t participant :
       global.participants().specific_followers().participant()) {
    participants_.insert(participant);
  }
}

absl::Status TrafficPatternTranslator::AddToResponse(
    int participant, verbsmarks::proto::ReadyResponse* response) {
  if (response == nullptr) {
    return absl::InvalidArgumentError(
        "The result holder should not be nullptr");
  }
  for (const auto& pattern : response->per_follower_traffic_patterns()) {
    if (pattern.global_traffic_pattern_id() == global_.global_traffic_id()) {
      return absl::InvalidArgumentError("pattern already added");
    }
  }
  if (!participants_.contains(participant)) {
    LOG(INFO) << "Participant " << participant
              << " doesn't belong to the traffic pattern.";
    // if this follower doesn't belong to the global traffic,
    // nothing to do.
    return absl::OkStatus();
  }

  // Sanity check for traffic characteristics.
  if (global_.has_explicit_traffic() &&
      global_.explicit_traffic().flows_size() == 0) {
    return absl::InvalidArgumentError(
        "required field flows within explicit_traffic is missing.");
  }
  proto::TrafficCharacteristics traffic_characteristics =
      global_.traffic_characteristics();
  if (traffic_characteristics.op_ratio().empty()) {
    return absl::InvalidArgumentError("required field op_ratio missing");
  }
  if (traffic_characteristics.message_size_distribution_type() ==
      proto::MESSAGE_SIZE_DISTRIBUTION_UNKNOWN) {
    return absl::InvalidArgumentError(
        "required field message_size_distribution_type unspecified");
  }
  proto::SizeDistributionParams* message_size_dist =
      traffic_characteristics.mutable_size_distribution_params_in_bytes();
  if (traffic_characteristics.message_size_distribution_type() ==
      proto::MESSAGE_SIZE_DISTRIBUTION_BY_OPS) {
    double sum = 0;
    double total_ratio = 0;
    double max = 0;
    double min = std::numeric_limits<double>::max();
    for (const auto& op_ratio : traffic_characteristics.op_ratio()) {
      // Only count what the follower can send.
      bool can_send = false;
      if (op_ratio.initiators().empty()) {
        can_send = true;
      } else {
        for (const auto& initiator : op_ratio.initiators()) {
          if (initiator == participant) {
            can_send = true;
            break;
          }
        }
      }
      if (!can_send) {
        continue;
      }
      // This ratio is the ratio of issuing this op. Not the ratio in the total
      // offered load.
      sum += (op_ratio.op_size() * op_ratio.ratio());
      if (op_ratio.op_size() > max) {
        max = op_ratio.op_size();
      }
      if (op_ratio.op_size() < min) {
        min = op_ratio.op_size();
      }
      total_ratio += op_ratio.ratio();
    }
    message_size_dist->set_min(min);
    message_size_dist->set_mean(sum / total_ratio);
    message_size_dist->set_max(max);
    for (auto& op_ratio : *traffic_characteristics.mutable_op_ratio()) {
      // Only count what the follower can send.
      bool can_send = false;
      if (op_ratio.initiators().empty()) {
        can_send = true;
      } else {
        for (const auto& initiator : op_ratio.initiators()) {
          if (initiator == participant) {
            can_send = true;
            break;
          }
        }
      }
      if (!can_send) {
        continue;
      }
      // The expected ratio of the load based on the op size and ratio to help
      // users to reason about the eventual throughput.
      op_ratio.set_expected_offered_load_ratio(
          sum / (op_ratio.ratio() * op_ratio.op_size()));
    }
  } else {
    // If max or min is unset, set them to mean.
    if (message_size_dist->max() == 0) {
      message_size_dist->set_max(message_size_dist->mean());
    }
    if (message_size_dist->min() == 0) {
      message_size_dist->set_min(message_size_dist->mean());
    }
  }
  if (message_size_dist->min() <= 0) {
    return absl::InvalidArgumentError(
        "min message size must be positive number");
  }
  if (!traffic_characteristics.has_delay_time()) {
    return absl::InvalidArgumentError("required field delay_time missing");
  }
  if (!traffic_characteristics.has_experiment_time()) {
    return absl::InvalidArgumentError("required field experiment_time missing");
  }
  if (utils::ProtoToDuration(traffic_characteristics.experiment_time()) <=
      absl::ZeroDuration()) {
    return absl::InvalidArgumentError("experiment_time must be positive");
  }
  if (!traffic_characteristics.has_buffer_time()) {
    return absl::InvalidArgumentError("required field buffer_time missing");
  }
  if (traffic_characteristics.batch_size() == 0) {
    return absl::InvalidArgumentError("required field batch_size missing");
  }

  // Allocate a new per follower traffic pattern, set common fields, and let the
  // specific translator fill the rest.
  auto* per_follower_traffic = response->add_per_follower_traffic_patterns();
  per_follower_traffic->set_global_traffic_pattern_id(
      global_.global_traffic_id());
  per_follower_traffic->set_follower_id(participant);
  *per_follower_traffic->mutable_traffic_characteristics() =
      traffic_characteristics;
  *per_follower_traffic->mutable_metrics_collection() =
      global_.metrics_collection();

  uint32_t max_outstanding = 0;
  switch (global_.load_parameters_case()) {
    case proto::GlobalTrafficPattern::LoadParametersCase::kOpenLoopParameters: {
      if (global_.open_loop_parameters().offered_load_gbps() == 0) {
        return absl::InvalidArgumentError(
            "open_loop_parameters required field offered_load_gbps is "
            "missing.");
      }
      if (global_.open_loop_parameters().arrival_time_distribution_type() ==
          proto::ARRIVAL_TIME_DISTRIBUTION_UNKNOWN) {
        return absl::InvalidArgumentError(
            "open_loop_parameters required field "
            "arrival_time_distribution_type is missing.");
      }
      // Calculate average interval from the load and message size and set it
      // for the follower.
      double avg_interval_sec =
          message_size_dist->mean() * 8 /
          (global_.open_loop_parameters().offered_load_gbps() * 1e9);
      utils::DurationToProto(
          absl::Seconds(avg_interval_sec),
          *per_follower_traffic->mutable_open_loop_parameters()
               ->mutable_average_interval());
      per_follower_traffic->mutable_open_loop_parameters()
          ->set_arrival_time_distribution_type(
              global_.open_loop_parameters().arrival_time_distribution_type());
      // For open loop, use max_outstanding of max_queue_depth, unless override
      // queue depth is requested. The leader sets to 0 and the follower will
      // read the attribute from the HW to use.
      if (global_.override_queue_depth() > 0) {
        max_outstanding = global_.override_queue_depth();
      }
      break;
    }
    case proto::GlobalTrafficPattern::LoadParametersCase::
        kClosedLoopMaxOutstanding:
      if (global_.closed_loop_max_outstanding() < 0) {
        return absl::InvalidArgumentError(absl::StrCat(
            "closed_loop_max_outstanding must be positive. Value provided: ",
            global_.closed_loop_max_outstanding()));
      }
      per_follower_traffic->set_closed_loop_max_outstanding(
          global_.closed_loop_max_outstanding());
      // Each queue pair's max outstanding will equal to the overall.
      max_outstanding = global_.closed_loop_max_outstanding();
      break;
    case proto::GlobalTrafficPattern::LOAD_PARAMETERS_NOT_SET:
      return absl::InvalidArgumentError(
          "required field load_parameters missing");
  }

  // Unless modified by FillTrafficPattern, number_to_post_at_once is 1 and per
  // follower initial delay is 0.
  per_follower_traffic->set_number_to_post_at_once(1);
  utils::DurationToProto(
      absl::ZeroDuration(),
      *per_follower_traffic->mutable_initial_delay_before_posting());

  // If needed, provide cpu affinity information.
  auto* affinity_info = per_follower_traffic->mutable_thread_affinity_info();
  switch (affinity_type_) {
    case proto::THREAD_AFFINITY_PER_TRAFFIC_PATTERN:
      affinity_info->set_cpu_to_pin_poller(global_.global_traffic_id());
      affinity_info->set_cpu_to_pin_poster(global_.global_traffic_id());
      affinity_info->set_halt_if_fail_to_pin(false);
      break;
    case proto::THREAD_AFFINITY_PER_TRAFFIC_PATTERN_STRICT:
      affinity_info->set_cpu_to_pin_poller(global_.global_traffic_id());
      affinity_info->set_cpu_to_pin_poster(global_.global_traffic_id());
      affinity_info->set_halt_if_fail_to_pin(true);
      break;
    case proto::THREAD_AFFINITY_TRAFFIC_PATTERN_SEPARATE:
      affinity_info->set_cpu_to_pin_poller(global_.global_traffic_id() * 2);
      affinity_info->set_cpu_to_pin_poster(global_.global_traffic_id() * 2 + 1);
      affinity_info->set_halt_if_fail_to_pin(false);
      break;
    case proto::THREAD_AFFINITY_TRAFFIC_PATTERN_SEPARATE_STRICT:
      affinity_info->set_cpu_to_pin_poller(global_.global_traffic_id() * 2);
      affinity_info->set_cpu_to_pin_poster(global_.global_traffic_id() * 2 + 1);
      affinity_info->set_halt_if_fail_to_pin(true);
      break;
    case proto::THREAD_AFFINITY_PER_FOLLOWER_AND_TRAFFIC_PATTERN: {
      if (auto round_robin_affinity =
              std::get_if<proto::ThreadAffinityParameter>(&affinity_param_)) {
        if (!round_robin_affinity->follower_id_to_cpu_min().contains(
                participant) ||
            !round_robin_affinity->follower_id_to_cpu_max().contains(
                participant)) {
          LOG(INFO) << *round_robin_affinity << "does not contain "
                    << participant;
          return absl::InvalidArgumentError(
              "THREAD_AFFINITY_PER_FOLLOWER_AND_TRAFFIC_PATTERN need follower "
              "IDs and CPU min / max in the parameter maps");
        }
        int min =
            round_robin_affinity->follower_id_to_cpu_min().at(participant);
        int max =
            round_robin_affinity->follower_id_to_cpu_max().at(participant);
        if (min > max) {
          return absl::InvalidArgumentError(
              "Config Error, max cpu num needs to be bigger than min cpu num");
        }
        int pos = global_.global_traffic_id() % (max + 1 - min);
        int pin = pos + min;
        affinity_info->set_cpu_to_pin_poller(pin);
        affinity_info->set_cpu_to_pin_poster(pin);
        affinity_info->set_halt_if_fail_to_pin(true);
      } else {
        return absl::InvalidArgumentError(
            "No ThreadAffinityParameter provided for "
            "THREAD_AFFINITY_PER_FOLLOWER_AND_TRAFFIC_PATTERN");
      }
    } break;
    case proto::THREAD_AFFINITY_EXPLICIT: {
      if (auto explicit_appinity =
              std::get_if<proto::ExplicitThreadAffinityParameter>(
                  &affinity_param_)) {
        for (const auto& explicit_affinity_config :
             explicit_appinity->explicit_thread_affinity_configs()) {
          if (explicit_affinity_config.follower_id() == participant &&
              explicit_affinity_config.traffic_pattern_id() ==
                  global_.global_traffic_id()) {
            affinity_info->set_cpu_to_pin_poller(
                explicit_affinity_config.cpu_core_id());
            affinity_info->set_cpu_to_pin_poster(
                explicit_affinity_config.cpu_core_id());
            affinity_info->set_halt_if_fail_to_pin(true);
          }
        }
      } else {
        return absl::InvalidArgumentError(
            "No ThreadAffinityParameter provided for THREAD_AFFINITY_EXPLICIT");
      }
    } break;
    default:
      per_follower_traffic->clear_thread_affinity_info();
  }

  if (auto status = FillTrafficPattern(participant, per_follower_traffic);
      !status.ok()) {
    return status;
  }

  if (max_outstanding > 0 &&
      max_outstanding < traffic_characteristics.batch_size()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "batch size cannot be larger than max outstanding, batch size = ",
        traffic_characteristics.batch_size(),
        " max outstanding = ", max_outstanding));
  }

  for (int i = 0; i < per_follower_traffic->queue_pairs_size(); ++i) {
    auto* queue_pair = per_follower_traffic->mutable_queue_pairs(i);
    queue_pair->set_max_outstanding_ops(max_outstanding);
    queue_pair->set_max_op_size(message_size_dist->max());
    queue_pair->set_populate_op_buffers(global_.populate_op_buffers());
    queue_pair->set_num_prepost_receive_ops(
        per_follower_traffic->traffic_characteristics()
            .num_prepost_receive_ops());
    queue_pair->set_validate_op_buffers(global_.validate_op_buffers());
    queue_pair->set_populate_op_buffers(global_.validate_op_buffers());
    if (global_.override_queue_depth() > 0) {
      queue_pair->set_override_queue_depth(global_.override_queue_depth());
    }
    *queue_pair->mutable_metrics_collection() = global_.metrics_collection();
  }
  return absl::OkStatus();
}

absl::Status ExplicitTrafficPatternTranslator::FillTrafficPattern(
    int participant,
    verbsmarks::proto::PerFollowerTrafficPattern* per_follower_traffic) {
  per_follower_traffic->set_traffic_type(proto::TRAFFIC_TYPE_EXPLICIT);
  *per_follower_traffic->mutable_explicit_traffic() =
      global_.explicit_traffic();

  per_follower_traffic->set_completion_queue_policy(
      global_.completion_queue_policy());

  auto& traffic_characteristics = global_.traffic_characteristics();
  uint32_t queue_pair_id = 0;
  for (int i = 0; i < global_.explicit_traffic().flows_size(); ++i) {
    const proto::ExplicitTraffic::Flow& flow =
        global_.explicit_traffic().flows().at(i);
    //
    int repeat = 1;
    if (flow.repeat() > 0) {
      repeat = flow.repeat();
    }
    for (int j = 0; j < repeat; ++j) {
      int32_t peer;
      if (flow.initiator() == participant) {
        peer = flow.target();
      } else if (flow.target() == participant) {
        peer = flow.initiator();
      } else {
        queue_pair_id++;
        continue;  // This participant is not a part of this flow.
      }

      per_follower_traffic->mutable_peers()->add_participant(peer);
      proto::QueuePairConfig* queue_pair =
          per_follower_traffic->add_queue_pairs();
      queue_pair->set_connection_type(global_.connection_type());
      queue_pair->set_peer(peer);
      queue_pair->set_queue_pair_id(queue_pair_id++);
      TrafficCharacteristicsToQueuePair(traffic_characteristics, queue_pair);
      if (flow.initiator() == participant ||
          global_.explicit_traffic().bidirectional()) {
        queue_pair->set_is_initiator(true);
      }
      if (flow.target() == participant ||
          global_.explicit_traffic().bidirectional()) {
        queue_pair->set_is_target(true);
      }
    }
  }

  return absl::OkStatus();
}

absl::Status PingPongTrafficPatternTranslator::FillTrafficPattern(
    int participant,
    verbsmarks::proto::PerFollowerTrafficPattern* per_follower_traffic) {
  const verbsmarks::proto::PingPongTraffic traffic = global_.pingpong_traffic();
  per_follower_traffic->set_traffic_type(proto::TRAFFIC_TYPE_PINGPONG);
  *per_follower_traffic->mutable_pingpong_traffic() = traffic;
  auto& traffic_characteristics = global_.traffic_characteristics();

  int32_t peer;
  if (traffic.initiator() == participant) {
    peer = traffic.target();
    per_follower_traffic->set_is_pingpong_initiator(true);
  } else if (traffic.target() == participant) {
    peer = traffic.initiator();
    per_follower_traffic->set_is_pingpong_initiator(false);
  } else {
    // This participant is not a part of this flow.
    return absl::OkStatus();
  }
  if (traffic.iterations() == 0) {
    return absl::InvalidArgumentError("Pingpong iterations are required");
  }

  if (global_.completion_queue_policy() !=
          proto::CompletionQueuePolicy::COMPLETION_QUEUE_POLICY_UNSPECIFIED &&
      global_.completion_queue_policy() !=
          proto::CompletionQueuePolicy::COMPLETION_QUEUE_POLICY_SHARED_CQ) {
    // The pingpong workload only utilizes the first QP, so CQ policies
    // which potentially allocate multiple CQs are not valid.
    // Reject CQ policies other than unspecified or shared single CQ.
    return absl::InvalidArgumentError(
        "Incompatible completion queue policy for pingpong traffic.");
  }

  per_follower_traffic->set_completion_queue_policy(
      proto::CompletionQueuePolicy::COMPLETION_QUEUE_POLICY_SHARED_CQ);

  per_follower_traffic->set_use_event(traffic.use_event());

  per_follower_traffic->set_iterations(traffic.iterations());
  // By default, initiators start posting after 2 seconds.
  //
  per_follower_traffic->mutable_initial_delay_before_posting()->set_seconds(2);

  per_follower_traffic->mutable_peers()->add_participant(peer);
  proto::QueuePairConfig* queue_pair = per_follower_traffic->add_queue_pairs();
  queue_pair->set_connection_type(global_.connection_type());
  queue_pair->set_peer(peer);
  queue_pair->set_queue_pair_id(0);
  TrafficCharacteristicsToQueuePair(traffic_characteristics, queue_pair);
  // Both ends should be able to send and receive.
  queue_pair->set_is_initiator(true);
  queue_pair->set_is_target(true);

  return absl::OkStatus();
}

absl::Status BandwidthTrafficPatternTranslator::FillTrafficPattern(
    int participant,
    verbsmarks::proto::PerFollowerTrafficPattern* per_follower_traffic) {
  const verbsmarks::proto::BandwidthTraffic traffic =
      global_.bandwidth_traffic();
  per_follower_traffic->set_traffic_type(proto::TRAFFIC_TYPE_BANDWIDTH);
  *per_follower_traffic->mutable_bandwidth_traffic() = traffic;
  auto& traffic_characteristics = global_.traffic_characteristics();

  int32_t peer;
  if (traffic.initiator() == participant) {
    peer = traffic.target();
  } else {
    peer = traffic.initiator();
  }

  if (global_.completion_queue_policy() !=
          proto::CompletionQueuePolicy::COMPLETION_QUEUE_POLICY_UNSPECIFIED &&
      global_.completion_queue_policy() !=
          proto::CompletionQueuePolicy::COMPLETION_QUEUE_POLICY_SHARED_CQ) {
    // The bandwidth workload only utilizes the first QP, so CQ policies
    // which potentially allocate multiple CQs are not valid.
    // Reject CQ policies other than unspecified or shared single CQ.
    return absl::InvalidArgumentError(
        "Incompatible completion queue policy for bandwidth traffic.");
  }

  per_follower_traffic->set_completion_queue_policy(
      proto::CompletionQueuePolicy::COMPLETION_QUEUE_POLICY_SHARED_CQ);

  per_follower_traffic->mutable_peers()->add_participant(peer);
  proto::QueuePairConfig* queue_pair = per_follower_traffic->add_queue_pairs();
  queue_pair->set_connection_type(global_.connection_type());
  queue_pair->set_peer(peer);
  queue_pair->set_queue_pair_id(0);
  TrafficCharacteristicsToQueuePair(traffic_characteristics, queue_pair);
  if (traffic.initiator() == participant || traffic.bidirectional()) {
    queue_pair->set_is_initiator(true);
  }
  if (traffic.target() == participant || traffic.bidirectional()) {
    queue_pair->set_is_target(true);
  }

  per_follower_traffic->set_iterations(traffic.iterations());
  return absl::OkStatus();
}

UniformRandomTrafficPatternTranslator::UniformRandomTrafficPatternTranslator(
    const verbsmarks::proto::GlobalTrafficPattern& global,
    const proto::ThreadAffinityType affinity_type,
    ThreadAffinityParamType affinity_param)
    : TrafficPatternTranslator(global, affinity_type, affinity_param),
      global_(global) {
  // Find initiators and targets.
  for (int initiator : global_.uniform_random_traffic()
                           .initiators()
                           .specific_followers()
                           .participant()) {
    initiators_.insert(initiator);
  }
  for (int target : global_.uniform_random_traffic()
                        .targets()
                        .specific_followers()
                        .participant()) {
    targets_.insert(target);
  }

  // Figure out number of QPs to use per <initiator, target> pair.
  uint32_t qps_per_flow = global_.uniform_random_traffic().qps_per_flow();
  if (qps_per_flow == 0) {
    // default to 1 if not qps_per_flow is not set
    qps_per_flow = 1;
  }

  // Generate QueuePair IDs and saves them, indexed by <initiator, target>.
  int qp_id = 0;
  for (int initiator : initiators_) {
    for (int target : targets_) {
      auto src_dst_pair = std::make_pair(initiator, target);
      if (initiator != target) {
        auto dst_src_pair = std::make_pair(target, initiator);
        if (qp_ids_.contains(dst_src_pair)) {
          // When both pairs are initiator and target at the same time, reuse
          // the QP ID.
          qp_ids_[src_dst_pair].insert(qp_ids_[dst_src_pair].begin(),
                                       qp_ids_[dst_src_pair].end());
          continue;
        }
        for (int i = 0; i < qps_per_flow; ++i) {
          qp_ids_[src_dst_pair].insert(qp_id);
          ++qp_id;
        }
      }
    }
  }
}

absl::Status UniformRandomTrafficPatternTranslator::FillTrafficPattern(
    int participant,
    verbsmarks::proto::PerFollowerTrafficPattern* per_follower_traffic) {
  per_follower_traffic->set_traffic_type(proto::TRAFFIC_TYPE_UNIFORM_RANDOM);
  *per_follower_traffic->mutable_uniform_random_traffic() =
      global_.uniform_random_traffic();
  auto& traffic_characteristics = global_.traffic_characteristics();

  per_follower_traffic->set_completion_queue_policy(
      global_.completion_queue_policy());

  // If participant is in initiator, create queue pairs for all the targets.
  if (initiators_.contains(participant)) {
    for (int target : targets_) {
      if (target == participant) {
        // skip oneself.
        continue;
      }
      per_follower_traffic->mutable_peers()->add_participant(target);
      for (int qp_id : qp_ids_[std::make_pair(participant, target)]) {
        proto::QueuePairConfig* queue_pair =
            per_follower_traffic->add_queue_pairs();
        queue_pair->set_connection_type(global_.connection_type());
        queue_pair->set_peer(target);
        queue_pair->set_queue_pair_id(qp_id);
        TrafficCharacteristicsToQueuePair(traffic_characteristics, queue_pair);
        queue_pair->set_is_initiator(true);
        // If the participant is also in the target, it should be target as
        // well. Otherwise, it should not be a target.
        queue_pair->set_is_target(initiators_.contains(target));
      }
    }
    return absl::OkStatus();
  }

  // The participant is not an initiator. If participant is in the target set,
  // create queue pairs for all the initiators.
  if (targets_.contains(participant)) {
    for (int initiator : initiators_) {
      if (initiator == participant) {
        // skip oneself.
        continue;
      }
      per_follower_traffic->mutable_peers()->add_participant(initiator);
      for (int qp_id : qp_ids_[std::make_pair(initiator, participant)]) {
        proto::QueuePairConfig* queue_pair =
            per_follower_traffic->add_queue_pairs();
        queue_pair->set_connection_type(global_.connection_type());
        queue_pair->set_peer(initiator);
        queue_pair->set_queue_pair_id(qp_id);
        queue_pair->set_is_initiator(false);
        queue_pair->set_is_target(true);
        TrafficCharacteristicsToQueuePair(traffic_characteristics, queue_pair);
      }
    }
  }

  return absl::OkStatus();
}

IncastTrafficPatternTranslator::IncastTrafficPatternTranslator(
    const verbsmarks::proto::GlobalTrafficPattern& global,
    const proto::ThreadAffinityType affinity_type,
    ThreadAffinityParamType affinity_param)
    : TrafficPatternTranslator(global, affinity_type, affinity_param),
      global_(global) {
  // Set sources and sinks.
  for (int source :
       global_.incast_traffic().sources().specific_followers().participant()) {
    sources_.insert(source);
    participants_.insert(source);
  }
  for (int sink : global_.incast_traffic().sinks()) {
    sinks_.push_back(sink);
    participants_.insert(sink);
  }
  // Generate QueuePair IDs between all the participants and saves them
  // indexed by <initiator, target>.
  int qp_id = 0;
  for (int initiator : participants_) {
    for (int target : participants_) {
      if (initiator != target) {
        if (qp_ids_.contains(std::make_pair(target, initiator))) {
          // If QP is created for the pair, reuse the QP ID.
          qp_ids_[std::make_pair(initiator, target)] =
              qp_ids_[std::make_pair(target, initiator)];
          continue;
        }
        qp_ids_[std::make_pair(initiator, target)] = qp_id;
        ++qp_id;
      }
    }
  }
}

absl::Status IncastTrafficPatternTranslator::FillTrafficPattern(
    int participant,
    verbsmarks::proto::PerFollowerTrafficPattern* per_follower_traffic) {
  if (sinks_.empty()) {
    return absl::InvalidArgumentError("Sink can't be empty in incast traffic");
  }
  if (sources_.empty()) {
    return absl::InvalidArgumentError(
        "Source can't be empty in incast traffic");
  }
  if (global_.incast_traffic().degree() < 1) {
    return absl::InvalidArgumentError("Incast degree must be positive");
  }

  *per_follower_traffic->mutable_incast_traffic() = global_.incast_traffic();
  auto& traffic_characteristics = global_.traffic_characteristics();
  if (traffic_characteristics.op_ratio_size() != 1) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Incast traffic should have exactly one op type per traffic "
        "pattern, but has ",
        traffic_characteristics.op_ratio_size()));
  }

  per_follower_traffic->set_completion_queue_policy(
      global_.completion_queue_policy());

  bool is_initiator = false;
  bool is_target = false;
  auto find_sink_index = [](absl::Span<const int32_t> sinks,
                            const int32_t participant) {
    int i = 0;
    for (auto s : sinks) {
      if (s == participant) {
        return i;
      }
      ++i;
    }
    return -1;
  };
  int index = find_sink_index(sinks_, participant);
  if (traffic_characteristics.op_ratio(0).op_code() == proto::RDMA_OP_READ) {
    // In READ, every participants can be a target.
    is_target = true;
    if (index >= 0) {
      // In READ, the sinks are the initiators.
      is_initiator = true;
    }
  }

  // Create queue pairs for all other targets.
  if (participants_.contains(participant)) {
    for (int target : participants_) {
      if (target == participant) {
        // skip oneself.
        continue;
      }
      per_follower_traffic->mutable_peers()->add_participant(target);
      proto::QueuePairConfig* queue_pair =
          per_follower_traffic->add_queue_pairs();
      queue_pair->set_connection_type(global_.connection_type());
      queue_pair->set_peer(target);
      queue_pair->set_queue_pair_id(
          qp_ids_[std::make_pair(participant, target)]);
      queue_pair->set_is_initiator(is_initiator);
      queue_pair->set_is_target(is_target);
      TrafficCharacteristicsToQueuePair(traffic_characteristics, queue_pair);
    }
  }
  if (traffic_characteristics.op_ratio(0).op_code() == proto::RDMA_OP_READ) {
    per_follower_traffic->set_traffic_type(proto::TRAFFIC_TYPE_UNIFORM_RANDOM);
    if (!is_initiator) {
      // This follower is not a sink and won't post. We're all done.
      return absl::OkStatus();
    }
    // For READ, we use extended delay and per-follower-delay.
    auto interval_between_burst = utils::ProtoToDuration(
        per_follower_traffic->open_loop_parameters().average_interval());
    absl::Duration initial_delay_for_sink = index * interval_between_burst;
    // Modify the interval and initial delay.
    utils::DurationToProto(interval_between_burst * sinks_.size(),
                           *per_follower_traffic->mutable_open_loop_parameters()
                                ->mutable_average_interval());
    utils::DurationToProto(
        initial_delay_for_sink,
        *per_follower_traffic->mutable_initial_delay_before_posting());
    // The load generator should post `incast degree` number post operations
    // at once.
    per_follower_traffic->set_number_to_post_at_once(
        global_.incast_traffic().degree());
    return absl::OkStatus();
  }

  return absl::UnimplementedError(absl::StrCat(
      "Incast for operation ", traffic_characteristics.op_ratio(0).op_code(),
      " not implemented"));
}

}  // namespace verbsmarks
