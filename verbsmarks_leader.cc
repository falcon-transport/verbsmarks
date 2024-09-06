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

#include "verbsmarks_leader.h"

#include <cstdint>
#include <fstream>
#include <functional>
#include <ios>
#include <list>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/container/btree_map.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/flags/flag.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "google/protobuf/text_format.h"
#include "google/rpc/code.pb.h"
#include "google/rpc/status.pb.h"
#include "traffic_pattern_translator.h"
#include "utils.h"
#include "verbsmarks.pb.h"
#include "verbsmarks_binary_flags.h"

namespace verbsmarks {

using ::google::rpc::Code;

namespace {
constexpr char kTrafficGenerationFailureStatusPrefix[] =
    "Quorum formation halted because VerbsMarksLeaderImpl failed to generate "
    "per-follower traffic pattern: ";
}

void FollowerHealthTracker::BeginTracking(
    const absl::flat_hash_set<int32_t>& follower_ids) {
  follower_set_.clear();
  follower_heartbeats_received_.clear();

  LOG(INFO) << "Begin tracking follower heartbeats with follower count "
            << follower_ids.size();

  follower_set_.insert(follower_ids.cbegin(), follower_ids.cend());
  heartbeat_interval_start_ = absl::Now();
  tracking_started_ = true;
  experiment_healthy_ = true;
}

void FollowerHealthTracker::RegisterHeartbeat(int32_t follower_id,
                                              bool is_healthy) {
  if (!follower_set_.contains(follower_id)) {
    LOG(INFO) << "Received heartbeat from un-tracked follower " << follower_id;
    return;
  }

  if (!is_healthy) {
    LOG(WARNING) << "Experiment will end due to error heartbeat from follower "
                 << follower_id;
    experiment_healthy_ = false;
  }

  follower_heartbeats_received_.insert(follower_id);
}

void FollowerHealthTracker::RegisterFinished(int32_t follower_id) {
  if (!follower_set_.contains(follower_id)) {
    LOG(INFO) << "Received finished status from un-tracked follower "
              << follower_id;
    return;
  }

  // Remove follower from heartbeat monitoring sets.
  follower_set_.erase(follower_id);
  follower_heartbeats_received_.erase(follower_id);
}

bool FollowerHealthTracker::ExperimentIsHealthy() {
  if (!tracking_started_) {
    // FollowerHealth won't trigger abort unless tracking was started.
    return true;
  }

  if (!experiment_healthy_) {
    // Once unhealthy state is entered, no need to continue checking health.
    return false;
  }

  auto now = absl::Now();
  auto interval_end = heartbeat_interval_start_ + heartbeat_check_interval_;
  if (now >= interval_end) {
    if (follower_heartbeats_received_.size() < follower_set_.size()) {
      LOG(ERROR) << "Ending experiment due to missed heartbeat(s)";
      for (const auto& follower : follower_set_) {
        if (!follower_heartbeats_received_.contains(follower)) {
          LOG(ERROR) << "Didn't receive heartbeat from follower " << follower;
          LOG(ERROR) << "Check the corresponding follower logs for errors.";
        }
      }
      experiment_healthy_ = false;
    }
    heartbeat_interval_start_ = now;
    follower_heartbeats_received_.clear();
  }

  return experiment_healthy_;
}

void StatisticsCollector::CollectStatistics(
    const proto::CurrentStats& follower_stats, int follower_id) {
  for (const auto& traffic_stat : follower_stats.traffic_current_stats()) {
    for (const auto& qp_stat : traffic_stat.queue_pair_current_stats()) {
      for (const auto& per_op_stat : qp_stat.per_op_type_size_current_stats()) {
        auto key = MakeStatsKey(follower_id, traffic_stat.global_traffic_id(),
                                qp_stat.queue_pair_id(), per_op_stat.op_type(),
                                per_op_stat.op_size());

        absl::btree_map<int, proto::Statistics>& stats_list = stats_[key];
        for (const auto& stats_record :
             per_op_stat.stats_per_second().statistics()) {
          stats_list[stats_record.throughput().seconds_from_start()] =
              stats_record;
        }
      }
    }
  }
}

void StatisticsCollector::CollectFinalReportStatistics(
    proto::ResultReport* report) {
  // Take any per-second stats out of ResultReport and add into stats_.
  for (auto& follower : *report->mutable_follower_results()) {
    for (auto& traffic : *follower.mutable_per_traffic_results()) {
      for (auto& qp : *traffic.mutable_per_queue_pair_results()) {
        for (auto& per_op : *qp.mutable_per_op_type_size_results()) {
          auto key = MakeStatsKey(
              follower.follower_id(), traffic.global_traffic_id(),
              qp.queue_pair_id(), per_op.op_code(), per_op.op_size());

          absl::btree_map<int, proto::Statistics>& stats_list = stats_[key];
          for (const auto& stats_record :
               per_op.statistics_per_second().statistics()) {
            stats_list[stats_record.throughput().seconds_from_start()] =
                stats_record;
          }
          // Clear stats from the final report, since they will be saved into
          // a periodic result file or sorted and added to the final result
          // output.
          per_op.mutable_statistics_per_second()->clear_statistics();
        }
      }
    }
  }
}

void StatisticsCollector::WriteFinalReport(proto::ResultReport* report) {
  // Take any per-second stats out of ResultReport and add into stats_.
  // This is done to ensure the final output has all stats in real-time order.
  for (auto& follower : *report->mutable_follower_results()) {
    for (auto& traffic : *follower.mutable_per_traffic_results()) {
      for (auto& qp : *traffic.mutable_per_queue_pair_results()) {
        for (auto& per_op : *qp.mutable_per_op_type_size_results()) {
          auto key = MakeStatsKey(
              follower.follower_id(), traffic.global_traffic_id(),
              qp.queue_pair_id(), per_op.op_code(), per_op.op_size());

          absl::btree_map<int, proto::Statistics>& stats_list = stats_[key];
          // Clear existing stats in the result proto then fill sequentially
          // from the collected list.
          per_op.mutable_statistics_per_second()->clear_statistics();
          for (const auto& stats_record : stats_list) {
            *per_op.mutable_statistics_per_second()->add_statistics() =
                stats_record.second;
          }
        }
      }
    }
  }
}

std::optional<proto::PeriodicResultReport>
StatisticsCollector::WritePeriodicReport() {
  if (stats_.empty()) {
    // If no stats are available, return nullopt (rather than empty report) to
    // skip writing the output file.
    return std::nullopt;
  }

  proto::PeriodicResultReport report;
  proto::PeriodicResultReport::FollowerCurrentStats* follower = nullptr;
  proto::CurrentStats::TrafficCurrentStats* traffic = nullptr;
  proto::CurrentStats::QueuePairCurrentStats* queue_pair = nullptr;
  proto::CurrentStats::PerOpTypeSizeCurrentStats* op_type_size = nullptr;

  for (const auto& stats_entry : stats_) {
    auto key = stats_entry.first;
    auto per_second_list = stats_entry.second;

    int follower_id = std::get<0>(key);
    int traffic_id = std::get<1>(key);
    int qp_id = std::get<2>(key);
    proto::RdmaOp op_type = std::get<3>(key);
    int op_size = std::get<4>(key);

    // Key tuple fields are in sorted order since stats_ is a btree.
    if (follower == nullptr || follower->follower_id() != follower_id) {
      follower = report.add_follower_current_stats();
      follower->set_follower_id(follower_id);
      traffic = nullptr;
      queue_pair = nullptr;
      op_type_size = nullptr;
    }
    if (traffic == nullptr || traffic->global_traffic_id() != traffic_id) {
      traffic = follower->mutable_current_stats()->add_traffic_current_stats();
      traffic->set_global_traffic_id(traffic_id);
      queue_pair = nullptr;
      op_type_size = nullptr;
    }
    if (queue_pair == nullptr || queue_pair->queue_pair_id() != qp_id) {
      queue_pair = traffic->add_queue_pair_current_stats();
      queue_pair->set_queue_pair_id(qp_id);
      op_type_size = nullptr;
    }
    if (op_type_size == nullptr || op_type_size->op_type() != op_type ||
        op_type_size->op_size() != op_size) {
      op_type_size = queue_pair->add_per_op_type_size_current_stats();
      op_type_size->set_op_type(op_type);
      op_type_size->set_op_size(op_size);
    }

    for (const auto& stats_record : per_second_list) {
      *op_type_size->mutable_stats_per_second()->add_statistics() =
          stats_record.second;
    }
  }

  // Stats are removed from storage after generating periodic output.
  stats_.clear();
  return report;
}

void VerbsMarksLeaderImpl::WaitForQuorum(absl::Time deadline) {
  // Waits for all followers of the quorum to join before processing them,
  // unless a timeout occurs.
  auto stop_waiting_cond = [this]() ABSL_SHARED_LOCKS_REQUIRED(quorum_mutex_) {
    // Wake up when either enough quorum requests have been received or we
    // want to abort.
    return quorum_queue_.size() == config_.group_size() ||
           quorum_formation_status_ == QuorumFormationStatus::kHalted;
  };
  quorum_mutex_.LockWhenWithDeadline(absl::Condition(&stop_waiting_cond),
                                     deadline);
  proto::QuorumResponse response;
  {
    absl::MutexLock lock(&traffic_pattern_mutex_);
    // Even though GetReady would fail because of traffic pattern translation
    // failure, return an error for quorum formation so that the failure
    // propagates are quickly as possible.
    if (traffic_pattern_generated_ && !traffic_pattern_status_.ok()) {
      response.mutable_status()->set_code(traffic_pattern_status_.raw_code());
      response.mutable_status()->set_message(
          absl::StrCat(kTrafficGenerationFailureStatusPrefix,
                       traffic_pattern_status_.message()));
      quorum_formation_status_ = QuorumFormationStatus::kFailed;
    }
  }
  if (quorum_formation_status_ != QuorumFormationStatus::kFailed) {
    if (quorum_queue_.size() == config_.group_size()) {
      // Enough quorum requests received.
      quorum_formation_status_ = QuorumFormationStatus::kFormed;
      LOG(INFO) << "Quorum reached successfully.";
      response.mutable_status()->set_code(Code::OK);
      int32_t next_follower_id = 0;
      for (auto& m : quorum_queue_) {
        auto* new_follower = response.add_followers();
        int32_t follower_id;
        if (config_.follower_alias_to_id().empty()) {
          // If the alias -> id map isn't provided, assign ids in the order in
          // which followers joined the quorum.
          follower_id = next_follower_id;
          ++next_follower_id;
        } else {
          // If the alias -> id map is provided, make sure there is a valid
          // entry for all followers, otherwise quorum formation fails.
          auto it =
              config_.follower_alias_to_id().find(m.request->follower_alias());
          if (it == config_.follower_alias_to_id().end()) {
            quorum_formation_status_ = QuorumFormationStatus::kFailed;
            std::string error_message = absl::StrCat(
                "Follower with alias ", m.request->follower_alias(),
                " does not have a valid entry in the follower_alias_to_id "
                "map: ",
                config_);
            LOG(ERROR) << error_message;
            response.mutable_status()->set_code(Code::INVALID_ARGUMENT);
            response.mutable_status()->set_message(error_message);
            break;
          }
          follower_id = it->second;
        }
        new_follower->set_follower_id(follower_id);
        new_follower->set_follower_alias(m.request->follower_alias());
        new_follower->set_http_server_address(m.request->http_server_address());
        followers_[follower_id] = *new_follower;
        // Add followers to the final report.
        *final_report_.add_followers() = *new_follower;
      }
    } else {
      quorum_formation_status_ = QuorumFormationStatus::kFailed;
      LOG(INFO) << "Failed to form a quorum in time limit.";
      response.mutable_status()->set_code(Code::DEADLINE_EXCEEDED);
      response.mutable_status()->set_message(absl::StrCat(
          "VerbsMarksLeaderImpl could not form a quorum because not enough "
          "quorum requests were received within the time limit. Expected ",
          config_.group_size(), " quorum requests, received ",
          quorum_queue_.size()));
    }
  }
  // Send the response to everyone.
  for (auto& m : quorum_queue_) {
    (*m.response_handler)(response);
  }
  // Do not proceed with the experiment if quorum formation fails.
  if (quorum_formation_status_ == QuorumFormationStatus::kFailed) {
    absl::MutexLock l(&experiment_mutex_);
    finished_.first = true;
    finished_.second = absl::InternalError(
        "Quorum formation error. Check follower logs to see what happened.");
  }
  quorum_mutex_.Unlock();
}

void VerbsMarksLeaderImpl::SeekQuorum(
    proto::QuorumRequest* request,
    std::unique_ptr<std::function<void(proto::QuorumResponse)>>
        response_handler) {
  {
    absl::MutexLock lock(&traffic_pattern_mutex_);
    // Fail quickly when traffic pattern translation fails.
    if (traffic_pattern_generated_ && !traffic_pattern_status_.ok()) {
      proto::QuorumResponse traffic_pattern_failed_response;
      traffic_pattern_failed_response.mutable_status()->set_code(
          traffic_pattern_status_.raw_code());
      traffic_pattern_failed_response.mutable_status()->set_message(
          absl::StrCat(kTrafficGenerationFailureStatusPrefix,
                       traffic_pattern_status_.message()));
      (*response_handler)(traffic_pattern_failed_response);
      return;
    }
  }

  absl::MutexLock lock(&quorum_mutex_);
  switch (quorum_formation_status_) {
    case QuorumFormationStatus::kForming:
      quorum_queue_.emplace_back(
          QuorumArrival{request, std::move(response_handler)});
      break;
    case QuorumFormationStatus::kFormed: {
      proto::QuorumResponse quorum_full_response;
      quorum_full_response.mutable_status()->set_code(Code::RESOURCE_EXHAUSTED);
      quorum_full_response.mutable_status()->set_message("Quorum already full");
      (*response_handler)(quorum_full_response);
      break;
    }
    case QuorumFormationStatus::kFailed:
    case QuorumFormationStatus::kHalted: {
      proto::QuorumResponse quorum_failed_response;
      quorum_failed_response.mutable_status()->set_code(Code::INTERNAL);
      quorum_failed_response.mutable_status()->set_message(
          "VerbsMarksLeaderImpl quorum formation has already failed.");
      (*response_handler)(quorum_failed_response);
      break;
    }
  }
}

void VerbsMarksLeaderImpl::GetReady(
    int32_t follower_id,
    std::unique_ptr<std::function<void(proto::ReadyResponse)>>
        response_handler) {
  {
    absl::MutexLock lock(&traffic_pattern_mutex_);
    // wait for traffic patterns ready.
    traffic_pattern_mutex_.Await(absl::Condition(&traffic_pattern_generated_));
  }

  proto::ReadyResponse response;
  {
    // If quorum not formed, return an error.
    absl::MutexLock quorum_lock(&quorum_mutex_);
    if (quorum_formation_status_ != QuorumFormationStatus::kFormed) {
      response.mutable_status()->set_code(Code::FAILED_PRECONDITION);
      if (quorum_formation_status_ == QuorumFormationStatus::kFailed) {
        response.mutable_status()->set_message("The quorum is not formed yet.");
      } else if (quorum_formation_status_ == QuorumFormationStatus::kFailed) {
        response.mutable_status()->set_message("Quorum formation failed.");
      }
      (*response_handler)(response);
      return;
    }
  }

  absl::MutexLock lock(&traffic_pattern_mutex_);
  response.mutable_status()->set_code(traffic_pattern_status_.raw_code());
  if (!traffic_pattern_status_.ok()) {
    response.mutable_status()->set_message(
        absl::StrCat(kTrafficGenerationFailureStatusPrefix,
                     traffic_pattern_status_.message()));
  } else if (!ready_responses_.contains(follower_id)) {
    response.mutable_status()->set_code(Code::INTERNAL);
    std::string error_message =
        absl::StrCat("A response for follower ", follower_id,
                     " was not found in ready responses. Ready responses:");
    for (const auto& it : ready_responses_) {
      absl::StrAppend(&error_message, "\n", it.first, ": ", it.second);
    }
    response.mutable_status()->set_message(error_message);

  } else {
    response = ready_responses_[follower_id];
  }
  (*response_handler)(response);
}

void VerbsMarksLeaderImpl::GeneratePerFollowerTraffic() {
  absl::MutexLock lock(&traffic_pattern_mutex_);
  // When the lock is release, per follower traffics are generated.
  traffic_pattern_generated_ = true;
  // Populate empty ready_responses for everyone.
  proto::ParticipantList all_followers;
  if (config_.follower_alias_to_id().empty()) {
    for (int i = 0; i < config_.group_size(); ++i) {
      ready_responses_[i] = proto::ReadyResponse{};
      if (config_.has_memory_resource_policy()) {
        *ready_responses_[i].mutable_memory_resource_policy() =
            config_.memory_resource_policy();
      }
      all_followers.add_participant(i);
    }
  } else {
    for (const auto& it : config_.follower_alias_to_id()) {
      ready_responses_[it.second] = proto::ReadyResponse{};
      if (config_.has_memory_resource_policy()) {
        *ready_responses_[it.second].mutable_memory_resource_policy() =
            config_.memory_resource_policy();
      }
      all_followers.add_participant(it.second);
    }
  }

  traffic_pattern_status_ = absl::OkStatus();
  for (auto& traffic_pattern : config_.traffic_patterns()) {
    if (config_.has_thread_affinity_param() &&
        config_.has_explicit_thread_affinity_param()) {
      LOG(ERROR) << "Mixed thread affinity config is not allowed.";
      return;
    }
    absl::StatusOr<std::unique_ptr<TrafficPatternTranslator>>
        status_or_translator;
    if (config_.has_thread_affinity_param()) {
      status_or_translator = TrafficPatternTranslator::Build(
          traffic_pattern, all_followers, config_.thread_affinity_type(),
          config_.thread_affinity_param());
    } else if (config_.has_explicit_thread_affinity_param()) {
      status_or_translator = TrafficPatternTranslator::Build(
          traffic_pattern, all_followers, config_.thread_affinity_type(),
          config_.explicit_thread_affinity_param());
    } else {
      status_or_translator = TrafficPatternTranslator::Build(
          traffic_pattern, all_followers, config_.thread_affinity_type());
    }
    if (!status_or_translator.ok()) {
      traffic_pattern_status_ = status_or_translator.status();
      LOG(ERROR) << "Failed to build translator for " << traffic_pattern
                 << traffic_pattern_status_;
      return;
    }
    auto translator = std::move(status_or_translator.value());
    for (const auto& it : ready_responses_) {
      int follower_id = it.first;
      traffic_pattern_status_ = translator->AddToResponse(
          follower_id, &(ready_responses_.at(follower_id)));
      if (!traffic_pattern_status_.ok()) {
        LOG(ERROR) << "Failed to translate for global traffic: "
                   << traffic_pattern.global_traffic_id()
                   << " participant: " << follower_id
                   << " reason: " << traffic_pattern_status_.message();
        return;
      }
    }
  }

  LOG(INFO) << "Finished GeneratePerFollowerTraffic";
}

void VerbsMarksLeaderImpl::StartExperiment(
    verbsmarks::proto::StartExperimentRequest* req,
    std::unique_ptr<
        std::function<void(verbsmarks::proto::StartExperimentResponse)>>
        response_handler) {
  absl::MutexLock lock(&experiment_mutex_);
  proto::StartExperimentResponse response;
  if (req->status().code() != google::rpc::OK) {
    LOG(ERROR) << "Follower " << req->follower_id()
               << " reported failure: " << *req
               << ". Stopping experiment. We have " << ready_followers_.size()
               << " ready followers.";
    // Failure reported. Send failure to the reporter.
    error_followers_.insert(req->follower_id());
    starting_experiment_failed_ = true;
    response.mutable_status()->set_code(google::rpc::FAILED_PRECONDITION);
    response.mutable_status()->set_message(absl::StrCat(
        "Some followers failed: ", absl::StrJoin(error_followers_, ",")));
    (*response_handler)(response);
    // If anyone has an error, we can't start experiment. Send error to
    // everyone who was ready.
    for (const auto& follower_pair : ready_followers_) {
      auto handler = *follower_pair.second;
      handler(response);
    }
    finished_.first = true;
    finished_.second = absl::InternalError("Start experiment error.");
    ready_followers_.clear();
    return;
  }
  if (starting_experiment_failed_) {
    // If already failed, just send an error.
    response.mutable_status()->set_code(google::rpc::FAILED_PRECONDITION);
    response.mutable_status()->set_message(absl::StrCat(
        "Some followers failed: ", absl::StrJoin(error_followers_, ",")));
    (*response_handler)(response);
    return;
  }

  ready_followers_[req->follower_id()] = std::move(response_handler);

  // Respond only when everyone in the group sent successful ready.
  if (ready_followers_.size() == config_.group_size()) {
    LOG(INFO) << "Everyone is ready. Start experiment";
    response.mutable_status()->set_code(Code::OK);
    response.set_enable_heartbeats(enable_heartbeats_);
    for (const auto& follower_pair : ready_followers_) {
      auto handler = *follower_pair.second;
      handler(response);
    }

    if (enable_heartbeats_) {
      absl::flat_hash_set<int32_t> follower_ids;
      for (const auto& [id, handler] : ready_followers_) {
        follower_ids.insert(id);
      }
      //
      follower_health_.BeginTracking(follower_ids);
    } else {
      // With an empty follower_id set, FollowerHealthTracker will remain in
      // healthy status and never timeout or abort.
      follower_health_.BeginTracking({});
    }

    ready_followers_.clear();
  }
  if (ready_followers_.size() > config_.group_size()) {
    LOG(FATAL) << "Something went wrong. We have more ready requests ("
               << ready_followers_.size()
               << ") than the "
                  "group size("
               << config_.group_size() << "). Please check the config.";
  }
}

void VerbsMarksLeaderImpl::FinishExperiment(
    verbsmarks::proto::ResultRequest* req,
    std::unique_ptr<std::function<void(verbsmarks::proto::ResultResponse)>>
        response_handler) {
  absl::MutexLock lock(&experiment_mutex_);
  // Followers will destroy and finish once receiving a response.
  proto::ResultResponse response;
  if (req->status().code() != google::rpc::OK || finished_.first) {
    response.mutable_status()->set_code(Code::INTERNAL);
    // If error happened, no need to wait to receive all the reports.
    LOG(ERROR) << "Follower " << req->follower_id()
               << " reported failure. Stopping experiment.";
    (*response_handler)(response);
    // Send a response to everyone who reported good so far, if any.
    for (const auto& follower_pair : finished_followers_) {
      auto handler = *follower_pair.second;
      handler(response);
    }
    ready_followers_.clear();
    finished_.first = true;
    finished_.second = absl::InternalError("Finish experiment error.");
    return;
  }
  LOG(INFO) << "Follower " << req->follower_id() << " finished.\n" << *req;
  finished_followers_[req->follower_id()] = std::move(response_handler);
  // Add the result to the final report.
  *final_report_.add_follower_results() = *req;

  // Stop tracking heartbeats for finished follower.
  follower_health_.RegisterFinished(req->follower_id());

  if (finished_followers_.size() == config_.group_size()) {
    LOG(INFO) << "Everyone finished.";
    response.mutable_status()->set_code(Code::OK);
    for (const auto& follower_pair : finished_followers_) {
      auto handler = *follower_pair.second;
      handler(response);
    }
    finished_followers_.clear();

    // Final report may include per-second stats from the last intervals.
    statistics_collector_.CollectFinalReportStatistics(&final_report_);
    // If periodic result output is enabled, we will save these stats into
    // a periodic result file for output consistency.
    SavePeriodicResult(/*time_check=*/false);
    // If periodic are disabled, all results are added to the final report
    // in sorted order.
    statistics_collector_.WriteFinalReport(&final_report_);

    auto filename = absl::GetFlag(FLAGS_result_file_name);
    if (!filename.empty()) {
      *final_report_.mutable_config() = config_;
      final_report_.set_description(absl::GetFlag(FLAGS_description));
      *final_report_.mutable_end_timestamp() = utils::TimeToProto(absl::Now());
      LOG(INFO) << "Saving the final report to: " << filename;
      std::string final_report_string;
      if (!google::protobuf::TextFormat::PrintToString(final_report_,
                                                       &final_report_string)) {
        LOG(ERROR) << "Failed to generate final report textproto string";
      } else if (auto status = utils::SaveToFile(filename, final_report_string);
                 !status.ok()) {
        LOG(ERROR) << "Failed to save file: " << filename << " " << status;
      }
    }
    finished_.first = true;
    if (enable_heartbeats_ && !follower_health_.ExperimentIsHealthy()) {
      finished_.second =
          absl::CancelledError("Experiment ended due to unhealthy follower(s)");
    }
  }
}

void VerbsMarksLeaderImpl::HandleHeartbeat(
    verbsmarks::proto::HeartbeatRequest* req,
    std::unique_ptr<std::function<void(verbsmarks::proto::HeartbeatResponse)>>
        response_handler) {
  absl::MutexLock lock(&experiment_mutex_);

  proto::HeartbeatResponse response;
  bool follower_healthy = req->follower_status().code() == google::rpc::OK;
  if (!follower_healthy && !first_follower_failure_) {
    first_follower_failure_ = {req->follower_id(), req->follower_status()};
  }

  follower_health_.RegisterHeartbeat(req->follower_id(), follower_healthy);
  statistics_collector_.CollectStatistics(req->current_stats(),
                                          req->follower_id());

  if (follower_health_.ExperimentIsHealthy()) {
    response.mutable_leader_status()->set_code(Code::OK);
  } else {
    LOG(WARNING) << "Sending abort response to follower " << req->follower_id();
    response.mutable_leader_status()->set_code(Code::CANCELLED);
    if (first_follower_failure_) {
      response.set_failure_follower_id(first_follower_failure_->first);
      *response.mutable_failure_reason() = first_follower_failure_->second;
    } else {
      response.mutable_failure_reason()->set_code(Code::INTERNAL);
      response.mutable_failure_reason()->set_message(
          "A follower disconnected or timed out without reporting a failure "
          "to the leader.");
    }
  }

  // Write periodic result file if enabled and interval has elapsed.
  SavePeriodicResult();

  (*response_handler)(response);
}

std::pair<bool, absl::Status> VerbsMarksLeaderImpl::IsFinished()
    ABSL_LOCKS_EXCLUDED(experiment_mutex_) {
  absl::MutexLock lock(&experiment_mutex_);
  if (finished_.first) {
    // Always return finished status once set to true.
    return finished_;
  }

  // Otherwise check health and return if unhealthy.
  if (enable_heartbeats_ && !follower_health_.ExperimentIsHealthy()) {
    finished_.first = true;
    finished_.second =
        absl::CancelledError("Experiment ended due to unhealthy follower(s)");
  }

  return finished_;
}

void VerbsMarksLeaderImpl::SavePeriodicResult(bool time_check) {
  std::string filename_template =
      absl::GetFlag(FLAGS_periodic_result_file_name_template);

  if (!enable_heartbeats_ || filename_template.empty()) {
    // Skip entirely if filename is unspecified or heartbeats disabled.
    return;
  }

  absl::Time now = absl::Now();
  if (time_check &&
      now - periodic_result_save_time_ < kPeriodicResultSaveInterval) {
    return;
  }

  std::optional<proto::PeriodicResultReport> result_or_null =
      statistics_collector_.WritePeriodicReport();

  if (result_or_null.has_value()) {
    std::string filename;
    if (absl::StrContains(filename_template, "$0")) {
      // Substitute placeholder if specified.
      filename =
          absl::Substitute(filename_template, periodic_result_save_count_++);
    } else {
      // Otherwise simply append _ and sequence number.
      filename =
          absl::StrCat(filename_template, "_", periodic_result_save_count_++);
    }

    LOG(INFO) << "Saving periodic report to: " << filename;
    std::fstream ofstream(filename,
                          std::ios::out | std::ios::trunc | std::ios::binary);
    if (ofstream.bad()) {
      LOG(ERROR) << "Failed to open periodic result file: " << filename;
    }
    if (!result_or_null.value().SerializeToOstream(&ofstream)) {
      LOG(ERROR) << "Failed to save periodic result file: " << filename;
      LOG(ERROR) << result_or_null.value();
    }
  }
  periodic_result_save_time_ = now;
}

}  // namespace verbsmarks
