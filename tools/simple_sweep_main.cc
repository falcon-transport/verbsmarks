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

#include <fstream>
#include <ios>
#include <iostream>
#include <string>
#include <utility>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/text_format.h"
#include "tools/experiment_config.pb.h"
#include "tools/experiment_sweep_parameter_util.h"
#include "utils.h"

ABSL_FLAG(std::string, experiment_config, "", "The experiment config file.");

ABSL_FLAG(std::string, output_dir, "",
          "Output directory for generated leader configs.");

template <typename T>
absl::StatusOr<T> LoadTextProto(absl::string_view filename) {
  T output_proto;
  std::ifstream infile(std::string(filename), std::ios::in);
  if (!infile.is_open()) {
    return absl::InternalError("Failed to open textproto file");
  }
  std::string textproto_data((std::istreambuf_iterator<char>(infile)),
                             std::istreambuf_iterator<char>());
  if (!google::protobuf::TextFormat::ParseFromString(textproto_data,
                                                     &output_proto)) {
    return absl::InvalidArgumentError(
        absl::StrCat("Failed to textproto file: ", filename));
  }
  return output_proto;
}

absl::Status SanityCheckExperimentConfig(
    const verbsmarks::proto::ExperimentConfig& config) {
  if (config.hosts_size() == 0) {
    return absl::InvalidArgumentError("No host information is provided.");
  }
  if (config.traffic_pattern_template_file_path().empty()) {
    return absl::InvalidArgumentError("No config template is provided.");
  }
  return absl::OkStatus();
}

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);

  // 1. Read experiment config.
  auto experiment_config_or_error =
      LoadTextProto<verbsmarks::proto::ExperimentConfig>(
          absl::GetFlag(FLAGS_experiment_config));
  if (!experiment_config_or_error.ok()) {
    LOG(FATAL) << "Exiting due to failure to load experiment config";
  }

  verbsmarks::proto::ExperimentConfig& experiment_config =
      experiment_config_or_error.value();

  // 2. Check for valid experiment config.
  auto check_status = SanityCheckExperimentConfig(experiment_config);
  if (!check_status.ok()) {
    LOG(FATAL) << "Exiting due to ExperimentConfig sanity check failure: "
               << check_status.message();
  }

  // 3. Read leader config template.
  auto leader_config_or_error = LoadTextProto<verbsmarks::proto::LeaderConfig>(
      experiment_config.traffic_pattern_template_file_path());
  if (!leader_config_or_error.ok()) {
    LOG(FATAL) << "Exiting due to failure to load leader config template";
  }
  verbsmarks::proto::LeaderConfig& leader_config_template =
      leader_config_or_error.value();

  // 4. Generate cartesian product of sweep parameters.
  verbsmarks::SweepParameters parameter_space =
      verbsmarks::GetVerbsmarksSweepParameters(
          experiment_config.verbsmarks_sweep_parameters(),
          experiment_config.experiment_iterations());
  verbsmarks::SweepParameters sweep_parameter_combinations =
      verbsmarks::GenerateCartesianProductOfSweepParameters(parameter_space);

  // 5. write leader configs to output dir.
  int file_index = 0;
  for (const auto& combination : sweep_parameter_combinations) {
    std::pair<std::string, std::string> name_and_iteration;
    name_and_iteration = verbsmarks::ConstructExperimentId(combination);
    verbsmarks::proto::LeaderConfig generated_config =
        verbsmarks::GenerateConfigFromSweepParameters(leader_config_template,
                                                      combination);

    std::string filename =
        absl::StrCat(absl::GetFlag(FLAGS_output_dir), "/", ++file_index, "_",
                     name_and_iteration.first, ".textpb");
    LOG(INFO) << "Writing LeaderConfig: " << filename;
    std::string config_textproto;
    if (!google::protobuf::TextFormat::PrintToString(generated_config,
                                                     &config_textproto)) {
      LOG(ERROR) << "Failed to generate textproto from LeaderConfig";
    } else if (auto status =
                   verbsmarks::utils::SaveToFile(filename, config_textproto);
               !status.ok()) {
      LOG(ERROR) << "Failed to save file: " << filename << " " << status;
    }
  }

  return 0;
}
