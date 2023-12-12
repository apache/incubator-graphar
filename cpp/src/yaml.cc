/*
 * Copyright 2022-2023 Alibaba Group Holding Limited.
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

#include <memory>
#include <string>

#include "yaml/Yaml.hpp"

#include "gar/util/result.h"
#include "gar/util/yaml.h"

namespace GAR_NAMESPACE_INTERNAL {

const ::Yaml::Node Yaml::operator[](const std::string& key) const {
  return root_node_->operator[](key);
}

Result<std::shared_ptr<Yaml>> Yaml::Load(const std::string& input) {
  std::shared_ptr<::Yaml::Node> root_node = std::make_shared<::Yaml::Node>();
  try {
    ::Yaml::Parse(*root_node, input);
  } catch (::Yaml::Exception& e) { return Status::YamlError(e.what()); }
  return std::make_shared<Yaml>(root_node);
}

Result<std::shared_ptr<Yaml>> Yaml::Load(std::iostream& input) {
  std::shared_ptr<::Yaml::Node> root_node = std::make_shared<::Yaml::Node>();
  try {
    ::Yaml::Parse(*root_node, input);
  } catch (::Yaml::Exception& e) { return Status::YamlError(e.what()); }
  return std::make_shared<Yaml>(root_node);
}

Result<std::shared_ptr<Yaml>> Yaml::LoadFile(const std::string& file_name) {
  std::shared_ptr<::Yaml::Node> root_node = std::make_shared<::Yaml::Node>();
  try {
    ::Yaml::Parse(*root_node, file_name.c_str());
  } catch (::Yaml::Exception& e) { return Status::YamlError(e.what()); }
  return std::make_shared<Yaml>(root_node);
}

}  // namespace GAR_NAMESPACE_INTERNAL
