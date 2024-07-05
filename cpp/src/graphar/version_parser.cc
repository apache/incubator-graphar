/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <algorithm>
#include <iostream>
#include <regex>  // NOLINT
#include <string>

#include "graphar/version_parser.h"

namespace graphar {

// Helper function for parsing version string
bool is_whitespace(char ch) {
  return ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n';
}

void trim(std::string& s) {  // NOLINT
  size_t trim_left = 0;
  for (auto it = s.begin(); it != s.end(); ++it) {
    if (!is_whitespace(*it)) {
      break;
    }
    ++trim_left;
  }

  if (trim_left == s.size()) {
    s.clear();
  } else {
    size_t trim_right = 0;
    for (auto it = s.rbegin(); it != s.rend(); ++it) {
      if (!is_whitespace(*it)) {
        break;
      }
      ++trim_right;
    }

    if (trim_left > 0 || trim_right > 0) {
      if (trim_left == 0) {
        s.resize(s.size() - trim_right);
      } else {
        std::string copy(s.c_str() + trim_left,
                         s.size() - trim_left - trim_right);
        s.swap(copy);
      }
    }
  }
}

int parserVersionImpl(const std::string& version_str) {
  std::smatch match;
  const std::regex version_regex("gar/v(\\d+).*");
  if (std::regex_match(version_str, match, version_regex)) {
    if (match.size() != 2) {
      throw std::runtime_error("Invalid version string: " + version_str);
    }
    return std::stoi(match[1].str());
  } else {
    throw std::runtime_error("Invalid version string: " + version_str);
  }
}

std::vector<std::string> parseUserDefineTypesImpl(
    const std::string& version_str) {
  std::smatch match;
  std::vector<std::string> user_define_types;
  const std::regex user_define_types_regex("gar/v\\d+ *\\((.*)\\).*");
  if (std::regex_match(version_str, match, user_define_types_regex)) {
    if (match.size() != 2) {
      throw std::runtime_error("Invalid version string: " + version_str);
    }
    std::string types_str = match[1].str();
    size_t pos = 0;
    while (pos != std::string::npos) {
      size_t next_pos = types_str.find(',', pos);
      std::string type = types_str.substr(pos, next_pos - pos);
      trim(type);
      if (!type.empty()) {
        user_define_types.push_back(type);
      }
      if (next_pos != std::string::npos) {
        pos = next_pos + 1;
      } else {
        pos = next_pos;
      }
    }
  }
  return user_define_types;
}

Result<std::shared_ptr<const InfoVersion>> InfoVersion::Parse(
    const std::string& version_str) noexcept {
  std::shared_ptr<InfoVersion> version;
  try {
    version = std::make_shared<InfoVersion>(
        parserVersionImpl(version_str), parseUserDefineTypesImpl(version_str));
  } catch (const std::exception& e) {
    return Status::Invalid("Invalid version string: ", version_str);
  }
  return version;
}
}  // namespace graphar
