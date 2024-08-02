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

#pragma once

#include <map>
#include <memory>
#include <regex>  // NOLINT
#include <string>
#include <vector>

#include "graphar/result.h"

namespace graphar {

/** InfoVersion is a class provide version information of info. */
class InfoVersion {
 public:
  /** Parse version string to InfoVersion. */
  static Result<std::shared_ptr<const InfoVersion>> Parse(
      const std::string& str) noexcept;

  /** Default constructor */
  InfoVersion() : version_(version2types.rbegin()->first) {}
  /** Constructor with version */
  explicit InfoVersion(int version) : version_(version) {
    if (version2types.find(version) == version2types.end()) {
      throw std::invalid_argument("Unsupported version: " +
                                  std::to_string(version));
    }
  }
  /** Constructor with version and user defined types. */
  explicit InfoVersion(int version,
                       const std::vector<std::string>& user_define_types)
      : version_(version), user_define_types_(user_define_types) {
    if (version2types.find(version) == version2types.end()) {
      throw std::invalid_argument("Unsupported version: " +
                                  std::to_string(version));
    }
  }
  /** Copy constructor */
  InfoVersion(const InfoVersion& other) = default;
  /** Copy assignment */
  inline InfoVersion& operator=(const InfoVersion& other) = default;

  /** Check if two InfoVersion are equal */
  bool operator==(const InfoVersion& other) const {
    return version_ == other.version_ &&
           user_define_types_ == other.user_define_types_;
  }

  /** Get version */
  int version() const { return version_; }

  /** Get user defined types */
  const std::vector<std::string>& user_define_types() const {
    return user_define_types_;
  }

  /** Dump version to string. */
  std::string ToString() const {
    std::string str = "gar/v" + std::to_string(version_);
    if (!user_define_types_.empty()) {
      str += " (";
      for (auto& type : user_define_types_) {
        str += type + ",";
      }
      str.back() = ')';
    }
    return str;
  }

  /** Check if type is supported by version. */
  inline bool CheckType(const std::string& type_str) const {
    auto& types = version2types.at(version_);
    // check if type_str is in supported types of version
    if (std::find(types.begin(), types.end(), type_str) != types.end()) {
      return true;
    }
    // check if type_str is in user defined types
    if (std::find(user_define_types_.begin(), user_define_types_.end(),
                  type_str) != user_define_types_.end()) {
      return true;
    }
    return false;
  }

 private:
  inline static const std::map<int, std::vector<std::string>> version2types{
      {1, {"bool", "int32", "int64", "float", "double", "string"}},
  };
  int version_;
  std::vector<std::string> user_define_types_;
};

}  // namespace graphar
