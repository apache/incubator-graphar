/** Copyright 2022 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef GAR_UTILS_VERSION_PARSER_H_
#define GAR_UTILS_VERSION_PARSER_H_

#include <map>
#include <string>
#include <vector>
#include <regex>

#include "gar/utils/result.h"

namespace GAR_NAMESPACE_INTERNAL {

/// \brief InfoVersion is a class provide version information of info.
class InfoVersion {
 public:
  // inline static const std::regex version_regex{"gar/v\\d+\\"};
  // inline static const std::regex user_define_types_regex{"\\(.*\\)"};
  static Result<InfoVersion> Parse(const std::string& str) noexcept;

  InfoVersion() : version_(version2types.rbegin()->first) {}
  explicit InfoVersion(int version) : version_(version) {}
  InfoVersion(const InfoVersion& other) = default;
  inline InfoVersion& operator=(const InfoVersion& other) = default;

  std::string ToString() const {
    std::string str = "gar/" + std::to_string(version_);
    if (!user_define_types_.empty()) {
      str += " (";
      for (auto& type : user_define_types_) {
        str += type + ",";
      }
      str.back() = ')';
    }
    return str;
  }

  inline bool CheckType(const std::string& type_str) noexcept {
    auto& types = version2types.at(version_);
    // check if type_str is in supported types of version
    if (std::find(types.begin(), types.end(), type_str) != types.end()) {
      return true;
    }
    // check if type_str is in user defined types
    if (std::find(user_define_types_.begin(), user_define_types_.end(), type_str) != user_define_types_.end()) {
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

}  // namespace GAR_NAMESPACE_INTERNAL
#endif  // GAR_UTILS_VERSION_PARSER_H_
