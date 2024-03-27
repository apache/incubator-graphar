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

#include <map>
#include <string>
#include <utility>

#include "gar/util/macros.h"

#ifndef GAR_UTIL_ADJ_LIST_TYPE_H_
#define GAR_UTIL_ADJ_LIST_TYPE_H_

namespace graphar {

/** Adj list type enumeration for adjacency list of graph. */
enum class AdjListType : std::uint8_t {
  /// collection of edges by source, but unordered, can represent COO format
  unordered_by_source = 0b00000001,
  /// collection of edges by destination, but unordered, can represent COO
  /// format
  unordered_by_dest = 0b00000010,
  /// collection of edges by source, ordered by source, can represent CSR format
  ordered_by_source = 0b00000100,
  /// collection of edges by destination, ordered by destination, can represent
  /// CSC format
  ordered_by_dest = 0b00001000,
};

constexpr AdjListType operator|(AdjListType lhs, AdjListType rhs) {
  return static_cast<AdjListType>(
      static_cast<std::underlying_type_t<AdjListType>>(lhs) |
      static_cast<std::underlying_type_t<AdjListType>>(rhs));
}

constexpr AdjListType operator&(AdjListType lhs, AdjListType rhs) {
  return static_cast<AdjListType>(
      static_cast<std::underlying_type_t<AdjListType>>(lhs) &
      static_cast<std::underlying_type_t<AdjListType>>(rhs));
}

static inline const char* AdjListTypeToString(AdjListType adj_list_type) {
  static const std::map<AdjListType, const char*> adj_list2string{
      {AdjListType::unordered_by_source, "unordered_by_source"},
      {AdjListType::unordered_by_dest, "unordered_by_dest"},
      {AdjListType::ordered_by_source, "ordered_by_source"},
      {AdjListType::ordered_by_dest, "ordered_by_dest"}};
  return adj_list2string.at(adj_list_type);
}

static inline AdjListType OrderedAlignedToAdjListType(
    bool ordered, const std::string& aligned) {
  if (ordered) {
    return aligned == "src" ? AdjListType::ordered_by_source
                            : AdjListType::ordered_by_dest;
  }
  return aligned == "src" ? AdjListType::unordered_by_source
                          : AdjListType::unordered_by_dest;
}

static inline std::pair<bool, std::string> AdjListTypeToOrderedAligned(
    AdjListType adj_list_type) {
  switch (adj_list_type) {
  case AdjListType::unordered_by_source:
    return std::make_pair(false, "src");
  case AdjListType::unordered_by_dest:
    return std::make_pair(false, "dst");
  case AdjListType::ordered_by_source:
    return std::make_pair(true, "src");
  case AdjListType::ordered_by_dest:
    return std::make_pair(true, "dst");
  default:
    return std::make_pair(false, "dst");
  }
}

}  // namespace graphar
#endif  // GAR_UTIL_ADJ_LIST_TYPE_H_
