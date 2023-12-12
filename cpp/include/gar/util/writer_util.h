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


#ifndef GAR_UTIL_WRITER_UTIL_H_
#define GAR_UTIL_WRITER_UTIL_H_

#include "gar/util/macros.h"

namespace GAR_NAMESPACE_INTERNAL {

/**
 * @brief The level for validating writing operations.
 */
enum class ValidateLevel : char {
  /// To use the default validate level of the writer/builder.
  default_validate = 0,
  /// To skip the validation.
  no_validate = 1,
  /// Weak validation: check if the index, count, adj_list type, property group
  /// and the size of the table passed to the writer/builder are valid.
  weak_validate = 2,
  /// Strong validation: except for the weak validation, also check if the
  /// schema (including each property name and data type) of the intput data
  /// passed to the writer/builder is consistent with that defined in the info.
  strong_validate = 3
};

}  // namespace GAR_NAMESPACE_INTERNAL
#endif  // GAR_UTIL_WRITER_UTIL_H_
