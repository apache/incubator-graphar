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

#ifndef GAR_UTILS_WRITER_UTILS_H_
#define GAR_UTILS_WRITER_UTILS_H_

#include "gar/utils/macros.h"

namespace GAR_NAMESPACE_INTERNAL {

/**
 * @brief The level for validating writing operations.
 */
enum class ValidateLevel : char {
  default_validate = 0,
  no_validate = 1,
  weak_validate = 2,
  strong_validate = 3
};

}  // namespace GAR_NAMESPACE_INTERNAL
#endif  // GAR_UTILS_WRITER_UTILS_H_
