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

#include <filesystem>
#include <string>

#ifndef CPP_GRIN_EXAMPLE_CONFIG_H_
#define CPP_GRIN_EXAMPLE_CONFIG_H_

static const std::string TEST_DATA_DIR =  // NOLINT
    std::filesystem::path(__FILE__).parent_path().string() + "/test_data";

static const std::string TEST_DATA_SMALL_DIR =  // NOLINT
    std::filesystem::path(__FILE__)
        .parent_path()
        .parent_path()
        .parent_path()
        .parent_path()
        .string() +
    "/test/gar-test";

#endif  // CPP_GRIN_EXAMPLE_CONFIG_H_
