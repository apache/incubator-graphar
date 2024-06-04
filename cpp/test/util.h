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

#include <filesystem>
#include <iostream>
#include <string>

namespace graphar {

// Define the fixture
struct GlobalFixture {
  GlobalFixture() {
    // Setup code here, this runs before each test case
    setup();
  }

  ~GlobalFixture() {}

  void setup() {
    const char* c_root = std::getenv("GAR_TEST_DATA");
    if (!c_root) {
      throw std::runtime_error(
          "Test resources not found, set GAR_TEST_DATA to auxiliary testing "
          "data");
    }
    test_data_dir = std::string(c_root);
  }

  // test data dir to be used in tests
  std::string test_data_dir;
};

}  // namespace graphar
