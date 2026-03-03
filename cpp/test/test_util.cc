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

#include <iostream>
#include <string>

#include <catch2/catch_test_macros.hpp>
#include "./util.h"
#include "graphar/util.h"

namespace graphar {

TEST_CASE_METHOD(GlobalFixture, "PathUtilTest") {
  SECTION("PathToDirectory_S3_Handling") {
    // 1. Test standard S3 URI with query (existing functionality)
    std::string s3_with_query = "s3://bucket/path/graph.yml?version=123";
    REQUIRE(util::PathToDirectory(s3_with_query) ==
            "s3://bucket/path/?version=123");

    // 2. Test S3 URI WITHOUT query (The Bug Fix!)
    std::string s3_no_query = "s3://bucket/path/graph.yml";
    REQUIRE(util::PathToDirectory(s3_no_query) == "s3://bucket/path/");

    // 3. Test S3 URI at root
    std::string s3_root = "s3://bucket/graph.yml";
    REQUIRE(util::PathToDirectory(s3_root) == "s3://bucket/");
  }

  SECTION("PathToDirectory_Local_Handling") {
    // 4. Test standard local path
    std::string local_path = "/tmp/data/graph.yml";
    REQUIRE(util::PathToDirectory(local_path) == "/tmp/data/");

    // 5. Test relative path
    std::string relative_path = "graph.yml";
    REQUIRE(util::PathToDirectory(relative_path) == "graph.yml");
  }
}

}  // namespace graphar
