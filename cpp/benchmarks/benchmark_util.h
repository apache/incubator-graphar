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
#include <memory>
#include <string>

#include "benchmark/benchmark.h"

#include "graphar/api/info.h"

#define SKIP_WITH_ERROR_STATUS(state, status)      \
  if (!status.ok()) {                              \
    state.SkipWithError(status.message().c_str()); \
    return;                                        \
  }

namespace graphar {

class BenchmarkFixture : public ::benchmark::Fixture {
 public:
  void SetUp(const ::benchmark::State& state) override {
    const char* c_root = std::getenv("GAR_TEST_DATA");
    if (!c_root) {
      throw std::runtime_error(
          "Test resources not found, set GAR_TEST_DATA to auxiliary testing "
          "data");
    }
    path_ = std::string(c_root) + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
    auto maybe_graph_info = GraphInfo::Load(path_);
    graph_info_ = maybe_graph_info.value();

    second_path_ = std::string(c_root) + "/ldbc/parquet/ldbc.graph.yml";
    auto second_maybe_graph_info = GraphInfo::Load(second_path_);
    second_graph_info_ = second_maybe_graph_info.value();
  }

  void TearDown(const ::benchmark::State& state) override {}

 protected:
  std::string path_;
  std::shared_ptr<GraphInfo> graph_info_;
  std::string second_path_;
  std::shared_ptr<GraphInfo> second_graph_info_;
};
}  // namespace graphar
