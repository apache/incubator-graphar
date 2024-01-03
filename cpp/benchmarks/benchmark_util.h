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

#include <filesystem>
#include <memory>
#include <string>

#include "benchmark/benchmark.h"

#include "gar/graph_info.h"
#include "gar/util/status.h"

#ifndef CPP_BENCHMARKS_BENCHMARK_UTIL_H_
#define CPP_BENCHMARKS_BENCHMARK_UTIL_H_

namespace GAR_NAMESPACE_INTERNAL {

// Return the value of the GAR_TEST_DATA environment variable or return error
// Status
GAR_NAMESPACE::Status GetTestResourceRoot(std::string* out) {
  const char* c_root = std::getenv("GAR_TEST_DATA");
  if (!c_root) {
    return GAR_NAMESPACE::Status::IOError(
        "Test resources not found, set GAR_TEST_DATA to <repo root>/testing");
  }
  // FIXME(@acezen): This is a hack to get around the fact that the testing
  *out = std::string(c_root) + "/new";
  return GAR_NAMESPACE::Status::OK();
}

class BenchmarkFixture : public ::benchmark::Fixture {
 public:
  void SetUp(const ::benchmark::State& state) override {
    std::string root;
    GAR_NAMESPACE::Status status = GetTestResourceRoot(&root);
    path_ = root + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
    auto maybe_graph_info = GraphInfo::Load(path_);
    graph_info_ = maybe_graph_info.value();
  }

  void TearDown(const ::benchmark::State& state) override {}

 protected:
  std::string path_;
  std::shared_ptr<GraphInfo> graph_info_;
};
}  // namespace GAR_NAMESPACE_INTERNAL

#endif  // CPP_BENCHMARKS_BENCHMARK_UTIL_H_
