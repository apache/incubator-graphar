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

#include "benchmark/benchmark.h"

#include "./benchmark_util.h"
#include "gar/graph_info.h"

namespace graphar {

static void CreateGraphInfo(::benchmark::State& state,  // NOLINT
                            const std::string& path) {
  for (auto _ : state) {
    auto maybe_graph_info = GraphInfo::Load(path);
    if (maybe_graph_info.has_error()) {
      state.SkipWithError(maybe_graph_info.status().message().c_str());
      return;
    }
  }
}

BENCHMARK_DEFINE_F(BenchmarkFixture, InitialGraphInfo)
(::benchmark::State& state) {  // NOLINT
  CreateGraphInfo(state, path_);
}
BENCHMARK_REGISTER_F(BenchmarkFixture, InitialGraphInfo);

}  // namespace graphar
