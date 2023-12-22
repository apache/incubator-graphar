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

#include "benchmark/benchmark.h"

#include "./benchmark_util.h"
#include "gar/graph_info.h"
#include "gar/reader/arrow_chunk_reader.h"

namespace GAR_NAMESPACE_INTERNAL {

template <class Reader> void ReadChunk(::benchmark::State& state, const std::shared_ptr<GraphInfo>& graph_info) {
  auto maybe_reader = Reader::Make(
      graph_info, state.range(0), state.range(3));
  if (maybe_reader.has_error()) {
    state.SkipWithError(maybe_reader.status().message().c_str());
    return;
  }
  auto reader = maybe_reader.value();
  for (auto _ : state) {
    reader->seek(0);
    reader->GetChunk();
    reader->next_chunk();
  }
}

BENCHMARK_DEFINE_F(BenchmarkFixture, CreateReader)(::benchmark::State& state) {
  for (auto _ : state) {
    auto maybe_reader = VertexPropertyArrowChunkReader::Make(
        graph_info_, state.range(0), state.range(3));
    if (maybe_reader.has_error()) {
      state.SkipWithError(maybe_reader.status().message().c_str());
      return;
    }
  }
}
BENCHMARK_REGISTER_F(BenchmarkFixture, CreateReader);

BENCHMARK_DEFINE_F(BenchmarkFixture, VertexPropertyArrowChunkReaderReadChunk)(::benchmark::State& state) {
  ReadParquetChunk(state, graph_info_);
}
BENCHMARK_REGISTER_F(BenchmarkFixture, VertexPropertyArrowChunkReaderReadChunk);
}  // namespace GAR_NAMESPACE_INTERNAL
