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

#include "gar/reader/arrow_chunk_reader.h"

namespace GAR_NAMESPACE_INTERNAL {

static void BM_ReadParquetChunk(::benchmark::State& state) {
  std::string root;
  GAR_NAMESPACE::Status status = GAR_NAMESPACE::GetTestResourceRoot(&root);
  if (!status.ok()) {
    state.SkipWithError(status.message().c_str());
    return;
  }
  std::string path = root + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
  auto maybe_graph_info = GraphInfo::Load(path);
  if (maybe_graph_info.has_error()) {
    state.SkipWithError(maybe_graph_info.status().message().c_str());
    return;
  }
  auto graph_info = maybe_graph_info.value();
  auto maybe_reader = VertexPropertyArrowChunkReader::Make(
      graph_info, "person", "id");
  if (maybe_reader.has_error()) {
    state.SkipWithError(maybe_reader.status().message().c_str());
    return;
  }
  auto reader = maybe_reader.value();
  for (auto _ : state) {
    reader->seek(0);
    reader->GetChunk();
    reader->NextChunk();
  }
}
}
