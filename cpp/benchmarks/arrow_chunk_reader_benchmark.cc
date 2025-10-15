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
#include "graphar/api/arrow_reader.h"
#include "graphar/fwd.h"

namespace graphar {

BENCHMARK_DEFINE_F(BenchmarkFixture, CreateVertexPropertyArrowChunkReader)
(::benchmark::State& state) {  // NOLINT
  for (auto _ : state) {
    auto gp =
        graph_info_->GetVertexInfo("person")->GetPropertyGroup("firstName");
    auto maybe_reader =
        VertexPropertyArrowChunkReader::Make(graph_info_, "person", gp);
    SKIP_WITH_ERROR_STATUS(state, maybe_reader.status());
  }
}

BENCHMARK_DEFINE_F(BenchmarkFixture, CreateAdjListArrowChunkReader)
(::benchmark::State& state) {  // NOLINT
  for (auto _ : state) {
    auto maybe_reader =
        AdjListArrowChunkReader::Make(graph_info_, "person", "knows", "person",
                                      AdjListType::ordered_by_source);
    SKIP_WITH_ERROR_STATUS(state, maybe_reader.status());
  }
}

BENCHMARK_DEFINE_F(BenchmarkFixture, CreateAdjListOffsetArrowChunkReader)
(::benchmark::State& state) {  // NOLINT
  for (auto _ : state) {
    auto maybe_reader = AdjListOffsetArrowChunkReader::Make(
        graph_info_, "person", "knows", "person",
        AdjListType::ordered_by_source);
    SKIP_WITH_ERROR_STATUS(state, maybe_reader.status());
  }
}

BENCHMARK_DEFINE_F(BenchmarkFixture, CreateAdjListPropertyArrowChunkReader)
(::benchmark::State& state) {  // NOLINT
  for (auto _ : state) {
    auto maybe_reader = AdjListPropertyArrowChunkReader::Make(
        graph_info_, "person", "knows", "person", "creationDate",
        AdjListType::ordered_by_source);
    SKIP_WITH_ERROR_STATUS(state, maybe_reader.status());
  }
}

BENCHMARK_DEFINE_F(BenchmarkFixture, AdjListArrowChunkReaderReadChunk)
(::benchmark::State& state) {  // NOLINT
  auto maybe_reader = AdjListArrowChunkReader::Make(
      graph_info_, "person", "knows", "person", AdjListType::ordered_by_source);
  SKIP_WITH_ERROR_STATUS(state, maybe_reader.status());
  auto reader = maybe_reader.value();
  for (auto _ : state) {
    auto st = reader->seek(0);
    SKIP_WITH_ERROR_STATUS(state, st);
    auto chunk_result = reader->GetChunk();
    SKIP_WITH_ERROR_STATUS(state, chunk_result.status());
    auto next_result = reader->next_chunk();
    SKIP_WITH_ERROR_STATUS(state, next_result);
  }
}

BENCHMARK_DEFINE_F(BenchmarkFixture, AdjListOffsetArrowChunkReaderReadChunk)
(::benchmark::State& state) {  // NOLINT
  auto maybe_reader = AdjListOffsetArrowChunkReader::Make(
      graph_info_, "person", "knows", "person", AdjListType::ordered_by_source);
  SKIP_WITH_ERROR_STATUS(state, maybe_reader.status());
  auto reader = maybe_reader.value();
  for (auto _ : state) {
    auto st = reader->seek(0);
    SKIP_WITH_ERROR_STATUS(state, st);
    auto chunk_result = reader->GetChunk();
    SKIP_WITH_ERROR_STATUS(state, chunk_result.status());
    auto next_result = reader->next_chunk();
    SKIP_WITH_ERROR_STATUS(state, next_result);
  }
}

BENCHMARK_DEFINE_F(BenchmarkFixture, AdjListPropertyArrowChunkReaderReadChunk)
(::benchmark::State& state) {  // NOLINT
  auto maybe_reader = AdjListPropertyArrowChunkReader::Make(
      graph_info_, "person", "knows", "person", "creationDate",
      AdjListType::ordered_by_source);
  SKIP_WITH_ERROR_STATUS(state, maybe_reader.status());
  auto reader = maybe_reader.value();
  for (auto _ : state) {
    auto st = reader->seek(0);
    SKIP_WITH_ERROR_STATUS(state, st);
    auto chunk_result = reader->GetChunk();
    SKIP_WITH_ERROR_STATUS(state, chunk_result.status());
    auto next_result = reader->next_chunk();
    SKIP_WITH_ERROR_STATUS(state, next_result);
  }
}

BENCHMARK_DEFINE_F(
    BenchmarkFixture,
    VertexPropertyArrowChunkReaderReadChunk_firstGraph_AllColumns_V1)
(::benchmark::State& state) {  // NOLINT
  auto gp = graph_info_->GetVertexInfo("person")->GetPropertyGroup("firstName");
  auto maybe_reader =
      VertexPropertyArrowChunkReader::Make(graph_info_, "person", gp);
  SKIP_WITH_ERROR_STATUS(state, maybe_reader.status());
  auto reader = maybe_reader.value();
  for (auto _ : state) {
    auto st = reader->seek(0);
    SKIP_WITH_ERROR_STATUS(state, st);
    auto chunk_result = reader->GetChunk(GetChunkVersion::V1);
    SKIP_WITH_ERROR_STATUS(state, chunk_result.status());
    auto next_result = reader->next_chunk();
    SKIP_WITH_ERROR_STATUS(state, next_result);
  }
}
// select one columns and internal ID column
BENCHMARK_DEFINE_F(
    BenchmarkFixture,
    VertexPropertyArrowChunkReaderReadChunk_firstGraph_OneColumns_V1)
(::benchmark::State& state) {  // NOLINT
  auto maybe_reader =
      VertexPropertyArrowChunkReader::Make(graph_info_, "person", "firstName");
  SKIP_WITH_ERROR_STATUS(state, maybe_reader.status());
  auto reader = maybe_reader.value();
  for (auto _ : state) {
    auto st = reader->seek(0);
    SKIP_WITH_ERROR_STATUS(state, st);
    auto chunk_result = reader->GetChunk(GetChunkVersion::V1);
    SKIP_WITH_ERROR_STATUS(state, chunk_result.status());
    auto next_result = reader->next_chunk();
    SKIP_WITH_ERROR_STATUS(state, next_result);
  }
}

// select tow columns and internal ID column
BENCHMARK_DEFINE_F(
    BenchmarkFixture,
    VertexPropertyArrowChunkReaderReadChunk_firstGraph_TwoColumns_V1)
(::benchmark::State& state) {  // NOLINT
  auto maybe_reader = VertexPropertyArrowChunkReader::Make(
      graph_info_, "person", {"firstName", "lastName"}, SelectType::PROPERTIES);
  SKIP_WITH_ERROR_STATUS(state, maybe_reader.status());
  auto reader = maybe_reader.value();
  for (auto _ : state) {
    auto st = reader->seek(0);
    SKIP_WITH_ERROR_STATUS(state, st);
    auto chunk_result = reader->GetChunk(GetChunkVersion::V1);
    SKIP_WITH_ERROR_STATUS(state, chunk_result.status());
    auto next_result = reader->next_chunk();
    SKIP_WITH_ERROR_STATUS(state, next_result);
  }
}

BENCHMARK_DEFINE_F(
    BenchmarkFixture,
    VertexPropertyArrowChunkReaderReadChunk_firstGraph_AllColumns_V2)
(::benchmark::State& state) {  // NOLINT
  auto gp = graph_info_->GetVertexInfo("person")->GetPropertyGroup("firstName");
  auto maybe_reader =
      VertexPropertyArrowChunkReader::Make(graph_info_, "person", gp);
  SKIP_WITH_ERROR_STATUS(state, maybe_reader.status());
  auto reader = maybe_reader.value();
  for (auto _ : state) {
    auto st = reader->seek(0);
    SKIP_WITH_ERROR_STATUS(state, st);
    auto chunk_result = reader->GetChunk(GetChunkVersion::V2);
    SKIP_WITH_ERROR_STATUS(state, chunk_result.status());
    auto next_result = reader->next_chunk();
    SKIP_WITH_ERROR_STATUS(state, next_result);
  }
}
// select one columns and internal ID column
BENCHMARK_DEFINE_F(
    BenchmarkFixture,
    VertexPropertyArrowChunkReaderReadChunk_firstGraph_OneColumns_V2)
(::benchmark::State& state) {  // NOLINT
  auto maybe_reader =
      VertexPropertyArrowChunkReader::Make(graph_info_, "person", "firstName");
  SKIP_WITH_ERROR_STATUS(state, maybe_reader.status());
  auto reader = maybe_reader.value();
  for (auto _ : state) {
    auto st = reader->seek(0);
    SKIP_WITH_ERROR_STATUS(state, st);
    auto chunk_result = reader->GetChunk(GetChunkVersion::V2);
    SKIP_WITH_ERROR_STATUS(state, chunk_result.status());
    auto next_result = reader->next_chunk();
    SKIP_WITH_ERROR_STATUS(state, next_result);
  }
}

// select tow columns and internal ID column
BENCHMARK_DEFINE_F(
    BenchmarkFixture,
    VertexPropertyArrowChunkReaderReadChunk_firstGraph_TwoColumns_V2)
(::benchmark::State& state) {  // NOLINT
  auto maybe_reader = VertexPropertyArrowChunkReader::Make(
      graph_info_, "person", {"firstName", "lastName"}, SelectType::PROPERTIES);
  SKIP_WITH_ERROR_STATUS(state, maybe_reader.status());
  auto reader = maybe_reader.value();
  for (auto _ : state) {
    auto st = reader->seek(0);
    SKIP_WITH_ERROR_STATUS(state, st);
    auto chunk_result = reader->GetChunk(GetChunkVersion::V2);
    SKIP_WITH_ERROR_STATUS(state, chunk_result.status());
    auto next_result = reader->next_chunk();
    SKIP_WITH_ERROR_STATUS(state, next_result);
  }
}

BENCHMARK_DEFINE_F(
    BenchmarkFixture,
    VertexPropertyArrowChunkReaderReadChunk_secondGraph_AllColumns_V1)
(::benchmark::State& state) {  // NOLINT
  auto gp =
      second_graph_info_->GetVertexInfo("organisation")->GetPropertyGroup("id");
  auto maybe_reader = VertexPropertyArrowChunkReader::Make(second_graph_info_,
                                                           "organisation", gp);
  SKIP_WITH_ERROR_STATUS(state, maybe_reader.status());
  auto reader = maybe_reader.value();
  for (auto _ : state) {
    auto st = reader->seek(0);
    SKIP_WITH_ERROR_STATUS(state, st);
    auto chunk_result = reader->GetChunk(GetChunkVersion::V1);
    SKIP_WITH_ERROR_STATUS(state, chunk_result.status());
    auto next_result = reader->next_chunk();
    SKIP_WITH_ERROR_STATUS(state, next_result);
  }
}
// select one columns and internal ID column
BENCHMARK_DEFINE_F(
    BenchmarkFixture,
    VertexPropertyArrowChunkReaderReadChunk_secondGraph_OneColumns_V1)
(::benchmark::State& state) {  // NOLINT
  auto maybe_reader = VertexPropertyArrowChunkReader::Make(
      second_graph_info_, "organisation", "id");
  SKIP_WITH_ERROR_STATUS(state, maybe_reader.status());
  auto reader = maybe_reader.value();
  for (auto _ : state) {
    auto st = reader->seek(0);
    SKIP_WITH_ERROR_STATUS(state, st);
    auto chunk_result = reader->GetChunk(GetChunkVersion::V1);
    SKIP_WITH_ERROR_STATUS(state, chunk_result.status());
    auto next_result = reader->next_chunk();
    SKIP_WITH_ERROR_STATUS(state, next_result);
  }
}

// select tow columns and internal ID column
BENCHMARK_DEFINE_F(
    BenchmarkFixture,
    VertexPropertyArrowChunkReaderReadChunk_secondGraph_TwoColumns_V1)
(::benchmark::State& state) {  // NOLINT
  auto maybe_reader = VertexPropertyArrowChunkReader::Make(
      second_graph_info_, "organisation", {"id", "name"},
      SelectType::PROPERTIES);
  SKIP_WITH_ERROR_STATUS(state, maybe_reader.status());
  auto reader = maybe_reader.value();
  for (auto _ : state) {
    auto st = reader->seek(0);
    SKIP_WITH_ERROR_STATUS(state, st);
    auto chunk_result = reader->GetChunk(GetChunkVersion::V1);
    SKIP_WITH_ERROR_STATUS(state, chunk_result.status());
    auto next_result = reader->next_chunk();
    SKIP_WITH_ERROR_STATUS(state, next_result);
  }
}

BENCHMARK_DEFINE_F(
    BenchmarkFixture,
    VertexPropertyArrowChunkReaderReadChunk_secondGraph_AllColumns_V2)
(::benchmark::State& state) {  // NOLINT
  auto gp =
      second_graph_info_->GetVertexInfo("organisation")->GetPropertyGroup("id");
  auto maybe_reader = VertexPropertyArrowChunkReader::Make(second_graph_info_,
                                                           "organisation", gp);
  SKIP_WITH_ERROR_STATUS(state, maybe_reader.status());
  auto reader = maybe_reader.value();
  for (auto _ : state) {
    auto st = reader->seek(0);
    SKIP_WITH_ERROR_STATUS(state, st);
    auto chunk_result = reader->GetChunk(GetChunkVersion::V2);
    SKIP_WITH_ERROR_STATUS(state, chunk_result.status());
    auto next_result = reader->next_chunk();
    SKIP_WITH_ERROR_STATUS(state, next_result);
  }
}
// select one columns and internal ID column
BENCHMARK_DEFINE_F(
    BenchmarkFixture,
    VertexPropertyArrowChunkReaderReadChunk_secondGraph_OneColumns_V2)
(::benchmark::State& state) {  // NOLINT
  auto maybe_reader = VertexPropertyArrowChunkReader::Make(
      second_graph_info_, "organisation", "id");
  SKIP_WITH_ERROR_STATUS(state, maybe_reader.status());
  auto reader = maybe_reader.value();
  for (auto _ : state) {
    auto st = reader->seek(0);
    SKIP_WITH_ERROR_STATUS(state, st);
    auto chunk_result = reader->GetChunk(GetChunkVersion::V2);
    SKIP_WITH_ERROR_STATUS(state, chunk_result.status());
    auto next_result = reader->next_chunk();
    SKIP_WITH_ERROR_STATUS(state, next_result);
  }
}

// select tow columns and internal ID column
BENCHMARK_DEFINE_F(
    BenchmarkFixture,
    VertexPropertyArrowChunkReaderReadChunk_secondGraph_TwoColumns_V2)
(::benchmark::State& state) {  // NOLINT
  auto maybe_reader = VertexPropertyArrowChunkReader::Make(
      second_graph_info_, "organisation", {"id", "name"},
      SelectType::PROPERTIES);
  SKIP_WITH_ERROR_STATUS(state, maybe_reader.status());
  auto reader = maybe_reader.value();
  for (auto _ : state) {
    auto st = reader->seek(0);
    SKIP_WITH_ERROR_STATUS(state, st);
    auto chunk_result = reader->GetChunk(GetChunkVersion::V2);
    SKIP_WITH_ERROR_STATUS(state, chunk_result.status());
    auto next_result = reader->next_chunk();
    SKIP_WITH_ERROR_STATUS(state, next_result);
  }
}

BENCHMARK_REGISTER_F(BenchmarkFixture, CreateVertexPropertyArrowChunkReader);
BENCHMARK_REGISTER_F(BenchmarkFixture, CreateAdjListArrowChunkReader);
BENCHMARK_REGISTER_F(BenchmarkFixture, CreateAdjListOffsetArrowChunkReader);
BENCHMARK_REGISTER_F(BenchmarkFixture,
                     AdjListPropertyArrowChunkReaderReadChunk);
BENCHMARK_REGISTER_F(BenchmarkFixture, AdjListArrowChunkReaderReadChunk);
BENCHMARK_REGISTER_F(BenchmarkFixture, AdjListOffsetArrowChunkReaderReadChunk);
BENCHMARK_REGISTER_F(
    BenchmarkFixture,
    VertexPropertyArrowChunkReaderReadChunk_firstGraph_AllColumns_V1);
BENCHMARK_REGISTER_F(
    BenchmarkFixture,
    VertexPropertyArrowChunkReaderReadChunk_firstGraph_TwoColumns_V1);
BENCHMARK_REGISTER_F(
    BenchmarkFixture,
    VertexPropertyArrowChunkReaderReadChunk_firstGraph_OneColumns_V1);
BENCHMARK_REGISTER_F(
    BenchmarkFixture,
    VertexPropertyArrowChunkReaderReadChunk_firstGraph_AllColumns_V2);
BENCHMARK_REGISTER_F(
    BenchmarkFixture,
    VertexPropertyArrowChunkReaderReadChunk_firstGraph_TwoColumns_V2);
BENCHMARK_REGISTER_F(
    BenchmarkFixture,
    VertexPropertyArrowChunkReaderReadChunk_firstGraph_OneColumns_V2);
BENCHMARK_REGISTER_F(
    BenchmarkFixture,
    VertexPropertyArrowChunkReaderReadChunk_secondGraph_AllColumns_V1);
BENCHMARK_REGISTER_F(
    BenchmarkFixture,
    VertexPropertyArrowChunkReaderReadChunk_secondGraph_TwoColumns_V1);
BENCHMARK_REGISTER_F(
    BenchmarkFixture,
    VertexPropertyArrowChunkReaderReadChunk_secondGraph_OneColumns_V1);
BENCHMARK_REGISTER_F(
    BenchmarkFixture,
    VertexPropertyArrowChunkReaderReadChunk_secondGraph_AllColumns_V2);
BENCHMARK_REGISTER_F(
    BenchmarkFixture,
    VertexPropertyArrowChunkReaderReadChunk_secondGraph_TwoColumns_V2);
BENCHMARK_REGISTER_F(
    BenchmarkFixture,
    VertexPropertyArrowChunkReaderReadChunk_secondGraph_OneColumns_V2);
}  // namespace graphar
