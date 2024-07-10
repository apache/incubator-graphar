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

namespace graphar {

BENCHMARK_DEFINE_F(BenchmarkFixture, CreateVertexPropertyArrowChunkReader)
(::benchmark::State& state) {  // NOLINT
  for (auto _ : state) {
    auto maybe_reader = VertexPropertyArrowChunkReader::Make(
        graph_info_, "person", "firstName");
    if (maybe_reader.has_error()) {
      state.SkipWithError(maybe_reader.status().message().c_str());
      return;
    }
  }
}

BENCHMARK_DEFINE_F(BenchmarkFixture, CreateAdjListArrowChunkReader)
(::benchmark::State& state) {  // NOLINT
  for (auto _ : state) {
    auto maybe_reader =
        AdjListArrowChunkReader::Make(graph_info_, "person", "knows", "person",
                                      AdjListType::ordered_by_source);
    if (maybe_reader.has_error()) {
      state.SkipWithError(maybe_reader.status().message().c_str());
      return;
    }
  }
}

BENCHMARK_DEFINE_F(BenchmarkFixture, CreateAdjListOffsetArrowChunkReader)
(::benchmark::State& state) {  // NOLINT
  for (auto _ : state) {
    auto maybe_reader = AdjListOffsetArrowChunkReader::Make(
        graph_info_, "person", "knows", "person",
        AdjListType::ordered_by_source);
    if (maybe_reader.has_error()) {
      state.SkipWithError(maybe_reader.status().message().c_str());
      return;
    }
  }
}

BENCHMARK_DEFINE_F(BenchmarkFixture, CreateAdjListPropertyArrowChunkReader)
(::benchmark::State& state) {  // NOLINT
  for (auto _ : state) {
    auto maybe_reader = AdjListPropertyArrowChunkReader::Make(
        graph_info_, "person", "knows", "person", "creationDate",
        AdjListType::ordered_by_source);
    if (maybe_reader.has_error()) {
      state.SkipWithError(maybe_reader.status().message().c_str());
      return;
    }
  }
}

BENCHMARK_DEFINE_F(BenchmarkFixture, VertexPropertyArrowChunkReaderReadChunk)
(::benchmark::State& state) {  // NOLINT
  auto maybe_reader =
      VertexPropertyArrowChunkReader::Make(graph_info_, "person", "firstName");
  if (maybe_reader.has_error()) {
    state.SkipWithError(maybe_reader.status().message().c_str());
    return;
  }
  auto reader = maybe_reader.value();
  for (auto _ : state) {
    assert(reader->seek(0).ok());
    assert(reader->GetChunk().status().ok());
    assert(reader->next_chunk().ok());
  }
}

BENCHMARK_DEFINE_F(BenchmarkFixture, AdjListArrowChunkReaderReadChunk)
(::benchmark::State& state) {  // NOLINT
  auto maybe_reader = AdjListArrowChunkReader::Make(
      graph_info_, "person", "knows", "person", AdjListType::ordered_by_source);
  if (maybe_reader.has_error()) {
    state.SkipWithError(maybe_reader.status().message().c_str());
    return;
  }
  auto reader = maybe_reader.value();
  for (auto _ : state) {
    assert(reader->seek(0).ok());
    assert(reader->GetChunk().status().ok());
    assert(reader->next_chunk().ok());
  }
}

BENCHMARK_DEFINE_F(BenchmarkFixture, AdjListOffsetArrowChunkReaderReadChunk)
(::benchmark::State& state) {  // NOLINT
  auto maybe_reader = AdjListOffsetArrowChunkReader::Make(
      graph_info_, "person", "knows", "person", AdjListType::ordered_by_source);
  if (maybe_reader.has_error()) {
    state.SkipWithError(maybe_reader.status().message().c_str());
    return;
  }
  auto reader = maybe_reader.value();
  for (auto _ : state) {
    assert(reader->seek(0).ok());
    assert(reader->GetChunk().status().ok());
    assert(reader->next_chunk().ok());
  }
}

BENCHMARK_DEFINE_F(BenchmarkFixture, AdjListPropertyArrowChunkReaderReadChunk)
(::benchmark::State& state) {  // NOLINT
  auto maybe_reader = AdjListPropertyArrowChunkReader::Make(
      graph_info_, "person", "knows", "person", "creationDate",
      AdjListType::ordered_by_source);
  if (maybe_reader.has_error()) {
    state.SkipWithError(maybe_reader.status().message().c_str());
    return;
  }
  auto reader = maybe_reader.value();
  for (auto _ : state) {
    assert(reader->seek(0).ok());
    assert(reader->GetChunk().status().ok());
    assert(reader->next_chunk().ok());
  }
}

BENCHMARK_REGISTER_F(BenchmarkFixture, CreateVertexPropertyArrowChunkReader);
BENCHMARK_REGISTER_F(BenchmarkFixture, CreateAdjListArrowChunkReader);
BENCHMARK_REGISTER_F(BenchmarkFixture, CreateAdjListOffsetArrowChunkReader);
BENCHMARK_REGISTER_F(BenchmarkFixture,
                     AdjListPropertyArrowChunkReaderReadChunk);

BENCHMARK_REGISTER_F(BenchmarkFixture, VertexPropertyArrowChunkReaderReadChunk);
BENCHMARK_REGISTER_F(BenchmarkFixture, AdjListArrowChunkReaderReadChunk);
BENCHMARK_REGISTER_F(BenchmarkFixture, AdjListOffsetArrowChunkReaderReadChunk);
BENCHMARK_REGISTER_F(BenchmarkFixture, AdjListOffsetArrowChunkReaderReadChunk);
}  // namespace graphar
