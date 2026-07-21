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

#include <random>
#include <string>
#include <vector>

#include "benchmark/benchmark.h"

#include "./benchmark_util.h"
#include "graphar/api/arrow_reader.h"
#include "graphar/fwd.h"
#include "graphar/lru_cache.h"

namespace graphar {

// ============================================================================
// Raw LRU Cache benchmarks
// ============================================================================

// Benchmark: LRU Cache Put operation with varying cache sizes
static void BM_LRUCachePut(benchmark::State& state) {
  const int64_t capacity = state.range(0);
  const int64_t num_ops = state.range(1);
  LRUCache<int64_t, std::string> cache(capacity);

  int64_t i = 0;
  for (auto _ : state) {
    cache.Put(i % num_ops, "value_" + std::to_string(i % num_ops));
    ++i;
  }
  state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_LRUCachePut)
    ->Args({4, 1000})
    ->Args({4, 10000})
    ->Args({16, 1000})
    ->Args({16, 10000})
    ->Args({64, 1000})
    ->Args({64, 10000});

// Benchmark: LRU Cache Get with 100% hit rate
static void BM_LRUCacheGetHit(benchmark::State& state) {
  const int64_t capacity = state.range(0);
  LRUCache<int64_t, std::string> cache(capacity);
  for (int64_t i = 0; i < capacity; ++i) {
    cache.Put(i, "value_" + std::to_string(i));
  }

  int64_t i = 0;
  for (auto _ : state) {
    auto* v = cache.Get(i % capacity);
    benchmark::DoNotOptimize(v);
    ++i;
  }
  state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_LRUCacheGetHit)->Arg(4)->Arg(16)->Arg(64)->Arg(256);

// Benchmark: LRU Cache Get with 100% miss rate
static void BM_LRUCacheGetMiss(benchmark::State& state) {
  const int64_t capacity = state.range(0);
  LRUCache<int64_t, std::string> cache(capacity);
  for (int64_t i = 0; i < capacity; ++i) {
    cache.Put(i, "value_" + std::to_string(i));
  }

  int64_t i = capacity;
  for (auto _ : state) {
    auto* v = cache.Get(i);
    benchmark::DoNotOptimize(v);
    ++i;
  }
  state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_LRUCacheGetMiss)->Arg(4)->Arg(16)->Arg(64)->Arg(256);

// Benchmark: LRU Cache Mixed operations (80% reads, 20% writes)
static void BM_LRUCacheMixed(benchmark::State& state) {
  const int64_t capacity = state.range(0);
  const int64_t key_space = state.range(1);
  LRUCache<int64_t, std::string> cache(capacity);

  // Pre-fill to 50% capacity
  for (int64_t i = 0; i < capacity / 2; ++i) {
    cache.Put(i, "value_" + std::to_string(i));
  }

  int64_t i = capacity / 2;
  for (auto _ : state) {
    int64_t key = i % key_space;
    if (key % 5 == 0) {
      // 20% writes
      cache.Put(key, "updated_" + std::to_string(key));
    } else {
      // 80% reads
      auto* v = cache.Get(key);
      benchmark::DoNotOptimize(v);
    }
    ++i;
  }
  state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_LRUCacheMixed)
    ->Args({4, 100})
    ->Args({16, 100})
    ->Args({64, 1000})
    ->Args({256, 1000});

// ============================================================================
// Chunk Reader LRU Cache benchmarks
// These benchmarks measure the performance impact of the LRU cache within
// the chunk readers.  The cache is always compiled in; whether it is
// effective is controlled by the cache capacity (set via FilterOptions or
// the reader constructor).  When capacity is 0 or 1, caching is
// effectively disabled (every access reads from the filesystem).
// ============================================================================

// Benchmark: VertexPropertyArrowChunkReader - repeated access to same chunk
BENCHMARK_DEFINE_F(BenchmarkFixture, VertexPropertyChunkReaderCacheHit)
(::benchmark::State& state) {  // NOLINT
  auto gp = graph_info_->GetVertexInfo("person")->GetPropertyGroup("firstName");
  auto maybe_reader =
      VertexPropertyArrowChunkReader::Make(graph_info_, "person", gp);
  SKIP_WITH_ERROR_STATUS(state, maybe_reader.status());
  auto reader = maybe_reader.value();

  for (auto _ : state) {
    // Seek back to chunk 0 repeatedly - should hit cache on subsequent calls
    auto st = reader->seek(0);
    SKIP_WITH_ERROR_STATUS(state, st);
    auto chunk_result = reader->GetChunk();
    SKIP_WITH_ERROR_STATUS(state, chunk_result.status());
  }
  state.SetItemsProcessed(state.iterations());
}

// Benchmark: VertexPropertyArrowChunkReader - sequential scan (interleaved
// access)
BENCHMARK_DEFINE_F(BenchmarkFixture,
                   VertexPropertyChunkReaderSequentialScanWithRepeat)
(::benchmark::State& state) {  // NOLINT
  auto gp = graph_info_->GetVertexInfo("person")->GetPropertyGroup("firstName");
  auto maybe_reader =
      VertexPropertyArrowChunkReader::Make(graph_info_, "person", gp);
  SKIP_WITH_ERROR_STATUS(state, maybe_reader.status());
  auto reader = maybe_reader.value();

  // Read first 4 chunks, then repeat - exercises LRU with capacity=4
  for (auto _ : state) {
    for (int64_t chunk = 0; chunk < 4; ++chunk) {
      auto st = reader->seek(static_cast<IdType>(chunk));
      SKIP_WITH_ERROR_STATUS(state, st);
      auto chunk_result = reader->GetChunk();
      SKIP_WITH_ERROR_STATUS(state, chunk_result.status());
    }
    // Second pass through same chunks - should be all cache hits
    for (int64_t chunk = 0; chunk < 4; ++chunk) {
      auto st = reader->seek(static_cast<IdType>(chunk));
      SKIP_WITH_ERROR_STATUS(state, st);
      auto chunk_result = reader->GetChunk();
      SKIP_WITH_ERROR_STATUS(state, chunk_result.status());
    }
  }
  state.SetItemsProcessed(state.iterations() * 8);
}

// Benchmark: AdjListArrowChunkReader - repeated seek to same vertex
BENCHMARK_DEFINE_F(BenchmarkFixture, AdjListChunkReaderCacheHit)
(::benchmark::State& state) {  // NOLINT
  auto maybe_reader = AdjListArrowChunkReader::Make(
      graph_info_, "person", "knows", "person", AdjListType::ordered_by_source);
  SKIP_WITH_ERROR_STATUS(state, maybe_reader.status());
  auto reader = maybe_reader.value();

  for (auto _ : state) {
    // Seek to the same source vertex repeatedly - should hit cache
    auto st = reader->seek_src(0);
    SKIP_WITH_ERROR_STATUS(state, st);
    auto chunk_result = reader->GetChunk();
    SKIP_WITH_ERROR_STATUS(state, chunk_result.status());
  }
  state.SetItemsProcessed(state.iterations());
}

// Benchmark: AdjListArrowChunkReader - iterate through vertex chunks and
// re-visit the first one
BENCHMARK_DEFINE_F(BenchmarkFixture,
                   AdjListChunkReaderIterateThenRepeat)
(::benchmark::State& state) {  // NOLINT
  auto maybe_reader = AdjListArrowChunkReader::Make(
      graph_info_, "person", "knows", "person", AdjListType::ordered_by_source);
  SKIP_WITH_ERROR_STATUS(state, maybe_reader.status());
  auto reader = maybe_reader.value();

  for (auto _ : state) {
    // Visit vertex chunks 0-3 sequentially
    for (IdType v = 0; v < 4; ++v) {
      auto st = reader->seek_src(v);
      SKIP_WITH_ERROR_STATUS(state, st);
      auto chunk_result = reader->GetChunk();
      SKIP_WITH_ERROR_STATUS(state, chunk_result.status());
    }
    // Re-visit chunk 0 - should be evicted and cause cache miss,
    // demonstrating LRU eviction behavior with capacity=4
    {
      auto st = reader->seek_src(0);
      SKIP_WITH_ERROR_STATUS(state, st);
      auto chunk_result = reader->GetChunk();
      SKIP_WITH_ERROR_STATUS(state, chunk_result.status());
    }
  }
  state.SetItemsProcessed(state.iterations() * 5);
}

// Benchmark: AdjListOffsetArrowChunkReader - repeated offset reads
BENCHMARK_DEFINE_F(BenchmarkFixture, AdjListOffsetChunkReaderCacheHit)
(::benchmark::State& state) {  // NOLINT
  auto maybe_reader = AdjListOffsetArrowChunkReader::Make(
      graph_info_, "person", "knows", "person",
      AdjListType::ordered_by_source);
  SKIP_WITH_ERROR_STATUS(state, maybe_reader.status());
  auto reader = maybe_reader.value();

  for (auto _ : state) {
    auto st = reader->seek(0);
    SKIP_WITH_ERROR_STATUS(state, st);
    auto chunk_result = reader->GetChunk();
    SKIP_WITH_ERROR_STATUS(state, chunk_result.status());
  }
  state.SetItemsProcessed(state.iterations());
}

// Benchmark: AdjListPropertyArrowChunkReader - repeated property reads
BENCHMARK_DEFINE_F(BenchmarkFixture, AdjListPropertyChunkReaderCacheHit)
(::benchmark::State& state) {  // NOLINT
  auto maybe_reader = AdjListPropertyArrowChunkReader::Make(
      graph_info_, "person", "knows", "person", "creationDate",
      AdjListType::ordered_by_source);
  SKIP_WITH_ERROR_STATUS(state, maybe_reader.status());
  auto reader = maybe_reader.value();

  for (auto _ : state) {
    auto st = reader->seek_src(0);
    SKIP_WITH_ERROR_STATUS(state, st);
    auto chunk_result = reader->GetChunk();
    SKIP_WITH_ERROR_STATUS(state, chunk_result.status());
  }
  state.SetItemsProcessed(state.iterations());
}

// ============================================================================
// Register all benchmarks
// ============================================================================

BENCHMARK_REGISTER_F(BenchmarkFixture, VertexPropertyChunkReaderCacheHit);
BENCHMARK_REGISTER_F(BenchmarkFixture,
                     VertexPropertyChunkReaderSequentialScanWithRepeat);
BENCHMARK_REGISTER_F(BenchmarkFixture, AdjListChunkReaderCacheHit);
BENCHMARK_REGISTER_F(BenchmarkFixture,
                     AdjListChunkReaderIterateThenRepeat);
BENCHMARK_REGISTER_F(BenchmarkFixture, AdjListOffsetChunkReaderCacheHit);
BENCHMARK_REGISTER_F(BenchmarkFixture, AdjListPropertyChunkReaderCacheHit);

}  // namespace graphar
