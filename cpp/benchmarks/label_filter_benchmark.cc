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
#include "graphar/api/high_level_reader.h"
#include "graphar/api/info.h"

namespace graphar {

std::shared_ptr<graphar::VerticesCollection> SingleLabelFilter(
    const std::shared_ptr<graphar::GraphInfo>& graph_info) {
  std::string type = "organisation";
  auto vertex_info = graph_info->GetVertexInfo(type);
  auto labels = vertex_info->GetLabels();
  std::string filter_label = "university";
  auto maybe_filter_vertices_collection =
      VerticesCollection::verticesWithLabel(filter_label, graph_info, type);
  auto filter_vertices = maybe_filter_vertices_collection.value();
  return filter_vertices;
}

void SingleLabelFilterbyAcero(
    const std::shared_ptr<graphar::GraphInfo>& graph_info) {
  std::string type = "organisation";
  auto vertex_info = graph_info->GetVertexInfo(type);
  auto labels = vertex_info->GetLabels();
  std::string filter_label = "university";
  auto maybe_filter_vertices_collection =
      VerticesCollection::verticesWithLabelbyAcero(filter_label, graph_info,
                                                   type);
  auto filter_vertices = maybe_filter_vertices_collection.value();
}

void MultiLabelFilter(const std::shared_ptr<graphar::GraphInfo>& graph_info) {
  std::string type = "organisation";
  auto vertex_info = graph_info->GetVertexInfo(type);
  auto labels = vertex_info->GetLabels();
  std::vector<std::string> filter_label = {"university", "company"};
  auto maybe_filter_vertices_collection =
      VerticesCollection::verticesWithMultipleLabels(filter_label, graph_info,
                                                     type);
  auto filter_vertices = maybe_filter_vertices_collection.value();
}

void MultiLabelFilterbyAcero(
    const std::shared_ptr<graphar::GraphInfo>& graph_info) {
  std::string type = "organisation";
  auto vertex_info = graph_info->GetVertexInfo(type);
  auto labels = vertex_info->GetLabels();
  std::vector<std::string> filter_label = {"university", "company"};
  auto maybe_filter_vertices_collection =
      VerticesCollection::verticesWithMultipleLabelsbyAcero(filter_label,
                                                            graph_info, type);
  auto filter_vertices = maybe_filter_vertices_collection.value();
}

std::shared_ptr<graphar::VerticesCollection> LabelFilterFromSet(
    const std::shared_ptr<graphar::GraphInfo>& graph_info,
    const std::shared_ptr<VerticesCollection>& vertices_collection) {
  std::string type = "organisation";
  auto vertex_info = graph_info->GetVertexInfo(type);
  auto labels = vertex_info->GetLabels();
  std::vector<std::string> filter_label = {"company", "public"};
  auto maybe_filter_vertices_collection =
      VerticesCollection::verticesWithMultipleLabels(filter_label,
                                                     vertices_collection);
  auto filter_vertices = maybe_filter_vertices_collection.value();
  return filter_vertices;
}

BENCHMARK_DEFINE_F(BenchmarkFixture, SingleLabelFilter)
(::benchmark::State& state) {  // NOLINT
  for (auto _ : state) {
    SingleLabelFilter(second_graph_info_);
  }
}

BENCHMARK_DEFINE_F(BenchmarkFixture, SingleLabelFilterbyAcero)
(::benchmark::State& state) {  // NOLINT
  for (auto _ : state) {
    SingleLabelFilterbyAcero(second_graph_info_);
  }
}

BENCHMARK_DEFINE_F(BenchmarkFixture, MultiLabelFilter)
(::benchmark::State& state) {  // NOLINT
  for (auto _ : state) {
    MultiLabelFilter(second_graph_info_);
  }
}

BENCHMARK_DEFINE_F(BenchmarkFixture, MultiLabelFilterbyAcero)
(::benchmark::State& state) {  // NOLINT
  for (auto _ : state) {
    MultiLabelFilterbyAcero(second_graph_info_);
  }
}

BENCHMARK_DEFINE_F(BenchmarkFixture, LabelFilterFromSet)
(::benchmark::State& state) {  // NOLINT
  for (auto _ : state) {
    state.PauseTiming();
    auto vertices_collection = SingleLabelFilter(second_graph_info_);
    auto vertices_collection_2 =
        LabelFilterFromSet(second_graph_info_, vertices_collection);
    state.ResumeTiming();
    LabelFilterFromSet(second_graph_info_, vertices_collection_2);
  }
}

BENCHMARK_REGISTER_F(BenchmarkFixture, SingleLabelFilter)->Iterations(10);
BENCHMARK_REGISTER_F(BenchmarkFixture, SingleLabelFilterbyAcero)
    ->Iterations(10);
BENCHMARK_REGISTER_F(BenchmarkFixture, MultiLabelFilter)->Iterations(10);
BENCHMARK_REGISTER_F(BenchmarkFixture, MultiLabelFilterbyAcero)->Iterations(10);
BENCHMARK_REGISTER_F(BenchmarkFixture, LabelFilterFromSet)->Iterations(10);

}  // namespace graphar
