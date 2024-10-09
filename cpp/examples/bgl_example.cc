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

#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/connected_components.hpp>
#include <boost/graph/graph_traits.hpp>

#include "arrow/api.h"

#include "./config.h"
#include "graphar/api/arrow_writer.h"
#include "graphar/api/high_level_reader.h"
#include "graphar/api/high_level_writer.h"

int main(int argc, char* argv[]) {
  // read file and construct graph info
  std::string path =
      GetTestingResourceRoot() + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
  auto graph_info = graphar::GraphInfo::Load(path).value();
  ASSERT(graph_info->GetVertexInfos().size() == 1);
  ASSERT(graph_info->GetEdgeInfos().size() == 1);

  // construct vertices collection
  std::string type = "person";
  ASSERT(graph_info->GetVertexInfo(type) != nullptr);
  auto maybe_vertices = graphar::VerticesCollection::Make(graph_info, type);
  ASSERT(maybe_vertices.status().ok());
  auto& vertices = maybe_vertices.value();
  int num_vertices = vertices->size();
  std::cout << "num_vertices: " << num_vertices << std::endl;

  // construct edges collection
  std::string src_type = "person", edge_type = "knows", dst_type = "person";
  auto maybe_edges =
      graphar::EdgesCollection::Make(graph_info, src_type, edge_type, dst_type,
                                     graphar::AdjListType::ordered_by_source);
  ASSERT(!maybe_edges.has_error());
  auto& edges = maybe_edges.value();

  // define the Graph type
  typedef boost::adjacency_list<
      boost::vecS,         // use vector to store edges
      boost::vecS,         // use vector to store vertices
      boost::undirectedS,  // options: directionalS/bidirectionalS/undirectedS
      boost::property<boost::vertex_name_t, int64_t>,  // vertex property
      boost::no_property>
      Graph;  // no edge property
  // descriptors for vertex and edge
  typedef typename boost::graph_traits<Graph>::vertex_descriptor Vertex;

  // declare a graph object with (num_vertices) vertices and a edge iterator
  std::vector<std::pair<graphar::IdType, graphar::IdType>> edges_array;
  auto it_end = edges->end();
  for (auto it = edges->begin(); it != it_end; ++it) {
    edges_array.push_back(std::make_pair(it.source(), it.destination()));
  }
  Graph g(edges_array.begin(), edges_array.end(), num_vertices);

  // access the vertex set
  typedef boost::property_map<Graph, boost::vertex_index_t>::type IndexMap;
  IndexMap index = boost::get(boost::vertex_index, g);
  typedef boost::graph_traits<Graph>::vertex_iterator vertex_iter;
  std::pair<vertex_iter, vertex_iter> vp;

  // add vertex properties (internal)
  boost::property_map<Graph, boost::vertex_name_t>::type id =
      get(boost::vertex_name_t(), g);

  auto v_it_end = vertices->end();
  for (auto it = vertices->begin(); it != v_it_end; ++it) {
    // FIXME(@acezen): double free error when get string property
    boost::put(id, it.id(), it.property<int64_t>("id").value());
  }

  // define vertex property (external)
  std::vector<int> component(num_vertices);
  // call algorithm: connected components
  int cc_num = boost::connected_components(g, &component[0]);
  std::cout << "Total number of components: " << cc_num << std::endl;
  for (vp = boost::vertices(g); vp.first != vp.second; ++vp.first) {
    Vertex v = *vp.first;
    std::cout << index[v] << " original id: " << boost::get(id, v)
              << ", component id: " << component[index[v]] << std::endl;
  }

  // method 1 for writing results: construct new vertex type and write results
  // using vertex builder construct new property group
  graphar::Property cc("cc", graphar::int32(), false);
  std::vector<graphar::Property> property_vector = {cc};
  auto group =
      graphar::CreatePropertyGroup(property_vector, graphar::FileType::PARQUET);
  // construct new vertex info
  std::string vertex_type = "cc_result", vertex_prefix = "result/";
  int chunk_size = 100;
  auto version = graphar::InfoVersion::Parse("gar/v1").value();
  auto new_info = graphar::CreateVertexInfo(vertex_type, chunk_size, {group},
                                            {}, vertex_prefix, version);
  // dump new vertex info
  ASSERT(new_info->IsValidated());
  ASSERT(new_info->Dump().status().ok());
  ASSERT(new_info->Save("/tmp/cc_result.vertex.yml").ok());
  // construct vertices builder
  graphar::builder::VerticesBuilder builder(new_info, "/tmp/");
  // add vertices to the builder
  for (vp = boost::vertices(g); vp.first != vp.second; ++vp.first) {
    Vertex v = *vp.first;
    graphar::builder::Vertex vertex(index[v]);
    vertex.AddProperty(cc.name, component[index[v]]);
    builder.AddVertex(vertex);
  }
  ASSERT(builder.GetNum() == num_vertices);
  // dump the results through builder
  ASSERT(builder.Dump().ok());

  // method 2 for writing results: extend the original vertex info and write
  // results using writer extend the vertex_info
  auto vertex_info = graph_info->GetVertexInfo(vertex_type);
  auto maybe_extend_info = vertex_info->AddPropertyGroup(group);
  ASSERT(maybe_extend_info.status().ok());
  auto extend_info = maybe_extend_info.value();
  // dump the extened vertex info
  ASSERT(extend_info->IsValidated());
  ASSERT(extend_info->Dump().status().ok());
  ASSERT(extend_info->Save("/tmp/person-new.vertex.yml").ok());
  // construct vertex property writer
  graphar::VertexPropertyWriter writer(extend_info, "/tmp/");
  // convert results to arrow::Table
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  std::vector<std::shared_ptr<arrow::Field>> schema_vector;
  schema_vector.push_back(arrow::field(
      cc.name, graphar::DataType::DataTypeToArrowDataType(cc.type)));
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  typename arrow::TypeTraits<arrow::Int32Type>::BuilderType array_builder(pool);
  ASSERT(array_builder.Reserve(num_vertices).ok());
  ASSERT(array_builder.AppendValues(component).ok());
  std::shared_ptr<arrow::Array> array = array_builder.Finish().ValueOrDie();
  arrays.push_back(array);
  auto schema = std::make_shared<arrow::Schema>(schema_vector);
  std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, arrays);
  // dump the results through writer
  ASSERT(writer.WriteTable(table, group, 0).ok());
}
