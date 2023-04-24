/** Copyright 2022 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#include "grin/include/topology/structure.h"
#include "grin/src/predefine.h"

GRIN_GRAPH grin_get_graph_from_storage(int argc, char** argv) {
  if (argc < 1)
    return GRIN_NULL_GRAPH;
  return get_graph_by_info_path(std::string(argv[0]));
}

void grin_destroy_graph(GRIN_GRAPH g) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  delete _g;
}

#if defined(GRIN_ASSUME_HAS_DIRECTED_GRAPH) && \
    defined(GRIN_ASSUME_HAS_UNDIRECTED_GRAPH)
bool grin_is_directed(GRIN_GRAPH);
#endif

#ifdef GRIN_ASSUME_HAS_MULTI_EDGE_GRAPH
bool grin_is_multigraph(GRIN_GRAPH g) { return true; }
#endif

size_t grin_get_vertex_num(GRIN_GRAPH g) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  size_t result = _g->tot_vertex_num;
  return result;
}

size_t grin_get_edge_num(GRIN_GRAPH g) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  size_t result = _g->tot_edge_num;
  return result;
}

// Vertex
void grin_destroy_vertex(GRIN_GRAPH g, GRIN_VERTEX v) {
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  delete _v;
}

bool grin_equal_vertex(GRIN_GRAPH g, GRIN_VERTEX v1, GRIN_VERTEX v2) {
  auto _v1 = static_cast<GRIN_VERTEX_T*>(v1);
  auto _v2 = static_cast<GRIN_VERTEX_T*>(v2);
  return _v1->id == _v2->id && _v1->type_id == _v2->type_id;
}

#ifdef GRIN_WITH_VERTEX_ORIGINAL_ID
void grin_destroy_vertex_original_id(GRIN_GRAPH g,
                                     GRIN_VERTEX_ORIGINAL_ID oid) {
  auto _oid = static_cast<GRIN_VERTEX_ORIGINAL_ID_T*>(oid);
  delete _oid;
}

GRIN_DATATYPE grin_get_vertex_original_id_type(GRIN_GRAPH g) {
  return GRIN_DATATYPE::Int64;
}

GRIN_VERTEX_ORIGINAL_ID grin_get_vertex_original_id(GRIN_GRAPH g,
                                                    GRIN_VERTEX v) {
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  return new GRIN_VERTEX_ORIGINAL_ID_T(_v->id);
}
#endif

#if defined(GRIN_WITH_VERTEX_ORIGINAL_ID) && \
    !defined(GRIN_ASSUME_BY_TYPE_VERTEX_ORIGINAL_ID)
GRIN_VERTEX grin_get_vertex_from_original_id(GRIN_GRAPH,
                                             GRIN_VERTEX_ORIGINAL_ID);
#endif

// Data
void grin_destroy_value(GRIN_GRAPH g, GRIN_DATATYPE type, const void* value) {
  switch (type) {
  case GRIN_DATATYPE::Int32:
    delete static_cast<int32_t const*>(value);
    break;
  case GRIN_DATATYPE::UInt32:
    delete static_cast<uint32_t const*>(value);
    break;
  case GRIN_DATATYPE::Int64:
    delete static_cast<int64_t const*>(value);
    break;
  case GRIN_DATATYPE::UInt64:
    delete static_cast<uint64_t const*>(value);
    break;
  case GRIN_DATATYPE::Float:
    delete static_cast<float const*>(value);
    break;
  case GRIN_DATATYPE::Double:
    delete static_cast<double const*>(value);
    break;
  case GRIN_DATATYPE::String:
    delete static_cast<std::string const*>(value);
    break;
  case GRIN_DATATYPE::Date32:
    delete static_cast<int32_t const*>(value);
    break;
  case GRIN_DATATYPE::Date64:
    delete static_cast<int64_t const*>(value);
    break;
  default:
    return;
  }
}

void grin_destroy_name(GRIN_GRAPH g, const char* name) { delete[] name; }

#ifdef GRIN_WITH_VERTEX_DATA
GRIN_DATATYPE grin_get_vertex_data_type(GRIN_GRAPH, GRIN_VERTEX);

const void* grin_get_vertex_data_value(GRIN_GRAPH, GRIN_VERTEX);
#endif

// Edge
void grin_destroy_edge(GRIN_GRAPH g, GRIN_EDGE e) {
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  delete _e;
}

GRIN_VERTEX grin_get_edge_src(GRIN_GRAPH g, GRIN_EDGE e) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto v = new GRIN_VERTEX_T(_e->edge.source(), _g->src_type_ids[_e->type_id]);
  return v;
}

GRIN_VERTEX grin_get_edge_dst(GRIN_GRAPH g, GRIN_EDGE e) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto v =
      new GRIN_VERTEX_T(_e->edge.destination(), _g->dst_type_ids[_e->type_id]);
  return v;
}

#ifdef GRIN_WITH_EDGE_DATA
GRIN_DATATYPE grin_get_edge_data_type(GRIN_GRAPH, GRIN_EDGE);

const void* grin_get_edge_data_value(GRIN_GRAPH, GRIN_EDGE);
#endif
