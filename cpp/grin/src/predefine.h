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

#ifndef CPP_GRIN_SRC_PREDEFINE_H_
#define CPP_GRIN_SRC_PREDEFINE_H_

#include <map>
#include <string>
#include <utility>
#include <vector>

extern "C" {
#include "grin/predefine.h"
}

#include "gar/graph.h"
#include "gar/graph_info.h"

#define GAR_ORDERED_BY_SOURCE GAR_NAMESPACE::AdjListType::ordered_by_source
#define GAR_ORDERED_BY_DEST GAR_NAMESPACE::AdjListType::ordered_by_dest
#define GAR_UNORDERED_BY_SOURCE GAR_NAMESPACE::AdjListType::unordered_by_source
#define GAR_UNORDERED_BY_DEST GAR_NAMESPACE::AdjListType::unordered_by_dest

struct GRIN_VERTEX_T {
  GAR_NAMESPACE::IdType id;
  unsigned type_id;
  std::optional<GAR_NAMESPACE::Vertex> vertex;
  GRIN_VERTEX_T(GAR_NAMESPACE::IdType _id, unsigned _type_id)
      : id(_id), type_id(_type_id) {}
  GRIN_VERTEX_T(GAR_NAMESPACE::IdType _id, unsigned _type_id,
                GAR_NAMESPACE::Vertex _vertex)
      : id(_id), type_id(_type_id), vertex(std::move(_vertex)) {}
};

struct GRIN_EDGE_T {
  GAR_NAMESPACE::Edge edge;
  unsigned type_id;
  GRIN_EDGE_T(GAR_NAMESPACE::Edge _edge, unsigned _type_id)
      : edge(std::move(_edge)), type_id(_type_id) {}
};

#ifdef GRIN_ENABLE_VERTEX_LIST

#ifdef GRIN_ENABLE_GRAPH_PARTITION
typedef enum {
  ALL_PARTITION = 0,
  ONE_PARTITION = 1,
  ALL_BUT_ONE_PARTITION = 2,
  PARTITION_TYPE_MAX = 3
} PARTITION_TYPE_IN_VERTEX_LIST;
#endif

struct GRIN_VERTEX_LIST_T {
  unsigned type_begin;
  unsigned type_end;
  PARTITION_TYPE_IN_VERTEX_LIST partition_type;
  unsigned partition_id;
  GRIN_VERTEX_LIST_T(
      unsigned _type_begin, unsigned _type_end,
      PARTITION_TYPE_IN_VERTEX_LIST _partition_type = ALL_PARTITION,
      unsigned _partition_id = 0)
      : type_begin(_type_begin),
        type_end(_type_end),
        partition_type(_partition_type),
        partition_id(_partition_id) {}
};
#endif

#ifdef GRIN_ENABLE_VERTEX_LIST_ITERATOR
struct GRIN_VERTEX_LIST_ITERATOR_T {
  unsigned type_begin;
  unsigned type_end;
  PARTITION_TYPE_IN_VERTEX_LIST partition_type;
  unsigned partition_id;
  unsigned current_type;
  GAR_NAMESPACE::IdType current_offset;
  GAR_NAMESPACE::VertexIter iter;
  GRIN_VERTEX_LIST_ITERATOR_T(unsigned _type_begin, unsigned _type_end,
                              PARTITION_TYPE_IN_VERTEX_LIST _partition_type,
                              unsigned _partition_id, unsigned _current_type,
                              GAR_NAMESPACE::IdType _current_offset,
                              GAR_NAMESPACE::VertexIter _iter)
      : type_begin(_type_begin),
        type_end(_type_end),
        partition_type(_partition_type),
        partition_id(_partition_id),
        current_type(_current_type),
        current_offset(_current_offset),
        iter(std::move(_iter)) {}
};
#endif

#ifdef GRIN_ENABLE_EDGE_LIST
struct GRIN_EDGE_LIST_T {
  unsigned type_begin;
  unsigned type_end;
  GRIN_EDGE_LIST_T(unsigned _type_begin, unsigned _type_end)
      : type_begin(_type_begin), type_end(_type_end) {}
};
#endif

#ifdef GRIN_ENABLE_EDGE_LIST_ITERATOR
struct GRIN_EDGE_LIST_ITERATOR_T {
  unsigned type_begin;
  unsigned type_end;
  unsigned current_type;
  GAR_NAMESPACE::EdgeIter iter;
  GRIN_EDGE_LIST_ITERATOR_T(unsigned _type_begin, unsigned _type_end,
                            unsigned _current_type,
                            GAR_NAMESPACE::EdgeIter _iter)
      : type_begin(_type_begin),
        type_end(_type_end),
        current_type(_current_type),
        iter(std::move(_iter)) {}
};
#endif

#ifdef GRIN_ENABLE_ADJACENT_LIST
struct GRIN_ADJACENT_LIST_T {
  GAR_NAMESPACE::IdType vid;
  unsigned vtype_id;
  GRIN_DIRECTION dir;
  unsigned etype_begin;
  unsigned etype_end;
  unsigned vtype_begin;
  unsigned vtype_end;
  GRIN_ADJACENT_LIST_T(GAR_NAMESPACE::IdType _vid, unsigned _vtype_id,
                       GRIN_DIRECTION _dir, unsigned _etype_begin,
                       unsigned _etype_end, unsigned _vtype_begin,
                       unsigned _vtype_end)
      : vid(_vid),
        vtype_id(_vtype_id),
        dir(_dir),
        etype_begin(_etype_begin),
        etype_end(_etype_end),
        vtype_begin(_vtype_begin),
        vtype_end(_vtype_end) {}
};
#endif

#ifdef GRIN_ENABLE_ADJACENT_LIST_ITERATOR
struct GRIN_ADJACENT_LIST_ITERATOR_T {
  GAR_NAMESPACE::IdType vid;
  unsigned vtype_id;
  GRIN_DIRECTION dir;
  unsigned etype_begin;
  unsigned etype_end;
  unsigned vtype_begin;
  unsigned vtype_end;
  unsigned current_etype;
  GAR_NAMESPACE::EdgeIter iter;
  GRIN_ADJACENT_LIST_ITERATOR_T(GAR_NAMESPACE::IdType _vid, unsigned _vtype_id,
                                GRIN_DIRECTION _dir, unsigned _etype_begin,
                                unsigned _etype_end, unsigned _vtype_begin,
                                unsigned _vtype_end, unsigned _current_etype,
                                GAR_NAMESPACE::EdgeIter _iter)
      : vid(_vid),
        vtype_id(_vtype_id),
        dir(_dir),
        etype_begin(_etype_begin),
        etype_end(_etype_end),
        vtype_begin(_vtype_begin),
        vtype_end(_vtype_end),
        current_etype(_current_etype),
        iter(std::move(_iter)) {}
};
#endif

#ifdef GRIN_WITH_VERTEX_PROPERTY
typedef std::vector<unsigned> GRIN_VERTEX_TYPE_LIST_T;
struct GRIN_VERTEX_PROPERTY_T {
  unsigned type_id;
  std::string name;
  GRIN_DATATYPE type;
  bool is_primary;
  GRIN_VERTEX_PROPERTY_T(unsigned _type_id, std::string _name,
                         GRIN_DATATYPE _type, bool _is_primary)
      : type_id(_type_id),
        name(std::move(_name)),
        type(_type),
        is_primary(_is_primary) {}
};
typedef std::vector<unsigned> GRIN_VERTEX_PROPERTY_LIST_T;
#endif

#ifdef GRIN_WITH_EDGE_PROPERTY
typedef std::vector<unsigned> GRIN_EDGE_TYPE_LIST_T;
struct GRIN_EDGE_PROPERTY_T {
  unsigned type_id;
  std::string name;
  GRIN_DATATYPE type;
  GRIN_EDGE_PROPERTY_T(unsigned _type_id, std::string _name,
                       GRIN_DATATYPE _type)
      : type_id(_type_id), name(std::move(_name)), type(_type) {}
  bool operator==(const GRIN_EDGE_PROPERTY_T& other) const {
    return type_id == other.type_id && name == other.name && type == other.type;
  }
  bool operator<(const GRIN_EDGE_PROPERTY_T& other) const {
    return type_id < other.type_id ||
           (type_id == other.type_id && name < other.name) ||
           (type_id == other.type_id && name == other.name &&
            type < other.type);
  }
};
typedef std::vector<unsigned> GRIN_EDGE_PROPERTY_LIST_T;
#endif

#if defined(GRIN_WITH_VERTEX_PROPERTY) || defined(GRIN_WITH_EDGE_PROPERTY)
typedef std::vector<std::any> GRIN_ROW_T;
#endif

#ifdef GRIN_ENABLE_GRAPH_PARTITION
typedef enum {
  SEGMENTED_PARTITION = 0,
  HASH_PARTITION = 1,
  PARTITION_STRATEGY_MAX = 2
} GAR_PARTITION_STRATEGY;

struct GRIN_PARTITIONED_GRAPH_T {
  std::string info_path;
  unsigned partition_num;
  GAR_PARTITION_STRATEGY partition_strategy;
  GRIN_PARTITIONED_GRAPH_T(
      std::string _info_path, unsigned _partition_num = 1,
      GAR_PARTITION_STRATEGY _partition_strategy = SEGMENTED_PARTITION)
      : info_path(_info_path),
        partition_num(_partition_num),
        partition_strategy(_partition_strategy) {}
};
typedef std::vector<unsigned> GRIN_PARTITION_LIST_T;
#endif

#ifdef GRIN_ENABLE_VERTEX_REF
struct GRIN_VERTEX_REF_T {
  GAR_NAMESPACE::IdType id;
  unsigned type_id;
  GRIN_VERTEX_REF_T(GAR_NAMESPACE::IdType _id, unsigned _type_id)
      : id(_id), type_id(_type_id) {}
};
#endif

struct GRIN_GRAPH_T {
  // statistics
  GAR_NAMESPACE::GraphInfo graph_info;
  size_t tot_vertex_num;
  size_t tot_edge_num;
  unsigned vertex_type_num;
  unsigned edge_type_num;
  unsigned unique_edge_type_num;
  std::vector<size_t> vertex_offsets, edge_num;
  // types
  std::vector<std::string> vertex_types, edge_types, unique_edge_types;
  std::map<std::string, unsigned> unique_edge_type_2_ids;
  std::vector<unsigned> src_type_ids, dst_type_ids, unique_edge_type_ids;
  std::vector<unsigned> unique_edge_type_begin_type;
  // vertices & edges
  std::vector<GAR_NAMESPACE::VerticesCollection> vertices_collections;
  std::vector<std::map<GAR_NAMESPACE::AdjListType, GAR_NAMESPACE::Edges>>
      edges_collections;
  // properties
  std::vector<GRIN_VERTEX_PROPERTY_T> vertex_properties;
  std::vector<GRIN_EDGE_PROPERTY_T> edge_properties;
  std::vector<unsigned> vertex_property_offsets, edge_property_offsets;
  std::vector<std::map<std::string, unsigned>> vertex_property_name_2_ids,
      edge_property_name_2_ids;
  // partitions
  unsigned partition_num;
  unsigned partition_id;
  GAR_PARTITION_STRATEGY partition_strategy;
  std::vector<size_t> vertex_chunk_size;
  std::vector<std::vector<size_t>> partitioned_vertex_offsets;
  // constructor
  explicit GRIN_GRAPH_T(GAR_NAMESPACE::GraphInfo graph_info_)
      : graph_info(std::move(graph_info_)),
        tot_vertex_num(0),
        tot_edge_num(0),
        vertex_type_num(0),
        edge_type_num(0),
        unique_edge_type_num(0),
        partition_num(1),
        partition_id(0),
        partition_strategy(SEGMENTED_PARTITION) {}
};

// basic functions
GRIN_GRAPH_T* get_graph_by_info_path(const std::string&);
std::string GetDataTypeName(GRIN_DATATYPE);
GRIN_DATATYPE GARToDataType(GAR_NAMESPACE::DataType);
size_t __grin_get_edge_num(GRIN_GRAPH_T*, unsigned, unsigned);

// graph init functions
void __grin_init_vertices_collections(GRIN_GRAPH_T*);
void __grin_init_edges_collections(GRIN_GRAPH_T*);
void __grin_init_vertex_properties(GRIN_GRAPH_T*);
void __grin_init_edge_properties(GRIN_GRAPH_T*);
void __grin_init_partitions(GRIN_GRAPH_T*, unsigned, unsigned,
                            GAR_PARTITION_STRATEGY);

// serialize & deserialize vertex
int64_t __grin_generate_int64_from_id_and_type(GAR_NAMESPACE::IdType, unsigned);
std::pair<GAR_NAMESPACE::IdType, unsigned>
    __grin_generate_id_and_type_from_int64(int64_t);

// mapping between vertex id and partitioned vertex id
GAR_NAMESPACE::IdType __grin_get_vertex_id_from_partitioned_vertex_id(
    GRIN_GRAPH_T*, unsigned, unsigned, GAR_PARTITION_STRATEGY,
    GAR_NAMESPACE::IdType);
GAR_NAMESPACE::IdType __grin_get_partitioned_vertex_id_from_vertex_id(
    GRIN_GRAPH_T*, unsigned, unsigned, GAR_PARTITION_STRATEGY,
    GAR_NAMESPACE::IdType);

// mapping between vertices with partitions
unsigned __grin_get_master_partition_id(GRIN_GRAPH_T*, GAR_NAMESPACE::IdType,
                                        unsigned);
size_t __grin_get_paritioned_vertex_num(GRIN_GRAPH_T*, unsigned, unsigned,
                                        GAR_PARTITION_STRATEGY);
GAR_NAMESPACE::IdType __grin_get_first_vertex_id_in_partition(
    GRIN_GRAPH_T*, unsigned, unsigned, GAR_PARTITION_STRATEGY);
GAR_NAMESPACE::IdType __grin_get_next_vertex_id_in_partition(
    GRIN_GRAPH_T*, unsigned, unsigned, GAR_PARTITION_STRATEGY,
    GAR_NAMESPACE::IdType);

#endif  // CPP_GRIN_SRC_PREDEFINE_H_
