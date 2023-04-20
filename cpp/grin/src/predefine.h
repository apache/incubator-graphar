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

#include "grin/predefine.h"

#include "gar/graph.h"
#include "gar/graph_info.h"

struct GRIN_VERTEX_T {
  GAR_NAMESPACE::IdType id;
  unsigned type_id;
  GRIN_VERTEX_T(GAR_NAMESPACE::IdType _id, unsigned _type_id)
      : id(_id), type_id(_type_id) {}
};

typedef GAR_NAMESPACE::IdType GRIN_VERTEX_ORIGINAL_ID_T;

struct GRIN_EDGE_T {
  GAR_NAMESPACE::EdgeIter iter;
  unsigned type_id;
  GRIN_EDGE_T(GAR_NAMESPACE::EdgeIter _iter, unsigned _type_id)
      : iter(_iter), type_id(_type_id) {}
};

struct GRIN_GRAPH_T {
  GAR_NAMESPACE::GraphInfo graph_info;
  size_t tot_vertex_num;
  size_t tot_edge_num;
  size_t vertex_type_num;
  size_t edge_type_num;
  size_t unique_edge_type_num;
  std::vector<size_t> vertex_offsets, edge_num;
  std::vector<std::string> vertex_types, edge_types, unique_edge_types;
  std::map<std::string, unsigned> unique_edge_type_2_ids;
  std::vector<unsigned> src_type_ids, dst_type_ids, unique_edge_type_ids;
  std::vector<GAR_NAMESPACE::VerticesCollection> vertices_collections;
  std::vector<std::map<GAR_NAMESPACE::AdjListType, GAR_NAMESPACE::Edges>>
      edges_collections;
  explicit GRIN_GRAPH_T(GAR_NAMESPACE::GraphInfo graph_info_)
      : graph_info(graph_info_),
        tot_vertex_num(0),
        tot_edge_num(0),
        vertex_type_num(0),
        edge_type_num(0),
        unique_edge_type_num(0) {}
};

GRIN_GRAPH get_graph_by_info_path(const std::string&);
std::string GetDataTypeName(GRIN_DATATYPE);
GRIN_DATATYPE GARToDataType(GAR_NAMESPACE::DataType);
size_t __grin_get_edge_num(GRIN_GRAPH_T*, unsigned, unsigned);

#ifdef GRIN_ENABLE_VERTEX_LIST
struct GRIN_VERTEX_LIST_T {
  unsigned type_begin;
  unsigned type_end;
  GRIN_VERTEX_LIST_T(unsigned _type_begin, unsigned _type_end)
      : type_begin(_type_begin), type_end(_type_end) {}
};
#endif

#ifdef GRIN_ENABLE_VERTEX_LIST_ITERATOR
struct GRIN_VERTEX_LIST_ITERATOR_T {
  unsigned type_begin;
  unsigned type_end;
  unsigned current_type;
  GAR_NAMESPACE::IdType current_offset;
  GRIN_VERTEX_LIST_ITERATOR_T(unsigned _type_begin, unsigned _type_end,
                              unsigned _current_type,
                              GAR_NAMESPACE::IdType _current_offset)
      : type_begin(_type_begin),
        type_end(_type_end),
        current_type(_current_type),
        current_offset(_current_offset) {}
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
        iter(_iter) {}
};
#endif

#ifdef GRIN_ENABLE_ADJACENT_LIST
struct GRIN_ADJACENT_LIST_T {
  GRIN_VERTEX_T v;
  GRIN_DIRECTION dir;
  unsigned etype_begin;
  unsigned etype_end;
  unsigned vtype_begin;
  unsigned vtype_end;
  GRIN_ADJACENT_LIST_T(GRIN_VERTEX_T _v, GRIN_DIRECTION _dir,
                       unsigned _etype_begin, unsigned _etype_end,
                       unsigned _vtype_begin, unsigned _vtype_end)
      : v(_v),
        dir(_dir),
        etype_begin(_etype_begin),
        etype_end(_etype_end),
        vtype_begin(_vtype_begin),
        vtype_end(_vtype_end) {}
};
#endif

#ifdef GRIN_ENABLE_ADJACENT_LIST_ITERATOR
struct GRIN_ADJACENT_LIST_ITERATOR_T {
  GRIN_VERTEX_T v;
  GRIN_DIRECTION dir;
  unsigned etype_begin;
  unsigned etype_end;
  unsigned vtype_begin;
  unsigned vtype_end;
  unsigned current_etype;
  GAR_NAMESPACE::EdgeIter iter;
  GRIN_ADJACENT_LIST_ITERATOR_T(GRIN_VERTEX_T _v, GRIN_DIRECTION _dir,
                                unsigned _etype_begin, unsigned _etype_end,
                                unsigned _vtype_begin, unsigned _vtype_end,
                                unsigned _current_etype,
                                GAR_NAMESPACE::EdgeIter _iter)
      : v(_v),
        dir(_dir),
        etype_begin(_etype_begin),
        etype_end(_etype_end),
        vtype_begin(_vtype_begin),
        vtype_end(_vtype_end),
        current_etype(_current_etype),
        iter(_iter) {}
};
#endif

#ifdef GRIN_WITH_VERTEX_PROPERTY
typedef unsigned GRIN_VERTEX_TYPE_T;
typedef std::vector<unsigned> GRIN_VERTEX_TYPE_LIST_T;
struct GRIN_VERTEX_PROPERTY_T {
  unsigned type_id;
  std::string name;
  GRIN_DATATYPE type;
  GRIN_VERTEX_PROPERTY_T(unsigned _type_id, std::string _name,
                         GRIN_DATATYPE _type)
      : type_id(_type_id), name(_name), type(_type) {}
};
typedef std::vector<GRIN_VERTEX_PROPERTY_T> GRIN_VERTEX_PROPERTY_LIST_T;
struct GRIN_VERTEX_PROPERTY_TABLE_T {
  unsigned vtype;
  explicit GRIN_VERTEX_PROPERTY_TABLE_T(unsigned _vtype) : vtype(_vtype) {}
};
#endif

#ifdef GRIN_WITH_EDGE_PROPERTY
typedef unsigned GRIN_EDGE_TYPE_T;
typedef std::vector<unsigned> GRIN_EDGE_TYPE_LIST_T;
struct GRIN_EDGE_PROPERTY_T {
  unsigned type_id;
  std::string name;
  GRIN_DATATYPE type;
  GRIN_EDGE_PROPERTY_T(unsigned _type_id, std::string _name,
                       GRIN_DATATYPE _type)
      : type_id(_type_id), name(_name), type(_type) {}
  bool operator==(const GRIN_EDGE_PROPERTY_T& other) const {
    return type_id == other.type_id && name == other.name && type == other.type;
  }
  bool operator<(const GRIN_EDGE_PROPERTY_T& other) const {
    return type_id < other.type_id ||
           (type_id == other.type_id && name < other.name) ||
           (type_id == other.type_id && name == other.name && type < other.type);
  }
};
typedef std::vector<GRIN_EDGE_PROPERTY_T> GRIN_EDGE_PROPERTY_LIST_T;
struct GRIN_EDGE_PROPERTY_TABLE_T {
  unsigned type_begin;
  unsigned type_end;
  GRIN_EDGE_PROPERTY_TABLE_T(unsigned _type_begin, unsigned _type_end)
      : type_begin(_type_begin), type_end(_type_end) {}
};
#endif

#if defined(GRIN_WITH_VERTEX_PROPERTY) || defined(GRIN_WITH_EDGE_PROPERTY)
typedef std::vector<std::any> GRIN_ROW_T;
#endif

#endif  // CPP_GRIN_SRC_PREDEFINE_H_
