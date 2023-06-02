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

#include "grin/src/predefine.h"
extern "C" {
#include "index/original_id.h"
}

GRIN_DATATYPE grin_get_vertex_original_id_datatype(GRIN_GRAPH g) {
  return GRIN_DATATYPE::Int64;
}

#ifdef GRIN_ENABLE_VERTEX_ORIGINAL_ID_OF_INT64
long long int grin_get_vertex_original_id_of_int64(GRIN_GRAPH g,  // NOLINT
                                                   GRIN_VERTEX v) {
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  return __grin_generate_int64_from_id_and_type(_v->id, _v->type_id);
}

GRIN_VERTEX grin_get_vertex_by_original_id_of_int64(
    GRIN_GRAPH g,
    long long int oid) {  // NOLINT
  auto pair = __grin_generate_id_and_type_from_int64(oid);
  auto v = new GRIN_VERTEX_T(pair.first, pair.second);
  return v;
}
#endif

#ifdef GRIN_ENABLE_VERTEX_ORIGINAL_ID_OF_STRING
const char* grin_get_vertex_original_id_of_string(GRIN_GRAPH, GRIN_VERTEX);

GRIN_VERTEX grin_get_vertex_by_original_id_of_string(GRIN_GRAPH, const char*);
#endif
