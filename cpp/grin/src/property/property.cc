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

#include "grin/include/property/property.h"
#include "grin/src/predefine.h"

#ifdef GRIN_WITH_VERTEX_PROPERTY_NAME
const char* grin_get_vertex_property_name(GRIN_GRAPH g,
                                          GRIN_VERTEX_PROPERTY vp) {
  auto _vp = static_cast<GRIN_VERTEX_PROPERTY_T*>(vp);
  auto s = _vp->name;
  int len = s.length() + 1;
  char* out = new char[len];
  snprintf(out, len, "%s", s.c_str());
  return out;
}

GRIN_VERTEX_PROPERTY grin_get_vertex_property_by_name(GRIN_GRAPH g,
                                                      GRIN_VERTEX_TYPE vtype,
                                                      const char* name) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _vtype = static_cast<GRIN_VERTEX_TYPE_T*>(vtype);
  auto s = std::string(name);
  auto vertex_info =
      _g->graph_info.GetVertexInfo(_g->vertex_types[*_vtype]).value();
  auto vp = new GRIN_VERTEX_PROPERTY_T(
      *_vtype, s, GARToDataType(vertex_info.GetPropertyType(s).value()));
  return vp;
}

GRIN_VERTEX_PROPERTY_LIST grin_get_vertex_properties_by_name(GRIN_GRAPH g,
                                                             const char* name) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto s = std::string(name);
  auto vpl = new GRIN_VERTEX_PROPERTY_LIST_T();
  for (auto vtype = 0; vtype < _g->vertex_type_num; ++vtype) {
    auto vertex_info =
        _g->graph_info.GetVertexInfo(_g->vertex_types[vtype]).value();
    if (vertex_info.ContainProperty(s))
      vpl->push_back(GRIN_VERTEX_PROPERTY_T(
          vtype, s, GARToDataType(vertex_info.GetPropertyType(s).value())));
  }
  return vpl;
}
#endif

#ifdef GRIN_WITH_EDGE_PROPERTY_NAME
const char* grin_get_edge_property_name(GRIN_GRAPH, GRIN_EDGE_PROPERTY);

GRIN_EDGE_PROPERTY grin_get_edge_property_by_name(GRIN_GRAPH, GRIN_EDGE_TYPE,
                                                  const char* name);

GRIN_EDGE_PROPERTY_LIST grin_get_edge_properties_by_name(GRIN_GRAPH,
                                                         const char* name);
#endif

#ifdef GRIN_WITH_VERTEX_PROPERTY
bool grin_equal_vertex_property(GRIN_GRAPH g, GRIN_VERTEX_PROPERTY vp1,
                                GRIN_VERTEX_PROPERTY vp2) {
  auto _vp1 = static_cast<GRIN_VERTEX_PROPERTY_T*>(vp1);
  auto _vp2 = static_cast<GRIN_VERTEX_PROPERTY_T*>(vp2);
  return (_vp1->type_id == _vp2->type_id && _vp1->name == _vp2->name &&
          _vp1->type == _vp2->type);
}

void grin_destroy_vertex_property(GRIN_GRAPH g, GRIN_VERTEX_PROPERTY vp) {
  auto _vp = static_cast<GRIN_VERTEX_PROPERTY_T*>(vp);
  delete _vp;
}

GRIN_DATATYPE grin_get_vertex_property_data_type(GRIN_GRAPH g,
                                                 GRIN_VERTEX_PROPERTY vp) {
  auto _vp = static_cast<GRIN_VERTEX_PROPERTY_T*>(vp);
  return _vp->type;
}

GRIN_VERTEX_TYPE grin_get_vertex_property_vertex_type(GRIN_GRAPH g,
                                                      GRIN_VERTEX_PROPERTY vp) {
  auto _vp = static_cast<GRIN_VERTEX_PROPERTY_T*>(vp);
  auto vt = new GRIN_VERTEX_TYPE_T(_vp->type_id);
  return vt;
}
#endif

#ifdef GRIN_WITH_EDGE_PROPERTY
bool grin_equal_edge_property(GRIN_GRAPH g, GRIN_EDGE_PROPERTY ep1,
                              GRIN_EDGE_PROPERTY ep2) {
  auto _ep1 = static_cast<GRIN_EDGE_PROPERTY_T*>(ep1);
  auto _ep2 = static_cast<GRIN_EDGE_PROPERTY_T*>(ep2);
  return (_ep1->type_id == _ep2->type_id && _ep1->name == _ep2->name &&
          _ep1->type == _ep2->type);
}

void grin_destroy_edge_property(GRIN_GRAPH g, GRIN_EDGE_PROPERTY ep) {
  auto _ep = static_cast<GRIN_EDGE_PROPERTY_T*>(ep);
  delete _ep;
}

GRIN_DATATYPE grin_get_edge_property_data_type(GRIN_GRAPH g,
                                               GRIN_EDGE_PROPERTY ep) {
  auto _ep = static_cast<GRIN_EDGE_PROPERTY_T*>(ep);
  return _ep->type;
}

GRIN_EDGE_TYPE grin_get_edge_property_edge_type(GRIN_GRAPH g,
                                                GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _ep = static_cast<GRIN_EDGE_PROPERTY_T*>(ep);
  auto et = new GRIN_EDGE_TYPE_T(_g->unique_edge_type_ids[_ep->type_id]);
  return et;
}
#endif
