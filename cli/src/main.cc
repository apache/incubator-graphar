#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <graphar/graph_info.h>

#define STRINGIFY(x) #x
#define MACRO_STRINGIFY(x) STRINGIFY(x)

std::string ShowGraph(const std::string& path) {
  auto graph_info = graphar::GraphInfo::Load(path).value();
  return graph_info->Dump().value();
}

std::string ShowVertex(const std::string& path,
                       const std::string& vertex_type) {
  auto graph_info = graphar::GraphInfo::Load(path).value();
  auto vertex_info = graph_info->GetVertexInfo(vertex_type);
  return vertex_info->Dump().value();
}

std::string ShowEdge(const std::string& path, const std::string& src_type,
                     const std::string& edge_type,
                     const std::string& dst_type) {
  auto graph_info = graphar::GraphInfo::Load(path).value();
  auto edge_info = graph_info->GetEdgeInfo(src_type, edge_type, dst_type);
  return edge_info->Dump().value();
}

bool CheckGraph(const std::string& path) {
  auto graph_info = graphar::GraphInfo::Load(path).value();
  return graph_info->IsValidated();
}

bool CheckVertex(const std::string& path, const std::string& vertex_type) {
  auto graph_info = graphar::GraphInfo::Load(path).value();
  auto vertex_info = graph_info->GetVertexInfo(vertex_type);
  return vertex_info->IsValidated();
}

bool CheckEdge(const std::string& path, const std::string& src_type,
               const std::string& edge_type, const std::string& dst_type) {
  auto graph_info = graphar::GraphInfo::Load(path).value();
  auto edge_info = graph_info->GetEdgeInfo(src_type, edge_type, dst_type);
  return edge_info->IsValidated();
}

std::vector<std::string> GetVertexTypes(const std::string& path) {
  auto graph_info = graphar::GraphInfo::Load(path).value();
  auto vertex_infos = graph_info->GetVertexInfos();
  std::vector<std::string> vertex_types;
  for (const auto& vertex_info : vertex_infos) {
    vertex_types.push_back(vertex_info->GetType());
  }
  return vertex_types;
}

std::vector<std::vector<std::string>> GetEdgeTypes(const std::string& path) {
  auto graph_info = graphar::GraphInfo::Load(path).value();
  auto edge_infos = graph_info->GetEdgeInfos();
  std::vector<std::vector<std::string>> edge_types;
  for (const auto& edge_info : edge_infos) {
    std::vector<std::string> edge_type;
    edge_type.push_back(edge_info->GetEdgeType());
    edge_type.push_back(edge_info->GetSrcType());
    edge_type.push_back(edge_info->GetDstType());
    edge_types.push_back(edge_type);
  }
  return edge_types;
}

namespace py = pybind11;
PYBIND11_MODULE(_core, m) {
  m.doc() = "GraphAr Python bindings";
  m.def("show_graph", &ShowGraph, "Show the graph info");
  m.def("show_vertex", &ShowVertex, "Show the vertex info");
  m.def("show_edge", &ShowEdge, "Show the edge info");
  m.def("check_graph", &CheckGraph, "Check the graph info");
  m.def("check_vertex", &CheckVertex, "Check the vertex info");
  m.def("check_edge", &CheckEdge, "Check the edge info");
  m.def("get_vertex_types", &GetVertexTypes, "Get the vertex types");
  m.def("get_edge_types", &GetEdgeTypes, "Get the edge types");

#ifdef VERSION_INFO
  m.attr("__version__") = MACRO_STRINGIFY(VERSION_INFO);
#else
  m.attr("__version__") = "dev";
#endif
}
