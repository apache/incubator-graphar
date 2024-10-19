#include <filesystem>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <graphar/api/high_level_writer.h>

namespace py = pybind11;
namespace fs = std::filesystem;

struct GraphArConfig {
  std::string path;
  std::string name;
  std::string version;
};

struct Property {
  std::string name;
  std::string data_type;
  bool is_primary;
  bool nullable;
};

struct PropertyGroup {
  std::string file_type;
  std::vector<Property> properties;
};

struct Source {
  std::string file_type;
  std::string path;
  char delimiter;
  std::map<std::string, std::string> columns;
};

struct Vertex {
  std::string type;
  std::vector<std::string> labels;
  int chunk_size;
  std::string prefix;
  std::vector<PropertyGroup> property_groups;
  std::vector<Source> sources;
};

struct AdjList {
  bool ordered;
  std::string aligned_by;
  std::string file_type;
};

struct Edge {
  std::string edge_type;
  std::string src_type;
  std::string src_prop;
  std::string dst_type;
  std::string dst_prop;
  std::vector<std::string> labels;
  int chunk_size;
  std::string prefix;
  std::vector<AdjList> adj_lists;
  std::vector<PropertyGroup> property_groups;
  std::vector<Source> sources;
};

struct ImportSchema {
  std::vector<Vertex> vertices;
  std::vector<Edge> edges;
};

struct ImportConfig {
  GraphArConfig graphar_config;
  ImportSchema import_schema;
};

ImportConfig ConvertPyDictToConfig(const py::dict& config_dict) {
  ImportConfig import_config;

  auto graphar_dict = config_dict["graphar"].cast<py::dict>();
  import_config.graphar_config.path = graphar_dict["path"].cast<std::string>();
  import_config.graphar_config.name = graphar_dict["name"].cast<std::string>();
  import_config.graphar_config.version =
      graphar_dict["version"].cast<std::string>();

  auto schema_dict = config_dict["import_schema"].cast<py::dict>();

  auto vertices_list = schema_dict["vertices"].cast<std::vector<py::dict>>();
  for (const auto& vertex_dict : vertices_list) {
    Vertex vertex;
    vertex.type = vertex_dict["type"].cast<std::string>();
    vertex.chunk_size = vertex_dict["chunk_size"].cast<int>();
    vertex.prefix = vertex_dict["prefix"].cast<std::string>();
    vertex.labels = vertex_dict["labels"].cast<std::vector<std::string>>();

    auto pg_list = vertex_dict["property_groups"].cast<std::vector<py::dict>>();
    for (const auto& pg_dict : pg_list) {
      PropertyGroup pg;
      pg.file_type = pg_dict["file_type"].cast<std::string>();

      auto prop_list = pg_dict["properties"].cast<std::vector<py::dict>>();
      for (const auto& prop_dict : prop_list) {
        Property prop;
        prop.name = prop_dict["name"].cast<std::string>();
        prop.data_type = prop_dict["data_type"].cast<std::string>();
        prop.is_primary = prop_dict["is_primary"].cast<bool>();
        prop.nullable = prop_dict["nullable"].cast<bool>();
        pg.properties.push_back(prop);
      }
      vertex.property_groups.push_back(pg);
    }

    auto source_list = vertex_dict["sources"].cast<std::vector<py::dict>>();
    for (const auto& source_dict : source_list) {
      Source src;
      src.file_type = source_dict["file_type"].cast<std::string>();
      src.path = source_dict["path"].cast<std::string>();
      src.delimiter = source_dict["delimiter"].cast<char>();
      src.columns =
          source_dict["columns"].cast<std::map<std::string, std::string>>();

      vertex.sources.push_back(src);
    }

    import_config.import_schema.vertices.push_back(vertex);
  }

  auto edges_list = schema_dict["edges"].cast<std::vector<py::dict>>();
  for (const auto& edge_dict : edges_list) {
    Edge edge;
    edge.edge_type = edge_dict["edge_type"].cast<std::string>();
    edge.src_type = edge_dict["src_type"].cast<std::string>();
    edge.src_prop = edge_dict["src_prop"].cast<std::string>();
    edge.dst_type = edge_dict["dst_type"].cast<std::string>();
    edge.dst_prop = edge_dict["dst_prop"].cast<std::string>();
    edge.labels = edge_dict["labels"].cast<std::vector<std::string>>();
    edge.chunk_size = edge_dict["chunk_size"].cast<int>();
    edge.prefix = edge_dict["prefix"].cast<std::string>();

    auto adj_list_dicts = edge_dict["adj_lists"].cast<std::vector<py::dict>>();
    for (const auto& adj_list_dict : adj_list_dicts) {
      AdjList adj_list;
      adj_list.ordered = adj_list_dict["ordered"].cast<bool>();
      adj_list.aligned_by = adj_list_dict["aligned_by"].cast<std::string>();
      adj_list.file_type = adj_list_dict["file_type"].cast<std::string>();
      edge.adj_lists.push_back(adj_list);
    }

    auto edge_pg_list =
        edge_dict["property_groups"].cast<std::vector<py::dict>>();
    for (const auto& edge_pg_dict : edge_pg_list) {
      PropertyGroup edge_pg;
      edge_pg.file_type = edge_pg_dict["file_type"].cast<std::string>();
      auto edge_prop_list =
          edge_pg_dict["properties"].cast<std::vector<py::dict>>();

      for (const auto& prop_dict : edge_prop_list) {
        Property edge_prop;
        edge_prop.name = prop_dict["name"].cast<std::string>();
        edge_prop.data_type = prop_dict["data_type"].cast<std::string>();
        edge_prop.is_primary = prop_dict["is_primary"].cast<bool>();
        edge_prop.nullable = prop_dict["nullable"].cast<bool>();
        edge_pg.properties.push_back(edge_prop);
      }

      edge.property_groups.push_back(edge_pg);
    }

    auto edge_source_list = edge_dict["sources"].cast<std::vector<py::dict>>();
    for (const auto& edge_source_dict : edge_source_list) {
      Source edge_src;
      edge_src.file_type = edge_source_dict["file_type"].cast<std::string>();
      edge_src.path = edge_source_dict["path"].cast<std::string>();
      edge_src.delimiter = edge_source_dict["delimiter"].cast<char>();
      edge_src.columns = edge_source_dict["columns"]
                             .cast<std::map<std::string, std::string>>();

      edge.sources.push_back(edge_src);
    }

    import_config.import_schema.edges.push_back(edge);
  }

  return import_config;
}

std::string DoImport(const py::dict& config_dict) {
  auto import_config = ConvertPyDictToConfig(config_dict);

  auto version =
      graphar::InfoVersion::Parse(import_config.graphar_config.version).value();
  fs::path save_path = import_config.graphar_config.path;

  for (const auto& vertex : import_config.import_schema.vertices) {
    auto pgs = std::vector<std::shared_ptr<graphar::PropertyGroup>>();

    for (const auto& pg : vertex.property_groups) {
      std::vector<graphar::Property> props;
      for (const auto& prop : pg.properties) {
        props.push_back(graphar::Property(
            prop.name, graphar::DataType::TypeNameToDataType(prop.data_type),
            prop.is_primary, prop.nullable));
      }
      // TODO: add prefix parameter in config
      auto property_group = graphar::CreatePropertyGroup(
          props, graphar::StringToFileType(pg.file_type));
      pgs.push_back(property_group);
    }

    auto vertex_info = graphar::CreateVertexInfo(vertex.type, vertex.chunk_size,
                                                 pgs, vertex.prefix, version);
    auto file_name = vertex.type + ".vertex.yml";
    vertex_info->Save(save_path / file_name);
    // TODO: add start_index in config
    graphar::IdType start_index = 0;
  }

  return "Importing...";
}