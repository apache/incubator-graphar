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

#pragma once

#include <filesystem>

#include "arrow/api.h"
#include "graphar/api/arrow_writer.h"
#include "graphar/api/high_level_writer.h"
#include "graphar/convert_to_arrow_type.h"
#include "graphar/graph_info.h"
#include "graphar/high-level/graph_reader.h"
#include "pybind11/pybind11.h"
#include "pybind11/stl.h"

#include "utils/import_util.h"

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
  std::unordered_map<std::string, std::string> columns;
};

struct Vertex {
  std::string type;
  std::vector<std::string> labels;
  int chunk_size;
  std::string validate_level;
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
  int chunk_size;
  std::string validate_level;
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
    vertex.validate_level = vertex_dict["validate_level"].cast<std::string>();
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
        pg.properties.emplace_back(prop);
      }
      vertex.property_groups.emplace_back(pg);
    }

    auto source_list = vertex_dict["sources"].cast<std::vector<py::dict>>();
    for (const auto& source_dict : source_list) {
      Source src;
      src.file_type = source_dict["file_type"].cast<std::string>();
      src.path = source_dict["path"].cast<std::string>();
      src.delimiter = source_dict["delimiter"].cast<char>();
      src.columns = source_dict["columns"]
                        .cast<std::unordered_map<std::string, std::string>>();

      vertex.sources.emplace_back(src);
    }

    import_config.import_schema.vertices.emplace_back(vertex);
  }

  auto edges_list = schema_dict["edges"].cast<std::vector<py::dict>>();
  for (const auto& edge_dict : edges_list) {
    Edge edge;
    edge.edge_type = edge_dict["edge_type"].cast<std::string>();
    edge.src_type = edge_dict["src_type"].cast<std::string>();
    edge.src_prop = edge_dict["src_prop"].cast<std::string>();
    edge.dst_type = edge_dict["dst_type"].cast<std::string>();
    edge.dst_prop = edge_dict["dst_prop"].cast<std::string>();
    edge.chunk_size = edge_dict["chunk_size"].cast<int>();
    edge.validate_level = edge_dict["validate_level"].cast<std::string>();
    edge.prefix = edge_dict["prefix"].cast<std::string>();

    auto adj_list_dicts = edge_dict["adj_lists"].cast<std::vector<py::dict>>();
    for (const auto& adj_list_dict : adj_list_dicts) {
      AdjList adj_list;
      adj_list.ordered = adj_list_dict["ordered"].cast<bool>();
      adj_list.aligned_by = adj_list_dict["aligned_by"].cast<std::string>();
      adj_list.file_type = adj_list_dict["file_type"].cast<std::string>();
      edge.adj_lists.emplace_back(adj_list);
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
        edge_pg.properties.emplace_back(edge_prop);
      }

      edge.property_groups.emplace_back(edge_pg);
    }

    auto edge_source_list = edge_dict["sources"].cast<std::vector<py::dict>>();
    for (const auto& edge_source_dict : edge_source_list) {
      Source edge_src;
      edge_src.file_type = edge_source_dict["file_type"].cast<std::string>();
      edge_src.path = edge_source_dict["path"].cast<std::string>();
      edge_src.delimiter = edge_source_dict["delimiter"].cast<char>();
      edge_src.columns =
          edge_source_dict["columns"]
              .cast<std::unordered_map<std::string, std::string>>();

      edge.sources.emplace_back(edge_src);
    }

    import_config.import_schema.edges.emplace_back(edge);
  }

  return import_config;
}

std::string DoImport(const py::dict& config_dict) {
  auto import_config = ConvertPyDictToConfig(config_dict);

  auto version =
      graphar::InfoVersion::Parse(import_config.graphar_config.version).value();
  fs::path save_path = import_config.graphar_config.path;

  std::unordered_map<std::string, graphar::IdType> vertex_chunk_sizes;
  std::unordered_map<std::string, int64_t> vertex_counts;

  std::map<std::pair<std::string, std::string>,
           std::unordered_map<std::shared_ptr<arrow::Scalar>, graphar::IdType,
                              arrow::Scalar::Hash, arrow::Scalar::PtrsEqual>>
      vertex_prop_index_map;

  std::unordered_map<std::string, std::vector<std::string>>
      vertex_props_in_edges;
  std::map<std::pair<std::string, std::string>, graphar::Property>
      vertex_prop_property_map;
  for (const auto& edge : import_config.import_schema.edges) {
    vertex_props_in_edges[edge.src_type].emplace_back(edge.src_prop);
    vertex_props_in_edges[edge.dst_type].emplace_back(edge.dst_prop);
  }
  for (const auto& vertex : import_config.import_schema.vertices) {
    vertex_chunk_sizes[vertex.type] = vertex.chunk_size;

    auto pgs = std::vector<std::shared_ptr<graphar::PropertyGroup>>();
    std::string primary_key;
    for (const auto& pg : vertex.property_groups) {
      std::vector<graphar::Property> props;
      for (const auto& prop : pg.properties) {
        if (prop.is_primary) {
          if (!primary_key.empty()) {
            throw std::runtime_error("Multiple primary keys found in vertex " +
                                     vertex.type);
          }
          primary_key = prop.name;
        }
        graphar::Property property(
            prop.name, graphar::DataType::TypeNameToDataType(prop.data_type),
            prop.is_primary, prop.nullable);
        props.emplace_back(property);
        vertex_prop_property_map[std::make_pair(vertex.type, prop.name)] =
            property;
      }
      // TODO: add prefix parameter in config
      auto property_group = graphar::CreatePropertyGroup(
          props, graphar::StringToFileType(pg.file_type));
      pgs.emplace_back(property_group);
    }

    auto vertex_info =
        graphar::CreateVertexInfo(vertex.type, vertex.chunk_size, pgs,
                                  vertex.labels, vertex.prefix, version);
    auto file_name = vertex.type + ".vertex.yml";
    vertex_info->Save(save_path / file_name);
    auto save_path_str = save_path.string();
    save_path_str += "/";
    auto vertex_prop_writer = graphar::VertexPropertyWriter::Make(
                                  vertex_info, save_path_str,
                                  StringToValidateLevel(vertex.validate_level))
                                  .value();

    std::vector<std::shared_ptr<arrow::Table>> vertex_tables;
    for (const auto& source : vertex.sources) {
      std::vector<std::string> column_names;
      for (const auto& [key, value] : source.columns) {
        column_names.emplace_back(key);
      }
      auto table = GetDataFromFile(source.path, column_names, source.delimiter,
                                   source.file_type);

      std::unordered_map<std::string, Property> column_prop_map;
      std::unordered_map<std::string, std::string> reversed_columns_config;
      for (const auto& [key, value] : source.columns) {
        reversed_columns_config[value] = key;
      }
      for (const auto& pg : vertex.property_groups) {
        for (const auto& prop : pg.properties) {
          column_prop_map[reversed_columns_config[prop.name]] = prop;
        }
      }
      std::unordered_map<
          std::string, std::pair<std::string, std::shared_ptr<arrow::DataType>>>
          columns_to_change;
      for (const auto& [column, prop] : column_prop_map) {
        auto arrow_data_type = graphar::DataType::DataTypeToArrowDataType(
            graphar::DataType::TypeNameToDataType(prop.data_type));
        auto arrow_column = table->GetColumnByName(column);
        // TODO: whether need to check duplicate values for primary key?
        if (!prop.nullable) {
          for (const auto& chunk : arrow_column->chunks()) {
            if (chunk->null_count() > 0) {
              throw std::runtime_error("Non-nullable column '" + column +
                                       "' has null values");
            }
          }
        }
        // TODO: check this
        if (column != prop.name ||
            arrow_column->type()->id() != arrow_data_type->id()) {
          columns_to_change[column] =
              std::make_pair(prop.name, arrow_data_type);
        }
      }
      table = ChangeNameAndDataType(table, columns_to_change);
      vertex_tables.emplace_back(table);
    }
    std::shared_ptr<arrow::Table> merged_vertex_table =
        MergeTables(vertex_tables);
    // TODO: check all fields in props
    // TODO: add start_index in config
    graphar::IdType start_chunk_index = 0;

    auto vertex_table_with_index =
        vertex_prop_writer
            ->AddIndexColumn(merged_vertex_table, start_chunk_index,
                             vertex_info->GetChunkSize())
            .value();
    for (const auto& property_group : pgs) {
      vertex_prop_writer->WriteTable(vertex_table_with_index, property_group,
                                     start_chunk_index);
    }
    if (vertex_props_in_edges.find(vertex.type) !=
        vertex_props_in_edges.end()) {
      for (const auto& vertex_prop : vertex_props_in_edges[vertex.type]) {
        vertex_prop_index_map[std::make_pair(vertex.type, vertex_prop)] =
            TableToUnorderedMap(vertex_table_with_index, vertex_prop,
                                graphar::GeneralParams::kVertexIndexCol);
      }
    }
    auto vertex_count = merged_vertex_table->num_rows();
    vertex_counts[vertex.type] = vertex_count;
    vertex_prop_writer->WriteVerticesNum(vertex_count);
  }

  for (const auto& edge : import_config.import_schema.edges) {
    auto pgs = std::vector<std::shared_ptr<graphar::PropertyGroup>>();

    for (const auto& pg : edge.property_groups) {
      std::vector<graphar::Property> props;
      for (const auto& prop : pg.properties) {
        props.emplace_back(graphar::Property(
            prop.name, graphar::DataType::TypeNameToDataType(prop.data_type),
            prop.is_primary, prop.nullable));
      }
      // TODO: add prefix parameter in config
      auto property_group = graphar::CreatePropertyGroup(
          props, graphar::StringToFileType(pg.file_type));
      pgs.emplace_back(property_group);
    }
    graphar::AdjacentListVector adj_lists;
    for (const auto& adj_list : edge.adj_lists) {
      // TODO: add prefix parameter in config
      adj_lists.emplace_back(graphar::CreateAdjacentList(
          graphar::OrderedAlignedToAdjListType(adj_list.ordered,
                                               adj_list.aligned_by),
          graphar::StringToFileType(adj_list.file_type)));
    }

    // TODO: add directed parameter in config

    bool directed = true;
    // TODO: whether prefix has default value?

    auto edge_info = graphar::CreateEdgeInfo(
        edge.src_type, edge.edge_type, edge.dst_type, edge.chunk_size,
        vertex_chunk_sizes[edge.src_type], vertex_chunk_sizes[edge.dst_type],
        directed, adj_lists, pgs, edge.prefix, version);
    auto file_name =
        ConcatEdgeTriple(edge.src_type, edge.edge_type, edge.dst_type) +
        ".edge.yml";
    edge_info->Save(save_path / file_name);
    auto save_path_str = save_path.string();
    save_path_str += "/";
    for (const auto& adj_list : adj_lists) {
      int64_t vertex_count;
      if (adj_list->GetType() == graphar::AdjListType::ordered_by_source ||
          adj_list->GetType() == graphar::AdjListType::unordered_by_source) {
        vertex_count = vertex_counts[edge.src_type];
      } else {
        vertex_count = vertex_counts[edge.dst_type];
      }
      std::vector<std::shared_ptr<arrow::Table>> edge_tables;

      for (const auto& source : edge.sources) {
        std::vector<std::string> column_names;
        for (const auto& [key, value] : source.columns) {
          column_names.emplace_back(key);
        }
        auto table = GetDataFromFile(source.path, column_names,
                                     source.delimiter, source.file_type);
        std::unordered_map<std::string, graphar::Property> column_prop_map;
        std::unordered_map<std::string, std::string> reversed_columns;
        for (const auto& [key, value] : source.columns) {
          reversed_columns[value] = key;
        }

        for (const auto& pg : edge.property_groups) {
          for (const auto& prop : pg.properties) {
            column_prop_map[reversed_columns[prop.name]] = graphar::Property(
                prop.name,
                graphar::DataType::TypeNameToDataType(prop.data_type),
                prop.is_primary, prop.nullable);
          }
        }
        column_prop_map[reversed_columns.at(edge.src_prop)] =
            vertex_prop_property_map.at(
                std::make_pair(edge.src_type, edge.src_prop));
        column_prop_map[reversed_columns.at(edge.dst_prop)] =
            vertex_prop_property_map.at(
                std::make_pair(edge.dst_type, edge.dst_prop));
        std::unordered_map<
            std::string,
            std::pair<std::string, std::shared_ptr<arrow::DataType>>>
            columns_to_change;
        for (const auto& [column, prop] : column_prop_map) {
          auto arrow_data_type =
              graphar::DataType::DataTypeToArrowDataType(prop.type);
          auto arrow_column = table->GetColumnByName(column);
          // TODO: is needed?
          if (!prop.is_nullable) {
            for (const auto& chunk : arrow_column->chunks()) {
              if (chunk->null_count() > 0) {
                throw std::runtime_error("Non-nullable column '" + column +
                                         "' has null values");
              }
            }
          }
          if (column != prop.name ||
              arrow_column->type()->id() != arrow_data_type->id()) {
            columns_to_change[column] =
                std::make_pair(prop.name, arrow_data_type);
          }
        }
        table = ChangeNameAndDataType(table, columns_to_change);
        edge_tables.emplace_back(table);
      }
      std::unordered_map<
          std::string, std::pair<std::string, std::shared_ptr<arrow::DataType>>>
          vertex_columns_to_change;

      std::shared_ptr<arrow::Table> merged_edge_table =
          MergeTables(edge_tables);
      // TODO: check all fields in props

      auto combined_edge_table =
          merged_edge_table->CombineChunks().ValueOrDie();

      auto edge_builder =
          graphar::builder::EdgesBuilder::Make(
              edge_info, save_path_str, adj_list->GetType(), vertex_count,
              StringToValidateLevel(edge.validate_level))
              .value();

      std::vector<std::string> edge_column_names;
      for (const auto& field : combined_edge_table->schema()->fields()) {
        edge_column_names.push_back(field->name());
      }
      const int64_t num_rows = combined_edge_table->num_rows();
      for (int64_t i = 0; i < num_rows; ++i) {
        auto edge_src_column =
            combined_edge_table->GetColumnByName(edge.src_prop);
        auto edge_dst_column =
            combined_edge_table->GetColumnByName(edge.dst_prop);

        graphar::builder::Edge e(
            vertex_prop_index_map
                .at(std::make_pair(edge.src_type, edge.src_prop))
                .at(edge_src_column->GetScalar(i).ValueOrDie()),
            vertex_prop_index_map
                .at(std::make_pair(edge.dst_type, edge.dst_prop))
                .at(edge_dst_column->GetScalar(i).ValueOrDie()));
        for (const auto& column_name : edge_column_names) {
          if (column_name != edge.src_prop && column_name != edge.dst_prop) {
            auto column = combined_edge_table->GetColumnByName(column_name);
            auto column_type = column->type();
            std::any value;
            TryToCastToAny(
                graphar::DataType::ArrowDataTypeToDataType(column_type),
                column->chunk(0), value);
            e.AddProperty(column_name, value);
          }
        }
        edge_builder->AddEdge(e);
      }
      edge_builder->Dump();
    }
  }
  return "Imported successfully!";
}