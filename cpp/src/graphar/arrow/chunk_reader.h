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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "graphar/fwd.h"
#include "graphar/reader_util.h"
#include "graphar/status.h"

// forward declaration
namespace arrow {
class Array;
class Schema;
class Table;
}  // namespace arrow

namespace graphar {

/**
 * @brief The arrow chunk reader for vertex property group.
 */
class VertexPropertyArrowChunkReader {
 public:
  /**
   * @brief Initialize the VertexPropertyArrowChunkReader.
   *
   * @param vertex_info The vertex info that describes the vertex type.
   * @param property_group The property group that describes the property group.
   * @param prefix The absolute prefix.
   */
  VertexPropertyArrowChunkReader(
      const std::shared_ptr<VertexInfo>& vertex_info,
      const std::shared_ptr<PropertyGroup>& property_group,
      const std::string& prefix, const util::FilterOptions& options = {});
  /**
   * @brief Initialize the VertexPropertyArrowChunkReader.
   *
   * @param vertex_info The vertex info that describes the vertex type.
   * @param property_group The property group that describes the property group.
   * @param property_names Only these properties will be read.
   * @param prefix The absolute prefix.
   */
  VertexPropertyArrowChunkReader(
      const std::shared_ptr<VertexInfo>& vertex_info,
      const std::shared_ptr<PropertyGroup>& property_group,
      const std::vector<std::string>& property_names, const std::string& prefix,
      const util::FilterOptions& options = {});

  VertexPropertyArrowChunkReader() : vertex_info_(nullptr), prefix_("") {}

  /**
   * @brief Initialize the VertexPropertyArrowChunkReader.
   *
   * @param vertex_info The vertex info that describes the vertex type.
   * @param labels The labels of the vertex type.
   * @param prefix The absolute prefix.
   */
  VertexPropertyArrowChunkReader(const std::shared_ptr<VertexInfo>& vertex_info,
                                 const std::vector<std::string>& labels,
                                 const std::string& prefix,
                                 const util::FilterOptions& options = {});
  /**
   * @brief Sets chunk position indicator for reader by internal vertex id.
   *    If internal vertex id is not found, will return Status::IndexError
   * error. After seeking to an invalid vertex id, the next call to GetChunk
   * function may undefined, e.g. return an non exist path.
   *
   * @param id the vertex id.
   */
  Status seek(IdType id);

  /**
   * @brief Return the current arrow chunk table of chunk position indicator.
   */
  Result<std::shared_ptr<arrow::Table>> GetChunk(
      GetChunkVersion version = GetChunkVersion::AUTO);
  /**
   * @brief Return the current arrow label chunk table of chunk position
   * indicator.
   */
  Result<std::shared_ptr<arrow::Table>> GetLabelChunk();
  /**
   * @brief Sets chunk position indicator to next chunk.
   *
   *  if current chunk is the last chunk, will return Status::IndexError error.
   */
  Status next_chunk();

  /**
   * @brief Get the chunk number of current vertex property group.
   */
  IdType GetChunkNum() const noexcept { return chunk_num_; }

  /**
   * @brief Apply the row filter to the table. No parameter call Filter() will
   * clear the filter.
   *
   * @param filter Predicate expression to filter rows.
   */
  void Filter(util::Filter filter = nullptr);

  /**
   * @brief Apply the projection to the table to be read. No parameter call
   * Select() will clear the projection.
   *
   * @param column_names The name of columns to be selected.
   */
  void Select(util::ColumnNames column_names = std::nullopt);

  /**
   * @brief Create a VertexPropertyArrowChunkReader instance from vertex info.
   *
   * @param vertex_info The vertex info.
   * @param property_group The property group of the vertex property.
   * @param prefix The absolute prefix of the graph.
   * @param options The filter options, default is empty.
   */
  static Result<std::shared_ptr<VertexPropertyArrowChunkReader>> Make(
      const std::shared_ptr<VertexInfo>& vertex_info,
      const std::shared_ptr<PropertyGroup>& property_group,
      const std::string& prefix, const util::FilterOptions& options = {});

  /**
   * @brief Create a VertexPropertyArrowChunkReader instance from vertex info.
   *
   * @param vertex_info The vertex info.
   * @param property_group The property group of the vertex property.
   * @param property_names is not empty, only these properties will be read.
   * @param prefix The absolute prefix of the graph.
   * @param options The filter options, default is empty.
   */
  static Result<std::shared_ptr<VertexPropertyArrowChunkReader>> Make(
      const std::shared_ptr<VertexInfo>& vertex_info,
      const std::shared_ptr<PropertyGroup>& property_group,
      const std::vector<std::string>& property_names, const std::string& prefix,
      const util::FilterOptions& options = {});

  /**
   * @brief Create a VertexPropertyArrowChunkReader instance from graph info and
   * property group.
   *
   * @param graph_info The graph info.
   * @param type The vertex type.
   * @param property_group The property group of the vertex property.
   * @param options The filter options, default is empty.
   */
  static Result<std::shared_ptr<VertexPropertyArrowChunkReader>> Make(
      const std::shared_ptr<GraphInfo>& graph_info, const std::string& type,
      const std::shared_ptr<PropertyGroup>& property_group,
      const util::FilterOptions& options = {});

  /**
   * @brief Create a VertexPropertyArrowChunkReader instance from graph info and
   * property name.
   *
   * @param graph_info The graph info.
   * @param type The vertex type.
   * @param property_name The name of one property in the property group you
   * want to read.
   * @param options The filter options, default is empty.
   */
  static Result<std::shared_ptr<VertexPropertyArrowChunkReader>> Make(
      const std::shared_ptr<GraphInfo>& graph_info, const std::string& type,
      const std::string& property_name,
      const util::FilterOptions& options = {});
  /**
   * @brief Create a VertexPropertyArrowChunkReader instance from vertex info
   * for labels.
   *
   * @param vertex_info The vertex info.
   * @param labels The name of labels you want to read.
   * @param select_type The select type, properties or labels.
   * @param prefix The absolute prefix of the graph.
   * @param options The filter options, default is empty.
   */

  static Result<std::shared_ptr<VertexPropertyArrowChunkReader>> Make(
      const std::shared_ptr<GraphInfo>& graph_info, const std::string& type,
      const std::vector<std::string>& property_names_or_labels,
      const SelectType select_type, const util::FilterOptions& options = {});

  /**
   * @brief Create a VertexPropertyArrowChunkReader instance from vertex info
   * for labels.
   *
   * @param vertex_info The vertex info.
   * @param labels The name of labels you want to read.
   * @param prefix The absolute prefix of the graph.
   * @param options The filter options, default is empty.
   */
  static Result<std::shared_ptr<VertexPropertyArrowChunkReader>> Make(
      const std::shared_ptr<VertexInfo>& vertex_info,
      const std::vector<std::string>& labels, const std::string& prefix,
      const util::FilterOptions& options = {});

  /**
   * @brief Create a VertexPropertyArrowChunkReader instance from graph info
   * for properties.
   *
   * @param graph_info The graph info.
   * @param type The vertex type.
   * @param property_names The name of properties you want to read.
   * @param prefix The absolute prefix of the graph.
   * @param options The filter options, default is empty.
   */
  static Result<std::shared_ptr<VertexPropertyArrowChunkReader>>
  MakeForProperties(const std::shared_ptr<GraphInfo>& graph_info,
                    const std::string& type,
                    const std::vector<std::string>& property_names,
                    const util::FilterOptions& options = {});

  /**
   * @brief Create a VertexPropertyArrowChunkReader instance from graph info
   * for labels.
   *
   * @param graph_info The graph info.
   * @param type The vertex type.
   * @param labels The name of labels you want to read.
   * @param prefix The absolute prefix of the graph.
   * @param options The filter options, default is empty.
   */
  static Result<std::shared_ptr<VertexPropertyArrowChunkReader>> MakeForLabels(
      const std::shared_ptr<GraphInfo>& graph_info, const std::string& type,
      const std::vector<std::string>& labels,
      const util::FilterOptions& options = {});

 private:
  /**
   * @brief Read the chunk through the scanner.
   */
  Result<std::shared_ptr<arrow::Table>> GetChunkV1();
  /**
   * @brief Read the chunk through the reader.
   */
  Result<std::shared_ptr<arrow::Table>> GetChunkV2();

 private:
  std::shared_ptr<VertexInfo> vertex_info_;
  std::shared_ptr<PropertyGroup> property_group_;
  std::vector<std::string> property_names_;
  std::string prefix_;
  std::vector<std::string> labels_;
  IdType chunk_index_;
  IdType seek_id_;
  IdType chunk_num_;
  IdType vertex_num_;
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<arrow::Table> chunk_table_;
  util::FilterOptions filter_options_;
  std::shared_ptr<FileSystem> fs_;
};

/**
 * @brief The arrow chunk reader for adj list topology chunk.
 */
class AdjListArrowChunkReader {
 public:
  using range_t = std::pair<IdType, IdType>;
  /**
   * @brief Initialize the AdjListArrowChunkReader.
   *
   * @param edge_info The edge info that describes the edge type.
   * @param adj_list_type The adj list type for the edge.
   * @param prefix The absolute prefix.
   */
  AdjListArrowChunkReader(const std::shared_ptr<EdgeInfo>& edge_info,
                          AdjListType adj_list_type, const std::string& prefix);

  /**
   * @brief Copy constructor.
   */
  AdjListArrowChunkReader(const AdjListArrowChunkReader& other);

  /**
   * @brief Sets chunk position indicator for reader by source vertex id.
   *
   * @param id the source vertex id.
   */
  Status seek_src(IdType id);

  /**
   * @brief Sets chunk position indicator for reader by destination vertex id.
   *
   * @param id the destination vertex id.
   */
  Status seek_dst(IdType offset);

  /**
   * @brief Sets chunk position indicator for reader by edge index.
   *
   * @param offset edge index of the vertex chunk.
   *     Note: the offset is the edge index of the vertex chunk, not the edge
   * index of the whole graph.
   */
  Status seek(IdType offset);

  /**
   * @brief Return the current chunk of chunk position indicator as
   * arrow::Table, if the chunk is empty, return nullptr.
   */
  Result<std::shared_ptr<arrow::Table>> GetChunk();

  /**
   * @brief Get the number of rows of the current chunk table.
   */
  Result<IdType> GetRowNumOfChunk();

  /**
   * @brief Sets chunk position indicator to next chunk.
   *
   * @return Status: ok or EndOfChunk error if the reader is at the end of
   *         current vertex chunk, or IndexError error if the reader is at the
   *         end of all vertex chunks.
   */
  Status next_chunk();

  /**
   * @brief Sets chunk position to the specific vertex chunk and edge chunk.
   *
   * @param vertex_chunk_index the vertex chunk index.
   * @param chunk_index the edge chunk index of vertex_chunk_index.
   * @return Status: ok or error
   */
  Status seek_chunk_index(IdType vertex_chunk_index, IdType chunk_index = 0);

  /**
   * @brief Create an AdjListArrowChunkReader instance from edge info.
   *
   * @param edge_info The edge info.
   * @param adj_list_type The adj list type for the edges.
   * @param prefix The absolute prefix of the graph.
   */
  static Result<std::shared_ptr<AdjListArrowChunkReader>> Make(
      const std::shared_ptr<EdgeInfo>& edge_info, AdjListType adj_list_type,
      const std::string& prefix);

  /**
   * @brief Create an AdjListArrowChunkReader instance from graph info.
   *
   * @param graph_info The graph info.
   * @param src_type The source vertex type.
   * @param edge_type The edge type.
   * @param dst_type The destination vertex type.
   * @param adj_list_type The adj list type for the edges.
   */
  static Result<std::shared_ptr<AdjListArrowChunkReader>> Make(
      const std::shared_ptr<GraphInfo>& graph_info, const std::string& src_type,
      const std::string& edge_type, const std::string& dst_type,
      AdjListType adj_list_type);

 private:
  Status initOrUpdateEdgeChunkNum();

 private:
  std::shared_ptr<EdgeInfo> edge_info_;
  AdjListType adj_list_type_;
  std::string prefix_;
  IdType vertex_chunk_index_, chunk_index_;
  IdType seek_offset_;
  std::shared_ptr<arrow::Table> chunk_table_;
  IdType vertex_chunk_num_, chunk_num_;
  std::string base_dir_;
  std::shared_ptr<FileSystem> fs_;
};

/**
 * @brief The arrow chunk reader for edge offset.
 */
class AdjListOffsetArrowChunkReader {
 public:
  using range_t = std::pair<IdType, IdType>;
  /**
   * @brief Initialize the AdjListOffsetArrowChunkReader.
   *
   * @param edge_info The edge info that describes the edge type.
   * @param adj_list_type The adj list type for the edges.
   *    Note that the adj list type must be AdjListType::ordered_by_source
   *    or AdjListType::ordered_by_dest.
   * @param prefix The absolute prefix.
   */
  AdjListOffsetArrowChunkReader(const std::shared_ptr<EdgeInfo>& edge_info,
                                AdjListType adj_list_type,
                                const std::string& prefix);

  /**
   * @brief Sets chunk position indicator for reader by internal vertex id.
   *    If internal vertex id is not found, will return Status::IndexError
   * error. After seeking to an invalid vertex id, the next call to GetChunk
   * function may undefined, e.g. return an non exist path.
   *
   * @param id the internal vertex id.
   */
  Status seek(IdType id);

  /**
   * @brief Get the current offset chunk as arrow::Array.
   */
  Result<std::shared_ptr<arrow::Array>> GetChunk();

  /**
   * @brief Sets chunk position indicator to next chunk.
   *     if current chunk is the last chunk, will return Status::IndexError
   * error.
   */
  Status next_chunk();

  /**
   * @brief Get current vertex chunk index.
   */
  IdType GetChunkIndex() const noexcept { return chunk_index_; }

  /**
   * @brief Create an AdjListOffsetArrowChunkReader instance from edge info.
   *
   * @param edge_info The edge info.
   * @param adj_list_type The adj list type for the edges.
   * @param prefix The absolute prefix of the graph.
   */
  static Result<std::shared_ptr<AdjListOffsetArrowChunkReader>> Make(
      const std::shared_ptr<EdgeInfo>& edge_info, AdjListType adj_list_type,
      const std::string& prefix);

  /**
   * @brief Create an AdjListOffsetArrowChunkReader instance from graph info.
   *
   * @param graph_info The graph info.
   * @param src_type The source vertex type.
   * @param edge_type The edge type.
   * @param dst_type The destination vertex type.
   * @param adj_list_type The adj list type for the edges.
   */
  static Result<std::shared_ptr<AdjListOffsetArrowChunkReader>> Make(
      const std::shared_ptr<GraphInfo>& graph_info, const std::string& src_type,
      const std::string& edge_type, const std::string& dst_type,
      AdjListType adj_list_type);

 private:
  std::shared_ptr<EdgeInfo> edge_info_;
  AdjListType adj_list_type_;
  std::string prefix_;
  IdType chunk_index_;
  IdType seek_id_;
  std::shared_ptr<arrow::Table> chunk_table_;
  IdType vertex_chunk_num_;
  IdType vertex_chunk_size_;
  std::string base_dir_;
  std::shared_ptr<FileSystem> fs_;
};

/**
 * @brief The arrow chunk reader for edge property group chunks.
 */
class AdjListPropertyArrowChunkReader {
 public:
  using range_t = std::pair<IdType, IdType>;
  /**
   * @brief Initialize the AdjListPropertyArrowChunkReader.
   *
   * @param edge_info The edge info that describes the edge type.
   * @param property_group The property group that describes the property
   * group.
   * @param adj_list_type The adj list type for the edges.
   * @param prefix The absolute prefix.
   */
  AdjListPropertyArrowChunkReader(
      const std::shared_ptr<EdgeInfo>& edge_info,
      const std::shared_ptr<PropertyGroup>& property_group,
      AdjListType adj_list_type, const std::string prefix,
      const util::FilterOptions& options = {});

  /**
   * @brief Copy constructor.
   */
  AdjListPropertyArrowChunkReader(const AdjListPropertyArrowChunkReader& other);

  /**
   * @brief Sets chunk position indicator for reader by source vertex id.
   *
   * @param id the source vertex id.
   */
  Status seek_src(IdType id);

  /**
   * @brief Sets chunk position indicator for reader by destination vertex id.
   *
   * @param id the destination vertex id.
   */
  Status seek_dst(IdType id);

  /**
   * @brief Sets chunk position indicator for reader by edge index.
   *
   * @param offset edge index of the vertex chunk.
   *     Note: the offset is the edge index of the vertex chunk, not the edge
   * index of the whole graph.
   */
  Status seek(IdType offset);

  /**
   * @brief Return the current chunk of chunk position indicator as
   * arrow::Table, if the chunk is empty, return nullptr.
   */
  Result<std::shared_ptr<arrow::Table>> GetChunk();

  /**
   * @brief Sets chunk position indicator to next chunk.
   *
   * @return Status: ok or EndOfChunk error if the reader is at the end of
   *         current vertex chunk, or IndexError error if the reader is at the
   *         end of all vertex chunks.
   */
  Status next_chunk();

  /**
   * @brief Sets chunk position to the specific vertex chunk and edge chunk.
   *
   * @param vertex_chunk_index the vertex chunk index.
   * @param chunk_index the edge chunk index of vertex_chunk_index.
   * @return Status: ok or error
   */
  Status seek_chunk_index(IdType vertex_chunk_index, IdType chunk_index = 0);

  /**
   * @brief Apply the row filter to the table. No parameter call Filter() will
   * clear the filter.
   *
   * @param filter Predicate expression to filter rows.
   */
  void Filter(util::Filter filter = nullptr);

  /**
   * @brief Apply the projection to the table to be read. No parameter call
   * Select() will clear the projection.
   *
   * @param column_names The name of columns to be selected.
   */
  void Select(util::ColumnNames column_names = std::nullopt);

  /**
   * @brief Create an AdjListPropertyArrowChunkReader instance from edge info.
   *
   * @param edge_info The edge info that describes the edge type.
   * @param property_group The property group that describes the property
   * @param adj_list_type The adj list type for the edges.
   * @param prefix The absolute prefix of the graph.
   * @param options The filter options, default is empty.
   */
  static Result<std::shared_ptr<AdjListPropertyArrowChunkReader>> Make(
      const std::shared_ptr<EdgeInfo>& edge_info,
      const std::shared_ptr<PropertyGroup>& property_group,
      AdjListType adj_list_type, const std::string& prefix,
      const util::FilterOptions& options = {});

  /**
   * @brief Create an AdjListPropertyArrowChunkReader instance from graph info
   * and property group.
   *
   * @param graph_info The graph info that describes the graph.
   * @param src_type The source vertex type.
   * @param edge_type The edge type.
   * @param dst_type The destination vertex type.
   * @param property_group The property group that describes the property
   * group.
   * @param adj_list_type The adj list type for the edges.
   * @param options The filter options, default is empty.
   */
  static Result<std::shared_ptr<AdjListPropertyArrowChunkReader>> Make(
      const std::shared_ptr<GraphInfo>& graph_info, const std::string& src_type,
      const std::string& edge_type, const std::string& dst_type,
      const std::shared_ptr<PropertyGroup>& property_group,
      AdjListType adj_list_type, const util::FilterOptions& options = {});

  /**
   * @brief Create an AdjListPropertyArrowChunkReader instance from graph info
   * and property name.
   *
   * @param graph_info The graph info that describes the graph.
   * @param src_type The source vertex type.
   * @param edge_type The edge type.
   * @param dst_type The destination vertex type.
   * @param property_name The name of one property in the property group you
   * want to read.
   * @param adj_list_type The adj list type for the edges.
   * @param options The filter options, default is empty.
   */
  static Result<std::shared_ptr<AdjListPropertyArrowChunkReader>> Make(
      const std::shared_ptr<GraphInfo>& graph_info, const std::string& src_type,
      const std::string& edge_type, const std::string& dst_type,
      const std::string& property_name, AdjListType adj_list_type,
      const util::FilterOptions& options = {});

 private:
  Status initOrUpdateEdgeChunkNum();

 private:
  std::shared_ptr<EdgeInfo> edge_info_;
  std::shared_ptr<PropertyGroup> property_group_;
  AdjListType adj_list_type_;
  std::string prefix_;
  IdType vertex_chunk_index_, chunk_index_;
  IdType seek_offset_;
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<arrow::Table> chunk_table_;
  util::FilterOptions filter_options_;
  IdType vertex_chunk_num_, chunk_num_;
  std::string base_dir_;
  std::shared_ptr<FileSystem> fs_;
};
}  // namespace graphar
