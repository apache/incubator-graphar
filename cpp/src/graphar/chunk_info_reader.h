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
#include <vector>

#include "graphar/fwd.h"

namespace graphar {

/** The chunk info reader for vertex property group. */
class VertexPropertyChunkInfoReader {
 public:
  ~VertexPropertyChunkInfoReader() {}

  /**
   * @brief Initialize the VertexPropertyChunkInfoReader.
   *
   * @param vertex_info The vertex info that describes the vertex type.
   * @param property_group The property group that describes the property group.
   * @param prefix The absolute prefix of the graph.
   */
  explicit VertexPropertyChunkInfoReader(
      const std::shared_ptr<VertexInfo>& vertex_info,
      const std::shared_ptr<PropertyGroup>& property_group,
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
   * @brief Return the current chunk file path of chunk position indicator.
   */
  Result<std::string> GetChunk() const;

  /**
   * Sets chunk position indicator to next chunk.
   *
   * if current chunk is the last chunk, will return Status::IndexError
   *   error.
   */
  Status next_chunk();

  /** Get the chunk number of the current vertex property group. */
  IdType GetChunkNum() const noexcept { return chunk_num_; }

  /**
   * @brief Create a VertexPropertyChunkInfoReader instance from vertex info.
   *
   * @param vertex_info The vertex info.
   * @param property_group The property group of the vertex property.
   * @param prefix The absolute prefix of the graph.
   */
  static Result<std::shared_ptr<VertexPropertyChunkInfoReader>> Make(
      const std::shared_ptr<VertexInfo>& vertex_info,
      const std::shared_ptr<PropertyGroup>& property_group,
      const std::string& prefix);

  /**
   * @brief Create a VertexPropertyChunkInfoReader instance from graph info and
   * property group
   *
   * @param graph_info The graph info.
   * @param type The vertex type.
   * @param property_group The property group of the vertex property.
   */
  static Result<std::shared_ptr<VertexPropertyChunkInfoReader>> Make(
      const std::shared_ptr<GraphInfo>& graph_info, const std::string& type,
      const std::shared_ptr<PropertyGroup>& property_group);

  /**
   *  @brief Create a VertexPropertyChunkInfoReader instance from graph info and
   * property name
   *
   * @param graph_info The graph info.
   * @param type The vertex type.
   * @param property_name The name of one property in the property group you
   * want to read.
   */
  static Result<std::shared_ptr<VertexPropertyChunkInfoReader>> Make(
      const std::shared_ptr<GraphInfo>& graph_info, const std::string& type,
      const std::string& property_name);

 private:
  std::shared_ptr<VertexInfo> vertex_info_;
  std::shared_ptr<PropertyGroup> property_group_;
  std::string prefix_;
  IdType chunk_index_;
  IdType chunk_num_;
};

/** The chunk info reader for adj list topology chunk. */
class AdjListChunkInfoReader {
 public:
  ~AdjListChunkInfoReader() {}

  /**
   * @brief Initialize the AdjListChunkInfoReader.
   *
   * @param edge_info The edge info that describes the edge type.
   * @param adj_list_type The adj list type for the edges.
   * @param prefix The absolute prefix of the graph.
   */
  explicit AdjListChunkInfoReader(const std::shared_ptr<EdgeInfo>& edge_info,
                                  AdjListType adj_list_type,
                                  const std::string& prefix);

  /**
   * @brief Sets chunk position indicator for reader by source internal vertex
   * id.
   *
   * @param id the source internal vertex id.
   */
  Status seek_src(IdType id);

  /**
   * @brief Sets chunk position indicator for reader by destination internal
   * vertex id.
   *
   * @param id the destination internal vertex id.
   */
  Status seek_dst(IdType id);

  /**
   * @brief Sets chunk position indicator for reader by edge index.
   *
   * @param offset edge index of the vertex chunk.
   *     Note: the offset is the edge index of the vertex chunk, not the edge
   * index of the whole graph.
   */
  Status seek(IdType index);

  /** Return the current chunk file path of chunk position indicator. */
  Result<std::string> GetChunk();

  /**
   * Sets chunk position indicator to next chunk.
   *
   * if current chunk is the last chunk, will return Status::IndexError
   *     error.
   */
  Status next_chunk();

  /**
   * @brief Create an AdjListChunkInfoReader instance from edge info.
   *
   * @param edge_info The edge info.
   * @param adj_list_type The adj list type for the edges.
   * @param prefix The absolute prefix of the graph.
   */
  static Result<std::shared_ptr<AdjListChunkInfoReader>> Make(
      const std::shared_ptr<EdgeInfo>& edge_info, AdjListType adj_list_type,
      const std::string& prefix);

  /**
   * @brief Create an AdjListChunkInfoReader instance from graph info.
   *
   * @param graph_info The graph info.
   * @param src_type The source vertex type.
   * @param edge_type The edge type.
   * @param dst_type The destination vertex type.
   * @param adj_list_type The adj list type for the edges.
   */
  static Result<std::shared_ptr<AdjListChunkInfoReader>> Make(
      const std::shared_ptr<GraphInfo>& graph_info, const std::string& src_type,
      const std::string& edge_type, const std::string& dst_type,
      AdjListType adj_list_type);

 private:
  std::shared_ptr<EdgeInfo> edge_info_;
  AdjListType adj_list_type_;
  std::string prefix_;
  IdType vertex_chunk_index_, chunk_index_;
  IdType vertex_chunk_num_, chunk_num_;
  std::string base_dir_;  // the chunk files base dir
  std::shared_ptr<FileSystem> fs_;
};

class AdjListOffsetChunkInfoReader {
 public:
  ~AdjListOffsetChunkInfoReader() {}

  /**
   * @brief Initialize the AdjListOffsetChunkInfoReader.
   *
   * @param edge_info The edge info that describes the edge type.
   * @param adj_list_type The adj list type for the edges.
   *    Note that the adj list type must be AdjListType::ordered_by_source
   *    or AdjListType::ordered_by_dest.
   * @param prefix The absolute prefix.
   */
  explicit AdjListOffsetChunkInfoReader(
      const std::shared_ptr<EdgeInfo>& edge_info, AdjListType adj_list_type,
      const std::string& prefix);

  /**
   * @brief Sets chunk position indicator for reader by source internal vertex
   * id.
   *
   * @param id the source internal vertex id.
   */
  Status seek(IdType id);

  /**
   * @brief Return the current chunk file path of chunk position indicator.
   */
  Result<std::string> GetChunk() const;

  /**
   * Sets chunk position indicator to next chunk.
   *
   * if current chunk is the last chunk, will return Status::IndexError
   *   error.
   */
  Status next_chunk();

  /**
   * @brief Create an AdjListOffsetChunkInfoReader instance from edge info.
   *
   * @param edge_info The edge info.
   * @param adj_list_type The adj list type for the edges.
   * @param prefix The absolute prefix of the graph.
   */
  static Result<std::shared_ptr<AdjListOffsetChunkInfoReader>> Make(
      const std::shared_ptr<EdgeInfo>& edge_info, AdjListType adj_list_type,
      const std::string& prefix);

  /**
   * @brief Create an AdjListOffsetChunkInfoReader instance from graph info.
   *
   * @param graph_info The graph info.
   * @param src_type The source vertex type.
   * @param edge_type The edge type.
   * @param dst_type The destination vertex type.
   * @param adj_list_type The adj list type for the edges.
   */
  static Result<std::shared_ptr<AdjListOffsetChunkInfoReader>> Make(
      const std::shared_ptr<GraphInfo>& graph_info, const std::string& src_type,
      const std::string& edge_type, const std::string& dst_type,
      AdjListType adj_list_type);

 private:
  std::shared_ptr<EdgeInfo> edge_info_;
  AdjListType adj_list_type_;
  std::string prefix_;
  IdType chunk_index_;
  IdType vertex_chunk_size_;
  IdType vertex_chunk_num_;
};

/**
 * The chunk info reader for edge property group chunk.
 */
class AdjListPropertyChunkInfoReader {
 public:
  /**
   * @brief Initialize the AdjListPropertyChunkInfoReader.
   *
   * @param edge_info The edge info that describes the edge type.
   * @param property_group The property group of the edge property.
   * @param adj_list_type The adj list type for the edges.
   * @param prefix The absolute prefix of the graph.
   */
  explicit AdjListPropertyChunkInfoReader(
      const std::shared_ptr<EdgeInfo>& edge_info,
      const std::shared_ptr<PropertyGroup>& property_group,
      AdjListType adj_list_type, const std::string prefix);

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

  /** Return the current chunk file path of chunk position indicator. */
  Result<std::string> GetChunk() const;

  /**
   * Sets chunk position indicator to next chunk.
   *
   * if current chunk is the last chunk, will return Status::IndexError
   *  error.
   */
  Status next_chunk();

  /**
   * @brief Create an AdjListPropertyChunkInfoReader instance from edge info.
   *
   * @param edge_info The edge info.
   * @param property_group The property group of the edge property.
   * @param adj_list_type The adj list type for the edge.
   * @param prefix The absolute prefix of the graph.
   */
  static Result<std::shared_ptr<AdjListPropertyChunkInfoReader>> Make(
      const std::shared_ptr<EdgeInfo>& edge_info,
      const std::shared_ptr<PropertyGroup>& property_group,
      AdjListType adj_list_type, const std::string& prefix);

  /**
   * @brief Create an AdjListPropertyChunkInfoReader instance from graph info
   * and property group.
   *
   * @param graph_info The graph info.
   * @param src_type The source vertex type.
   * @param edge_type The edge type.
   * @param dst_type The destination vertex type.
   * @param property_group The property group of the edge property.
   * @param adj_list_type The adj list type for the edge.
   */
  static Result<std::shared_ptr<AdjListPropertyChunkInfoReader>> Make(
      const std::shared_ptr<GraphInfo>& graph_info, const std::string& src_type,
      const std::string& edge_type, const std::string& dst_type,
      const std::shared_ptr<PropertyGroup>& property_group,
      AdjListType adj_list_type);

  /**
   * @brief Create an AdjListPropertyChunkInfoReader instance from graph info
   * and property name.
   *
   * @param graph_info The graph info.
   * @param src_type The source vertex type.
   * @param edge_type The edge type.
   * @param dst_type The destination vertex type.
   * @param property_name The name of one property in the property group you
   * want to read.
   * @param adj_list_type The adj list type for the edge.
   */
  static Result<std::shared_ptr<AdjListPropertyChunkInfoReader>> Make(
      const std::shared_ptr<GraphInfo>& graph_info, const std::string& src_type,
      const std::string& edge_type, const std::string& dst_type,
      const std::string& property_name, AdjListType adj_list_type);

 private:
  std::shared_ptr<EdgeInfo> edge_info_;
  std::shared_ptr<PropertyGroup> property_group_;
  AdjListType adj_list_type_;
  std::string prefix_;
  IdType vertex_chunk_index_, chunk_index_;
  IdType vertex_chunk_num_, chunk_num_;
  std::string base_dir_;  // the chunk files base dir
  std::shared_ptr<FileSystem> fs_;
};
}  // namespace graphar
