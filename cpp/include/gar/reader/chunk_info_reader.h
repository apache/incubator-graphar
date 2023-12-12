/*
 * Copyright 2022-2023 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#ifndef GAR_READER_CHUNK_INFO_READER_H_
#define GAR_READER_CHUNK_INFO_READER_H_

#include <memory>
#include <string>
#include <vector>

#include "gar/graph_info.h"
#include "gar/util/data_type.h"
#include "gar/util/filesystem.h"
#include "gar/util/reader_util.h"
#include "gar/util/result.h"
#include "gar/util/status.h"
#include "gar/util/util.h"

namespace GAR_NAMESPACE_INTERNAL {

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
  explicit VertexPropertyChunkInfoReader(const VertexInfo& vertex_info,
                                         const PropertyGroup& property_group,
                                         const std::string& prefix)
      : vertex_info_(vertex_info),
        property_group_(property_group),
        prefix_(prefix),
        chunk_index_(0) {
    // init vertex chunk num
    std::string base_dir;
    GAR_ASSIGN_OR_RAISE_ERROR(auto fs,
                              FileSystemFromUriOrPath(prefix, &base_dir));
    GAR_ASSIGN_OR_RAISE_ERROR(auto pg_path_prefix,
                              vertex_info.GetPathPrefix(property_group));
    base_dir += pg_path_prefix;
    GAR_ASSIGN_OR_RAISE_ERROR(chunk_num_,
                              util::GetVertexChunkNum(prefix_, vertex_info_));
  }

  /**
   * @brief Sets chunk position indicator for reader by internal vertex id.
   *    If internal vertex id is not found, will return Status::IndexError
   * error. After seeking to an invalid vertex id, the next call to GetChunk
   * function may undefined, e.g. return an non exist path.
   *
   * @param id the internal vertex id.
   */
  inline Status seek(IdType id) noexcept {
    chunk_index_ = id / vertex_info_.GetChunkSize();
    if (chunk_index_ >= chunk_num_) {
      return Status::IndexError("Internal vertex id ", id,
                                " is out of range [0,",
                                chunk_num_ * vertex_info_.GetChunkSize(),
                                ") of vertex ", vertex_info_.GetLabel());
    }
    return Status::OK();
  }

  /**
   * @brief Return the current chunk file path of chunk position indicator.
   */
  Result<std::string> GetChunk() noexcept {
    GAR_ASSIGN_OR_RAISE(
        auto chunk_file_path,
        vertex_info_.GetFilePath(property_group_, chunk_index_));
    return prefix_ + chunk_file_path;
  }

  /**
   * Sets chunk position indicator to next chunk.
   *
   * if current chunk is the last chunk, will return Status::IndexError
   *   error.
   */
  Status next_chunk() noexcept {
    if (++chunk_index_ >= chunk_num_) {
      return Status::IndexError(
          "vertex chunk index ", chunk_index_, " is out-of-bounds for vertex ",
          vertex_info_.GetLabel(), " chunk num ", chunk_num_);
    }
    return Status::OK();
  }

  /** Get the chunk number of the current vertex property group. */
  IdType GetChunkNum() noexcept { return chunk_num_; }

  /**
   * @brief Construct a VertexPropertyChunkInfoReader from vertex info.
   *
   * @param vertex_info The vertex info.
   * @param property_group The property group of the vertex property.
   * @param prefix The absolute prefix of the graph.
   */
  static Result<std::shared_ptr<VertexPropertyChunkInfoReader>> Make(
      const VertexInfo& vertex_info, const PropertyGroup& property_group,
      const std::string& prefix) {
    if (!vertex_info.ContainPropertyGroup(property_group)) {
      return Status::KeyError("No property group ", property_group,
                              " in vertex ", vertex_info.GetLabel(), ".");
    }
    return std::make_shared<VertexPropertyChunkInfoReader>(
        vertex_info, property_group, prefix);
  }

  /**
   * @brief Construct a VertexPropertyChunkInfoReader from graph info.
   *
   * @param graph_info The graph info.
   * @param label The vertex label.
   * @param property_group The property group of the vertex property.
   */
  static Result<std::shared_ptr<VertexPropertyChunkInfoReader>> Make(
      const GraphInfo& graph_info, const std::string& label,
      const PropertyGroup& property_group) {
    GAR_ASSIGN_OR_RAISE(const auto& vertex_info,
                        graph_info.GetVertexInfo(label));
    return Make(vertex_info, property_group, graph_info.GetPrefix());
  }

 private:
  VertexInfo vertex_info_;
  PropertyGroup property_group_;
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
  explicit AdjListChunkInfoReader(const EdgeInfo& edge_info,
                                  AdjListType adj_list_type,
                                  const std::string& prefix)
      : edge_info_(edge_info),
        adj_list_type_(adj_list_type),
        prefix_(prefix),
        vertex_chunk_index_(0),
        chunk_index_(0) {
    GAR_ASSIGN_OR_RAISE_ERROR(fs_, FileSystemFromUriOrPath(prefix, &base_dir_));
    GAR_ASSIGN_OR_RAISE_ERROR(auto adj_list_path_prefix,
                              edge_info.GetAdjListPathPrefix(adj_list_type));
    base_dir_ = prefix_ + adj_list_path_prefix;
    GAR_ASSIGN_OR_RAISE_ERROR(
        vertex_chunk_num_,
        util::GetVertexChunkNum(prefix_, edge_info_, adj_list_type_));
    GAR_ASSIGN_OR_RAISE_ERROR(
        chunk_num_, util::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_,
                                          vertex_chunk_index_));
  }

  /**
   * @brief Sets chunk position indicator for reader by source internal vertex
   * id.
   *
   * @param id the source internal vertex id.
   */
  Status seek_src(IdType id) noexcept;

  /**
   * @brief Sets chunk position indicator for reader by destination internal
   * vertex id.
   *
   * @param id the destination internal vertex id.
   */
  Status seek_dst(IdType id) noexcept;

  /**
   * @brief Sets chunk position indicator for reader by edge index.
   *
   * @param offset edge index of the vertex chunk.
   *     Note: the offset is the edge index of the vertex chunk, not the edge
   * index of the whole graph.
   */
  Status seek(IdType index) noexcept {
    chunk_index_ = index / edge_info_.GetChunkSize();
    if (chunk_index_ >= chunk_num_) {
      return Status::IndexError("The edge offset ", index,
                                " is out of range [0,",
                                edge_info_.GetChunkSize() * chunk_num_,
                                "), edge label: ", edge_info_.GetEdgeLabel());
    }
    return Status::OK();
  }

  /** Return the current chunk file path of chunk position indicator. */
  Result<std::string> GetChunk() noexcept {
    GAR_ASSIGN_OR_RAISE(auto chunk_file_path,
                        edge_info_.GetAdjListFilePath(
                            vertex_chunk_index_, chunk_index_, adj_list_type_));
    return prefix_ + chunk_file_path;
  }

  /**
   * Sets chunk position indicator to next chunk.
   *
   * if current chunk is the last chunk, will return Status::IndexError
   *     error.
   */
  Status next_chunk() {
    ++chunk_index_;
    while (chunk_index_ >= chunk_num_) {
      ++vertex_chunk_index_;
      if (vertex_chunk_index_ >= vertex_chunk_num_) {
        return Status::IndexError("vertex chunk index ", vertex_chunk_index_,
                                  " is out-of-bounds for vertex chunk num ",
                                  vertex_chunk_num_);
      }
      chunk_index_ = 0;
      GAR_ASSIGN_OR_RAISE_ERROR(
          chunk_num_, util::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_,
                                            vertex_chunk_index_));
    }
    return Status::OK();
  }

  /**
   * @brief Construct an AdjListChunkInfoReader from edge info.
   *
   * @param edge_info The edge info.
   * @param adj_list_type The adj list type for the edges.
   * @param prefix The absolute prefix of the graph.
   */
  static Result<std::shared_ptr<AdjListChunkInfoReader>> Make(
      const EdgeInfo& edge_info, AdjListType adj_list_type,
      const std::string& prefix) {
    if (!edge_info.ContainAdjList(adj_list_type)) {
      return Status::KeyError(
          "The adjacent list type ", AdjListTypeToString(adj_list_type),
          " doesn't exist in edge ", edge_info.GetEdgeLabel(), ".");
    }
    return std::make_shared<AdjListChunkInfoReader>(edge_info, adj_list_type,
                                                    prefix);
  }

  /**
   * @brief Construct an AdjListChunkInfoReader from graph info.
   *
   * @param graph_info The graph info.
   * @param src_label The source vertex label.
   * @param edge_label The edge label.
   * @param dst_label The destination vertex label.
   * @param adj_list_type The adj list type for the edges.
   */
  static Result<std::shared_ptr<AdjListChunkInfoReader>> Make(
      const GraphInfo& graph_info, const std::string& src_label,
      const std::string& edge_label, const std::string& dst_label,
      AdjListType adj_list_type) {
    GAR_ASSIGN_OR_RAISE(
        const auto& edge_info,
        graph_info.GetEdgeInfo(src_label, edge_label, dst_label));
    return Make(edge_info, adj_list_type, graph_info.GetPrefix());
  }

 private:
  EdgeInfo edge_info_;
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
  explicit AdjListOffsetChunkInfoReader(const EdgeInfo& edge_info,
                                        AdjListType adj_list_type,
                                        const std::string& prefix)
      : edge_info_(edge_info),
        adj_list_type_(adj_list_type),
        prefix_(prefix),
        chunk_index_(0) {
    std::string base_dir;
    GAR_ASSIGN_OR_RAISE_ERROR(auto fs,
                              FileSystemFromUriOrPath(prefix, &base_dir));
    GAR_ASSIGN_OR_RAISE_ERROR(auto dir_path,
                              edge_info.GetOffsetPathPrefix(adj_list_type));
    base_dir = prefix_ + dir_path;
    if (adj_list_type == AdjListType::ordered_by_source ||
        adj_list_type == AdjListType::ordered_by_dest) {
      GAR_ASSIGN_OR_RAISE_ERROR(
          vertex_chunk_num_,
          util::GetVertexChunkNum(prefix_, edge_info_, adj_list_type_));
      vertex_chunk_size_ = adj_list_type == AdjListType::ordered_by_source
                               ? edge_info_.GetSrcChunkSize()
                               : edge_info_.GetDstChunkSize();
    } else {
      std::string err_msg = "Invalid adj list type " +
                            std::string(AdjListTypeToString(adj_list_type)) +
                            " to construct AdjListOffsetReader.";
      throw std::runtime_error(err_msg);
    }
  }

  /**
   * @brief Sets chunk position indicator for reader by source internal vertex
   * id.
   *
   * @param id the source internal vertex id.
   */
  inline Status seek(IdType id) noexcept {
    chunk_index_ = id / vertex_chunk_size_;
    if (chunk_index_ >= vertex_chunk_num_) {
      return Status::IndexError(
          "Internal vertex id ", id, " is out of range [0,",
          vertex_chunk_num_ * vertex_chunk_size_, ") of vertex.");
    }
    return Status::OK();
  }

  /**
   * @brief Return the current chunk file path of chunk position indicator.
   */
  Result<std::string> GetChunk() noexcept {
    GAR_ASSIGN_OR_RAISE(
        auto chunk_file_path,
        edge_info_.GetAdjListOffsetFilePath(chunk_index_, adj_list_type_));
    return prefix_ + chunk_file_path;
  }

  /**
   * Sets chunk position indicator to next chunk.
   *
   * if current chunk is the last chunk, will return Status::IndexError
   *   error.
   */
  Status next_chunk() noexcept {
    if (++chunk_index_ >= vertex_chunk_num_) {
      return Status::IndexError("vertex chunk index ", chunk_index_,
                                " is out-of-bounds for vertex, chunk_num ",
                                vertex_chunk_num_);
    }
    return Status::OK();
  }

  /**
   * @brief Construct an AdjListOffsetChunkInfoReader from edge info.
   *
   * @param edge_info The edge info.
   * @param adj_list_type The adj list type for the edges.
   * @param prefix The absolute prefix of the graph.
   */
  static Result<std::shared_ptr<AdjListOffsetChunkInfoReader>> Make(
      const EdgeInfo& edge_info, AdjListType adj_list_type,
      const std::string& prefix) {
    if (!edge_info.ContainAdjList(adj_list_type)) {
      return Status::KeyError(
          "The adjacent list type ", AdjListTypeToString(adj_list_type),
          " doesn't exist in edge ", edge_info.GetEdgeLabel(), ".");
    }
    return std::make_shared<AdjListOffsetChunkInfoReader>(
        edge_info, adj_list_type, prefix);
  }

  /**
   * @brief Construct an AdjListOffsetChunkInfoReader from graph info.
   *
   * @param graph_info The graph info.
   * @param src_label The source vertex label.
   * @param edge_label The edge label.
   * @param dst_label The destination vertex label.
   * @param adj_list_type The adj list type for the edges.
   */
  static Result<std::shared_ptr<AdjListOffsetChunkInfoReader>> Make(
      const GraphInfo& graph_info, const std::string& src_label,
      const std::string& edge_label, const std::string& dst_label,
      AdjListType adj_list_type) {
    GAR_ASSIGN_OR_RAISE(
        const auto& edge_info,
        graph_info.GetEdgeInfo(src_label, edge_label, dst_label));
    return Make(edge_info, adj_list_type, graph_info.GetPrefix());
  }

 private:
  EdgeInfo edge_info_;
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
  explicit AdjListPropertyChunkInfoReader(const EdgeInfo& edge_info,
                                          const PropertyGroup& property_group,
                                          AdjListType adj_list_type,
                                          const std::string prefix)
      : edge_info_(edge_info),
        property_group_(property_group),
        adj_list_type_(adj_list_type),
        prefix_(prefix),
        vertex_chunk_index_(0),
        chunk_index_(0) {
    GAR_ASSIGN_OR_RAISE_ERROR(fs_, FileSystemFromUriOrPath(prefix, &base_dir_));
    GAR_ASSIGN_OR_RAISE_ERROR(
        auto pg_path_prefix,
        edge_info.GetPropertyGroupPathPrefix(property_group, adj_list_type));
    base_dir_ = prefix_ + pg_path_prefix;
    GAR_ASSIGN_OR_RAISE_ERROR(
        vertex_chunk_num_,
        util::GetVertexChunkNum(prefix_, edge_info_, adj_list_type_));
    GAR_ASSIGN_OR_RAISE_ERROR(
        chunk_num_, util::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_,
                                          vertex_chunk_index_));
  }

  /**
   * @brief Sets chunk position indicator for reader by source vertex id.
   *
   * @param id the source vertex id.
   */
  Status seek_src(IdType id) noexcept;

  /**
   * @brief Sets chunk position indicator for reader by destination vertex id.
   *
   * @param id the destination vertex id.
   */
  Status seek_dst(IdType id) noexcept;

  /**
   * @brief Sets chunk position indicator for reader by edge index.
   *
   * @param offset edge index of the vertex chunk.
   *     Note: the offset is the edge index of the vertex chunk, not the edge
   * index of the whole graph.
   */
  Status seek(IdType offset) noexcept {
    chunk_index_ = offset / edge_info_.GetChunkSize();
    if (chunk_index_ >= chunk_num_) {
      return Status::IndexError("The edge offset ", offset,
                                " is out of range [0,",
                                edge_info_.GetChunkSize() * chunk_num_,
                                "), edge label: ", edge_info_.GetEdgeLabel());
    }
    return Status::OK();
  }

  /** Return the current chunk file path of chunk position indicator. */
  Result<std::string> GetChunk() noexcept {
    GAR_ASSIGN_OR_RAISE(
        auto chunk_file_path,
        edge_info_.GetPropertyFilePath(property_group_, adj_list_type_,
                                       vertex_chunk_index_, chunk_index_));
    return prefix_ + chunk_file_path;
  }

  /**
   * Sets chunk position indicator to next chunk.
   *
   * if current chunk is the last chunk, will return Status::IndexError
   *  error.
   */
  Status next_chunk() {
    ++chunk_index_;
    while (chunk_index_ >= chunk_num_) {
      ++vertex_chunk_index_;
      if (vertex_chunk_index_ >= vertex_chunk_num_) {
        return Status::IndexError(
            "vertex chunk index ", vertex_chunk_index_,
            " is out-of-bounds for vertex chunk num ", vertex_chunk_num_,
            " of edge ", edge_info_.GetEdgeLabel(), " of adj list type ",
            AdjListTypeToString(adj_list_type_), ", property group ",
            property_group_, ".");
      }
      chunk_index_ = 0;
      GAR_ASSIGN_OR_RAISE_ERROR(
          chunk_num_, util::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_,
                                            vertex_chunk_index_));
    }
    return Status::OK();
  }

  /**
   * @brief Construct an AdjListPropertyChunkInfoReader from edge info.
   *
   * @param edge_info The edge info.
   * @param property_group The property group of the edge property.
   * @param adj_list_type The adj list type for the edge.
   * @param prefix The absolute prefix of the graph.
   */
  static Result<std::shared_ptr<AdjListPropertyChunkInfoReader>> Make(
      const EdgeInfo& edge_info, const PropertyGroup& property_group,
      AdjListType adj_list_type, const std::string& prefix) {
    if (!edge_info.ContainAdjList(adj_list_type)) {
      return Status::KeyError(
          "The adjacent list type ", AdjListTypeToString(adj_list_type),
          " doesn't exist in edge ", edge_info.GetEdgeLabel(), ".");
    }
    if (!edge_info.ContainPropertyGroup(property_group, adj_list_type)) {
      return Status::KeyError("No property group ", property_group, " in edge ",
                              edge_info.GetEdgeLabel(), " with adj list type ",
                              AdjListTypeToString(adj_list_type), ".");
    }
    return std::make_shared<AdjListPropertyChunkInfoReader>(
        edge_info, property_group, adj_list_type, prefix);
  }

  /**
   * @brief Construct an AdjListPropertyChunkInfoReader from graph info.
   *
   * @param graph_info The graph info.
   * @param src_label The source vertex label.
   * @param edge_label The edge label.
   * @param dst_label The destination vertex label.
   * @param property_group The property group of the edge property.
   * @param adj_list_type The adj list type for the edge.
   */
  static Result<std::shared_ptr<AdjListPropertyChunkInfoReader>> Make(
      const GraphInfo& graph_info, const std::string& src_label,
      const std::string& edge_label, const std::string& dst_label,
      const PropertyGroup& property_group, AdjListType adj_list_type) {
    GAR_ASSIGN_OR_RAISE(
        const auto& edge_info,
        graph_info.GetEdgeInfo(src_label, edge_label, dst_label));
    return Make(edge_info, property_group, adj_list_type,
                graph_info.GetPrefix());
  }

 private:
  EdgeInfo edge_info_;
  PropertyGroup property_group_;
  AdjListType adj_list_type_;
  std::string prefix_;
  IdType vertex_chunk_index_, chunk_index_;
  IdType vertex_chunk_num_, chunk_num_;
  std::string base_dir_;  // the chunk files base dir
  std::shared_ptr<FileSystem> fs_;
};
}  // namespace GAR_NAMESPACE_INTERNAL
#endif  // GAR_READER_CHUNK_INFO_READER_H_
