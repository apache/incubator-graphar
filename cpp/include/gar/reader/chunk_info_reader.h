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

#ifndef GAR_READER_CHUNK_INFO_READER_H_
#define GAR_READER_CHUNK_INFO_READER_H_

#include <memory>
#include <string>
#include <vector>

#include "gar/graph_info.h"
#include "gar/utils/data_type.h"
#include "gar/utils/filesystem.h"
#include "gar/utils/reader_utils.h"
#include "gar/utils/result.h"
#include "gar/utils/status.h"
#include "gar/utils/utils.h"

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
    GAR_ASSIGN_OR_RAISE_ERROR(chunk_num_, utils::GetVertexChunkNum(prefix_, vertex_info_));
  }

  /**
   * @brief Sets chunk position indicator for reader by vertex id.
   *    If vertex id is not found, will return Status::KeyError error.
   *    After seeking to an invalid vertex id, the next call to GetChunk
   * function may undefined, e.g. return an non exist path.
   *
   * @param id the vertex id.
   */
  inline Status seek(IdType id) noexcept {
    chunk_index_ = id / vertex_info_.GetChunkSize();
    if (chunk_index_ >= chunk_num_) {
      return Status::KeyError("vertex id is not found, id: " +
                              std::to_string(id));
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
   * if current chunk is the last chunk, will return Status::OutOfRange
   *   error.
   */
  Status next_chunk() noexcept {
    if (++chunk_index_ >= chunk_num_) {
      return Status::OutOfRange();
    }
    return Status::OK();
  }

  /** Get the chunk number of the current vertex property group. */
  IdType GetChunkNum() noexcept { return chunk_num_; }

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
    GAR_ASSIGN_OR_RAISE_ERROR(vertex_chunk_num_,
                              fs_->GetFileNumOfDir(base_dir_));
    GAR_ASSIGN_OR_RAISE_ERROR(chunk_num_, utils::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_, vertex_chunk_index_));
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
  Status seek(IdType index) noexcept {
    chunk_index_ = index / edge_info_.GetChunkSize();
    if (chunk_index_ >= chunk_num_) {
      return Status::KeyError("The index is out of range.");
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
   * if current chunk is the last chunk, will return Status::OutOfRange
   *     error.
   */
  Status next_chunk() {
    if (++chunk_index_ >= chunk_num_) {
      ++vertex_chunk_index_;
      if (vertex_chunk_index_ >= vertex_chunk_num_) {
        return Status::OutOfRange();
      }
      chunk_index_ = 0;
    GAR_ASSIGN_OR_RAISE_ERROR(chunk_num_, utils::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_, vertex_chunk_index_));
    }
    return Status::OK();
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
    GAR_ASSIGN_OR_RAISE_ERROR(vertex_chunk_num_,
                              fs_->GetFileNumOfDir(base_dir_));
    GAR_ASSIGN_OR_RAISE_ERROR(chunk_num_, utils::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_, vertex_chunk_index_));
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
      return Status::KeyError("The offset is out of range.");
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
   * if current chunk is the last chunk, will return Status::OutOfRange
   *  error.
   */
  Status next_chunk() {
    if (++chunk_index_ >= chunk_num_) {
      ++vertex_chunk_index_;
      if (vertex_chunk_index_ >= vertex_chunk_num_) {
        return Status::OutOfRange();
      }
      chunk_index_ = 0;
    GAR_ASSIGN_OR_RAISE_ERROR(chunk_num_, utils::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_, vertex_chunk_index_));
    }
    return Status::OK();
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

/**
 * @brief Helper function to Construct VertexPropertyChunkInfoReader.
 *
 * @param graph_info The graph info to describe the graph.
 * @param label label name of the vertex.
 * @param property_group The property group of the vertex.
 */
static inline Result<VertexPropertyChunkInfoReader>
ConstructVertexPropertyChunkInfoReader(
    const GraphInfo& graph_info, const std::string& label,
    const PropertyGroup& property_group) noexcept {
  VertexInfo vertex_info;
  GAR_ASSIGN_OR_RAISE(vertex_info, graph_info.GetVertexInfo(label));
  if (!vertex_info.ContainPropertyGroup(property_group)) {
    return Status::Invalid();
  }
  return VertexPropertyChunkInfoReader(vertex_info, property_group,
                                       graph_info.GetPrefix());
}

/**
 * @brief Helper function to Construct AdjListChunkInfoReader.
 *
 * @param graph_info The graph info to describe the graph.
 * @param src_label label of source vertex.
 * @param edge_label label of edge.
 * @param dst_label label of destination vertex.
 * @param adj_list_type The adj list type for the edges.
 */
static inline Result<AdjListChunkInfoReader> ConstructAdjListChunkInfoReader(
    const GraphInfo& graph_info, const std::string& src_label,
    const std::string& edge_label, const std::string& dst_label,
    AdjListType adj_list_type) noexcept {
  EdgeInfo edge_info;
  GAR_ASSIGN_OR_RAISE(edge_info,
                      graph_info.GetEdgeInfo(src_label, edge_label, dst_label));
  if (!edge_info.ContainAdjList(adj_list_type)) {
    return Status::Invalid();
  }

  return AdjListChunkInfoReader(edge_info, adj_list_type,
                                graph_info.GetPrefix());
}

/**
 * @brief Helper function to Construct AdjListPropertyChunkInfoReader.
 *
 * @param graph_info The graph info to describe the graph.
 * @param src_label label of source vertex.
 * @param edge_label label of edge.
 * @param dst_label label of destination vertex.
 * @param property_group The property group of the edge.
 * @param adj_list_type The adj list type for the edges.
 */
static inline Result<AdjListPropertyChunkInfoReader>
ConstructAdjListPropertyChunkInfoReader(const GraphInfo& graph_info,
                                        const std::string& src_label,
                                        const std::string& edge_label,
                                        const std::string& dst_label,
                                        const PropertyGroup& property_group,
                                        AdjListType adj_list_type) noexcept {
  EdgeInfo edge_info;
  GAR_ASSIGN_OR_RAISE(edge_info,
                      graph_info.GetEdgeInfo(src_label, edge_label, dst_label));
  if (!edge_info.ContainPropertyGroup(property_group, adj_list_type)) {
    return Status::Invalid();
  }

  return AdjListPropertyChunkInfoReader(edge_info, property_group,
                                        adj_list_type, graph_info.GetPrefix());
}

}  // namespace GAR_NAMESPACE_INTERNAL
#endif  // GAR_READER_CHUNK_INFO_READER_H_
