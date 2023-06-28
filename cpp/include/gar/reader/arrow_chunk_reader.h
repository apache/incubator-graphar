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

#ifndef GAR_READER_ARROW_CHUNK_READER_H_
#define GAR_READER_ARROW_CHUNK_READER_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gar/graph_info.h"
#include "gar/utils/data_type.h"
#include "gar/utils/filesystem.h"
#include "gar/utils/reader_utils.h"
#include "gar/utils/result.h"
#include "gar/utils/status.h"
#include "gar/utils/utils.h"

// forward declaration
namespace arrow {
class Array;
class Table;
}  // namespace arrow

namespace GAR_NAMESPACE_INTERNAL {

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
  VertexPropertyArrowChunkReader(const VertexInfo& vertex_info,
                                 const PropertyGroup& property_group,
                                 const std::string& prefix,
                                 IdType chunk_index = 0)
      : vertex_info_(vertex_info),
        property_group_(property_group),
        chunk_index_(chunk_index),
        seek_id_(chunk_index * vertex_info.GetChunkSize()),
        chunk_table_(nullptr) {
    GAR_ASSIGN_OR_RAISE_ERROR(fs_, FileSystemFromUriOrPath(prefix, &prefix_));
    GAR_ASSIGN_OR_RAISE_ERROR(auto pg_path_prefix,
                              vertex_info.GetPathPrefix(property_group));
    std::string base_dir = prefix_ + pg_path_prefix;
    GAR_ASSIGN_OR_RAISE_ERROR(chunk_num_,
                              utils::GetVertexChunkNum(prefix_, vertex_info));
  }

  /**
   * @brief Sets chunk position indicator for reader by internal vertex id.
   *    If internal vertex id is not found, will return Status::IndexError
   * error. After seeking to an invalid vertex id, the next call to GetChunk
   * function may undefined, e.g. return an non exist path.
   *
   * @param id the vertex id.
   */
  inline Status seek(IdType id) noexcept {
    seek_id_ = id;
    IdType pre_chunk_index = chunk_index_;
    chunk_index_ = id / vertex_info_.GetChunkSize();
    if (chunk_index_ != pre_chunk_index) {
      // TODO(@acezen): use a cache to avoid reloading the same chunk, could use
      //  a LRU cache.
      chunk_table_.reset();
    }
    if (chunk_index_ >= chunk_num_) {
      return Status::IndexError("Internal vertex id ", id,
                                " is not out of range [0,",
                                chunk_num_ * vertex_info_.GetChunkSize(),
                                ") of vertex ", vertex_info_.GetLabel());
    }
    return Status::OK();
  }

  /**
   * @brief Return the current arrow chunk table of chunk position indicator.
   */
  Result<std::shared_ptr<arrow::Table>> GetChunk() noexcept;

  /**
   * @brief Get the vertex id range of current chunk.
   *
   * @return Result: std::pair<begin_id, end_id> or error.
   */
  Result<std::pair<IdType, IdType>> GetRange() noexcept;

  /**
   * @brief Sets chunk position indicator to next chunk.
   *
   *  if current chunk is the last chunk, will return Status::IndexError error.
   */
  Status next_chunk() noexcept {
    if (++chunk_index_ >= chunk_num_) {
      return Status::IndexError(
          "vertex chunk index ", chunk_index_, " is out-of-bounds for vertex ",
          vertex_info_.GetLabel(), " chunk num ", chunk_num_);
    }
    seek_id_ = chunk_index_ * vertex_info_.GetChunkSize();
    chunk_table_.reset();

    return Status::OK();
  }

  /**
   * @brief Get the chunk number of current vertex property group.
   */
  IdType GetChunkNum() const noexcept { return chunk_num_; }

 private:
  VertexInfo vertex_info_;
  PropertyGroup property_group_;
  std::string prefix_;
  IdType chunk_index_;
  IdType seek_id_;
  IdType chunk_num_;
  std::shared_ptr<arrow::Table> chunk_table_;
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
   * @param vertex_chunk_index The vertex chunk index, default is 0.
   */
  AdjListArrowChunkReader(const EdgeInfo& edge_info, AdjListType adj_list_type,
                          const std::string& prefix,
                          IdType vertex_chunk_index = 0)
      : edge_info_(edge_info),
        adj_list_type_(adj_list_type),
        prefix_(prefix),
        vertex_chunk_index_(vertex_chunk_index),
        chunk_index_(0),
        seek_offset_(0),
        chunk_table_(nullptr) {
    GAR_ASSIGN_OR_RAISE_ERROR(fs_, FileSystemFromUriOrPath(prefix, &prefix_));
    GAR_ASSIGN_OR_RAISE_ERROR(auto adj_list_path_prefix,
                              edge_info.GetAdjListPathPrefix(adj_list_type));
    base_dir_ = prefix_ + adj_list_path_prefix;
    GAR_ASSIGN_OR_RAISE_ERROR(
        vertex_chunk_num_,
        utils::GetVertexChunkNum(prefix_, edge_info_, adj_list_type_));
    GAR_ASSIGN_OR_RAISE_ERROR(
        chunk_num_, utils::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_,
                                           vertex_chunk_index_));
  }

  /**
   * @brief Copy constructor.
   */
  AdjListArrowChunkReader(const AdjListArrowChunkReader& other)
      : edge_info_(other.edge_info_),
        adj_list_type_(other.adj_list_type_),
        prefix_(other.prefix_),
        vertex_chunk_index_(other.vertex_chunk_index_),
        chunk_index_(other.chunk_index_),
        seek_offset_(other.seek_offset_),
        chunk_table_(nullptr),
        vertex_chunk_num_(other.vertex_chunk_num_),
        chunk_num_(other.chunk_num_),
        base_dir_(other.base_dir_),
        fs_(other.fs_) {}

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
  Status seek_dst(IdType offset) noexcept;

  /**
   * @brief Sets chunk position indicator for reader by edge index.
   *
   * @param offset edge index of the vertex chunk.
   *     Note: the offset is the edge index of the vertex chunk, not the edge
   * index of the whole graph.
   */
  Status seek(IdType offset) noexcept {
    seek_offset_ = offset;
    IdType pre_chunk_index = chunk_index_;
    chunk_index_ = offset / edge_info_.GetChunkSize();
    if (chunk_index_ != pre_chunk_index) {
      chunk_table_.reset();
    }
    if (chunk_index_ >= chunk_num_) {
      return Status::IndexError("The edge offset ", offset,
                                " is out of range [0,",
                                edge_info_.GetChunkSize() * chunk_num_,
                                "), edge label: ", edge_info_.GetEdgeLabel());
    }
    return Status::OK();
  }

  /**
   * @brief Return the current chunk of chunk position indicator as arrow::Table
   */
  Result<std::shared_ptr<arrow::Table>> GetChunk() noexcept;

  /**
   * @brief Get the number of rows of the current chunk table.
   */
  Result<IdType> GetRowNumOfChunk() noexcept;

  /**
   * @brief Sets chunk position indicator to next chunk.
   *
   * @return Status: ok or EndOfChunk error if the reader is at the end of
   *         current vertex chunk, or IndexError error if the reader is at the
   *         end of all vertex chunks.
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
          chunk_num_,
          utils::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_,
                                 vertex_chunk_index_));
    }
    seek_offset_ = chunk_index_ * edge_info_.GetChunkSize();
    chunk_table_.reset();
    return Status::OK();
  }

  /**
   * @brief Sets chunk position to the specific vertex chunk and edge chunk.
   *
   * @param vertex_chunk_index the vertex chunk index.
   * @param chunk_index the edge chunk index of vertex_chunk_index.
   * @return Status: ok or error
   */
  Status seek_chunk_index(IdType vertex_chunk_index, IdType chunk_index = 0) {
    if (vertex_chunk_index_ != vertex_chunk_index) {
      vertex_chunk_index_ = vertex_chunk_index;
      GAR_ASSIGN_OR_RAISE_ERROR(
          chunk_num_,
          utils::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_,
                                 vertex_chunk_index_));
      chunk_table_.reset();
    }
    if (chunk_index_ != chunk_index) {
      chunk_index_ = chunk_index;
      seek_offset_ = chunk_index * edge_info_.GetChunkSize();
      chunk_table_.reset();
    }
    return Status::OK();
  }

 private:
  EdgeInfo edge_info_;
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
  AdjListOffsetArrowChunkReader(const EdgeInfo& edge_info,
                                AdjListType adj_list_type,
                                const std::string& prefix)
      : edge_info_(edge_info),
        adj_list_type_(adj_list_type),
        prefix_(prefix),
        chunk_index_(0),
        seek_id_(0),
        chunk_table_(nullptr) {
    GAR_ASSIGN_OR_RAISE_ERROR(fs_, FileSystemFromUriOrPath(prefix, &prefix_));
    GAR_ASSIGN_OR_RAISE_ERROR(auto dir_path,
                              edge_info.GetOffsetPathPrefix(adj_list_type));
    base_dir_ = prefix_ + dir_path;
    if (adj_list_type == AdjListType::ordered_by_source ||
        adj_list_type == AdjListType::ordered_by_dest) {
      GAR_ASSIGN_OR_RAISE_ERROR(
          vertex_chunk_num_,
          utils::GetVertexChunkNum(prefix_, edge_info_, adj_list_type_));
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
   * @brief Sets chunk position indicator for reader by internal vertex id.
   *    If internal vertex id is not found, will return Status::IndexError
   * error. After seeking to an invalid vertex id, the next call to GetChunk
   * function may undefined, e.g. return an non exist path.
   *
   * @param id the internal vertex id.
   */
  Status seek(IdType id) noexcept {
    seek_id_ = id;
    IdType pre_chunk_index = chunk_index_;
    chunk_index_ = id / vertex_chunk_size_;
    if (chunk_index_ != pre_chunk_index) {
      chunk_table_.reset();
    }
    if (chunk_index_ >= vertex_chunk_num_) {
      return Status::IndexError(
          "Internal vertex id ", id, "is out of range [0,",
          vertex_chunk_num_ * vertex_chunk_size_, "), of edge ",
          edge_info_.GetEdgeLabel(), " of adj list type ",
          AdjListTypeToString(adj_list_type_), ".");
    }
    return Status::OK();
  }

  /**
   * @brief Get the current offset chunk as arrow::Array.
   */
  Result<std::shared_ptr<arrow::Array>> GetChunk() noexcept;

  /**
   * @brief Sets chunk position indicator to next chunk.
   *     if current chunk is the last chunk, will return Status::IndexError
   * error.
   */
  Status next_chunk() {
    if (++chunk_index_ >= vertex_chunk_num_) {
      return Status::IndexError("vertex chunk index ", chunk_index_,
                                " is out-of-bounds for vertex chunk num ",
                                vertex_chunk_num_, " of edge ",
                                edge_info_.GetEdgeLabel(), " of adj list type ",
                                AdjListTypeToString(adj_list_type_), ".");
    }
    seek_id_ = chunk_index_ * vertex_chunk_size_;
    chunk_table_.reset();

    return Status::OK();
  }

  /**
   * @brief Get current vertex chunk index.
   */
  IdType GetChunkIndex() noexcept { return chunk_index_; }

 private:
  EdgeInfo edge_info_;
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
   * @param property_group The property group that describes the property group.
   * @param adj_list_type The adj list type for the edges.
   * @param prefix The absolute prefix.
   * @param vertex_chunk_index The vertex chunk index, default is 0.
   */
  AdjListPropertyArrowChunkReader(const EdgeInfo& edge_info,
                                  const PropertyGroup& property_group,
                                  AdjListType adj_list_type,
                                  const std::string prefix,
                                  IdType vertex_chunk_index = 0)
      : edge_info_(edge_info),
        property_group_(property_group),
        adj_list_type_(adj_list_type),
        prefix_(prefix),
        vertex_chunk_index_(vertex_chunk_index),
        chunk_index_(0),
        seek_offset_(0),
        chunk_table_(nullptr) {
    GAR_ASSIGN_OR_RAISE_ERROR(fs_, FileSystemFromUriOrPath(prefix, &prefix_));
    GAR_ASSIGN_OR_RAISE_ERROR(
        auto pg_path_prefix,
        edge_info.GetPropertyGroupPathPrefix(property_group, adj_list_type));
    base_dir_ = prefix_ + pg_path_prefix;
    GAR_ASSIGN_OR_RAISE_ERROR(
        vertex_chunk_num_,
        utils::GetVertexChunkNum(prefix_, edge_info_, adj_list_type_));
    GAR_ASSIGN_OR_RAISE_ERROR(
        chunk_num_, utils::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_,
                                           vertex_chunk_index_));
  }

  /**
   * @brief Copy constructor.
   */
  AdjListPropertyArrowChunkReader(const AdjListPropertyArrowChunkReader& other)
      : edge_info_(other.edge_info_),
        property_group_(other.property_group_),
        adj_list_type_(other.adj_list_type_),
        prefix_(other.prefix_),
        vertex_chunk_index_(other.vertex_chunk_index_),
        chunk_index_(other.chunk_index_),
        seek_offset_(other.seek_offset_),
        chunk_table_(nullptr),
        vertex_chunk_num_(other.vertex_chunk_num_),
        chunk_num_(other.chunk_num_),
        base_dir_(other.base_dir_),
        fs_(other.fs_) {}

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
    IdType pre_chunk_index = chunk_index_;
    seek_offset_ = offset;
    chunk_index_ = offset / edge_info_.GetChunkSize();
    if (chunk_index_ != pre_chunk_index) {
      chunk_table_.reset();
    }
    if (chunk_index_ >= chunk_num_) {
      return Status::IndexError("The edge offset ", offset,
                                " is out of range [0,",
                                edge_info_.GetChunkSize() * chunk_num_,
                                "), edge label: ", edge_info_.GetEdgeLabel());
    }
    return Status::OK();
  }

  /**
   * @brief Return the current chunk of chunk position indicator as arrow::Table
   */
  Result<std::shared_ptr<arrow::Table>> GetChunk() noexcept;

  /**
   * @brief Sets chunk position indicator to next chunk.
   *
   * @return Status: ok or EndOfChunk error if the reader is at the end of
   *         current vertex chunk, or IndexError error if the reader is at the
   *         end of all vertex chunks.
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
          chunk_num_,
          utils::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_,
                                 vertex_chunk_index_));
    }
    seek_offset_ = chunk_index_ * edge_info_.GetChunkSize();
    chunk_table_.reset();
    return Status::OK();
  }

  /**
   * @brief Sets chunk position to the specific vertex chunk and edge chunk.
   *
   * @param vertex_chunk_index the vertex chunk index.
   * @param chunk_index the edge chunk index of vertex_chunk_index.
   * @return Status: ok or error
   */
  Status seek_chunk_index(IdType vertex_chunk_index, IdType chunk_index = 0) {
    if (vertex_chunk_index_ != vertex_chunk_index) {
      vertex_chunk_index_ = vertex_chunk_index;
      GAR_ASSIGN_OR_RAISE_ERROR(
          chunk_num_,
          utils::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_,
                                 vertex_chunk_index_));
      chunk_table_.reset();
    }
    if (chunk_index_ != chunk_index) {
      chunk_index_ = chunk_index;
      seek_offset_ = chunk_index * edge_info_.GetChunkSize();
      chunk_table_.reset();
    }
    return Status::OK();
  }

 private:
  EdgeInfo edge_info_;
  PropertyGroup property_group_;
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
 * @brief Helper function to Construct VertexPropertyArrowChunkReader.
 *
 * @param graph_info The graph info to describe the graph.
 * @param label label of the vertex.
 * @param property_group The property group of the vertex.
 */
static inline Result<VertexPropertyArrowChunkReader>
ConstructVertexPropertyArrowChunkReader(
    const GraphInfo& graph_info, const std::string& label,
    const PropertyGroup& property_group) noexcept {
  VertexInfo vertex_info;
  GAR_ASSIGN_OR_RAISE(vertex_info, graph_info.GetVertexInfo(label));
  if (!vertex_info.ContainPropertyGroup(property_group)) {
    return Status::KeyError("No property group ", property_group, " in vertex ",
                            label, ".");
  }
  return VertexPropertyArrowChunkReader(vertex_info, property_group,
                                        graph_info.GetPrefix());
}

/**
 * @brief Helper function to Construct AdjListArrowChunkReader.
 *
 * @param graph_info The graph info to describe the graph.
 * @param src_label label of source vertex.
 * @param edge_label label of edge.
 * @param dst_label label of destination vertex.
 * @param adj_list_type The adj list type for the edges.
 */
static inline Result<AdjListArrowChunkReader> ConstructAdjListArrowChunkReader(
    const GraphInfo& graph_info, const std::string& src_label,
    const std::string& edge_label, const std::string& dst_label,
    AdjListType adj_list_type) noexcept {
  EdgeInfo edge_info;
  GAR_ASSIGN_OR_RAISE(edge_info,
                      graph_info.GetEdgeInfo(src_label, edge_label, dst_label));

  if (!edge_info.ContainAdjList(adj_list_type)) {
    return Status::KeyError("The adjacent list type ",
                            AdjListTypeToString(adj_list_type),
                            " doesn't exist in edge ", edge_label, ".");
  }
  return AdjListArrowChunkReader(edge_info, adj_list_type,
                                 graph_info.GetPrefix());
}

/**
 * @brief Helper function to Construct AdjListOffsetArrowChunkReader.
 *
 * @param graph_info The graph info to describe the graph.
 * @param src_label label of source vertex.
 * @param edge_label label of edge.
 * @param dst_label label of destination vertex.
 * @param adj_list_type The adj list type for the edges.
 */
static inline Result<AdjListOffsetArrowChunkReader>
ConstructAdjListOffsetArrowChunkReader(const GraphInfo& graph_info,
                                       const std::string& src_label,
                                       const std::string& edge_label,
                                       const std::string& dst_label,
                                       AdjListType adj_list_type) noexcept {
  EdgeInfo edge_info;
  GAR_ASSIGN_OR_RAISE(edge_info,
                      graph_info.GetEdgeInfo(src_label, edge_label, dst_label));

  if (!edge_info.ContainAdjList(adj_list_type)) {
    return Status::KeyError("The adjacent list type ",
                            AdjListTypeToString(adj_list_type),
                            " doesn't exist in edge ", edge_label, ".");
  }
  return AdjListOffsetArrowChunkReader(edge_info, adj_list_type,
                                       graph_info.GetPrefix());
}

/**
 * @brief Helper function to Construct AdjListPropertyArrowChunkReader.
 *
 * @param graph_info The graph info to describe the graph.
 * @param src_label label of source vertex.
 * @param edge_label label of edge.
 * @param dst_label label of destination vertex.
 * @param property_group The property group of the edge.
 * @param adj_list_type The adj list type for the edges.
 */
static inline Result<AdjListPropertyArrowChunkReader>
ConstructAdjListPropertyArrowChunkReader(const GraphInfo& graph_info,
                                         const std::string& src_label,
                                         const std::string& edge_label,
                                         const std::string& dst_label,
                                         const PropertyGroup& property_group,
                                         AdjListType adj_list_type) noexcept {
  EdgeInfo edge_info;
  GAR_ASSIGN_OR_RAISE(edge_info,
                      graph_info.GetEdgeInfo(src_label, edge_label, dst_label));
  if (!edge_info.ContainAdjList(adj_list_type)) {
    return Status::KeyError("The adjacent list type ",
                            AdjListTypeToString(adj_list_type),
                            " doesn't exist in edge ", edge_label, ".");
  }
  if (!edge_info.ContainPropertyGroup(property_group, adj_list_type)) {
    return Status::KeyError("No property group ", property_group, " in edge ",
                            edge_label, " with adj list type ",
                            AdjListTypeToString(adj_list_type), ".");
  }
  return AdjListPropertyArrowChunkReader(edge_info, property_group,
                                         adj_list_type, graph_info.GetPrefix());
}

}  // namespace GAR_NAMESPACE_INTERNAL
#endif  // GAR_READER_ARROW_CHUNK_READER_H_
