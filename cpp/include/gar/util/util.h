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

#ifndef GAR_UTIL_UTIL_H_
#define GAR_UTIL_UTIL_H_

#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "gar/util/result.h"

#define REGULAR_SEPARATOR "_"

// forward declarations
namespace arrow {
class Table;
class ChunkedArray;
class Array;
}  // namespace arrow

namespace GAR_NAMESPACE_INTERNAL {

namespace util {

struct IndexConverter {
  explicit IndexConverter(std::vector<IdType>&& edge_chunk_nums)
      : edge_chunk_nums_(std::move(edge_chunk_nums)) {}
  IdType IndexPairToGlobalChunkIndex(IdType vertex_chunk_index,
                                     IdType edge_chunk_index) {
    IdType global_edge_chunk_index = 0;
    for (IdType i = 0; i < vertex_chunk_index; ++i) {
      global_edge_chunk_index += edge_chunk_nums_[i];
    }
    return global_edge_chunk_index + edge_chunk_index;
  }

  // covert edge global chunk index to <vertex_chunk_index, edge_chunk_index>
  std::pair<IdType, IdType> GlobalChunkIndexToIndexPair(IdType global_index) {
    std::pair<IdType, IdType> index_pair(0, 0);
    for (size_t i = 0; i < edge_chunk_nums_.size(); ++i) {
      if (global_index < edge_chunk_nums_[i]) {
        index_pair.first = static_cast<IdType>(i);
        index_pair.second = global_index;
        break;
      }
      global_index -= edge_chunk_nums_[i];
    }
    return index_pair;
  }

 private:
  std::vector<IdType> edge_chunk_nums_;
};

static inline IdType IndexPairToGlobalChunkIndex(
    const std::vector<IdType>& edge_chunk_nums, IdType vertex_chunk_index,
    IdType edge_chunk_index) {
  IdType global_edge_chunk_index = 0;
  for (IdType i = 0; i < vertex_chunk_index; ++i) {
    global_edge_chunk_index += edge_chunk_nums[i];
  }
  return global_edge_chunk_index + edge_chunk_index;
}

// covert edge global chunk index to <vertex_chunk_index, edge_chunk_index>
static inline std::pair<IdType, IdType> GlobalChunkIndexToIndexPair(
    const std::vector<IdType>& edge_chunk_nums, IdType global_index) {
  std::pair<IdType, IdType> index_pair(0, 0);
  for (size_t i = 0; i < edge_chunk_nums.size(); ++i) {
    if (global_index < edge_chunk_nums[i]) {
      index_pair.first = static_cast<IdType>(i);
      index_pair.second = global_index;
      break;
    }
    global_index -= edge_chunk_nums[i];
  }
  return index_pair;
}

std::shared_ptr<arrow::ChunkedArray> GetArrowColumnByName(
    std::shared_ptr<arrow::Table> const& table, const std::string& name);

std::shared_ptr<arrow::Array> GetArrowArrayByChunkIndex(
    std::shared_ptr<arrow::ChunkedArray> const& chunk_array,
    int64_t chunk_index);

Result<const void*> GetArrowArrayData(
    std::shared_ptr<arrow::Array> const& array);

static inline std::string ConcatStringWithDelimiter(
    const std::vector<std::string>& str_vec, const std::string& delimiter) {
  return std::accumulate(
      std::begin(str_vec), std::end(str_vec), std::string(),
      [&delimiter](const std::string& ss, const std::string& s) {
        return ss.empty() ? s : ss + delimiter + s;
      });
}

template <typename T>
struct ValueGetter {
  inline static T Value(const void* data, int64_t offset) {
    return reinterpret_cast<const T*>(data)[offset];
  }
};

template <>
struct ValueGetter<std::string> {
  static std::string Value(const void* data, int64_t offset);
};

}  // namespace util

template <typename T>
class Array {
 public:
  using value_type = T;
  Array() : data_(nullptr), size_(0) {}
  Array(T* data, size_t size) : data_(data), size_(size) {}
  Array(const Array& other) = default;
  Array(Array&& other) = default;
  Array& operator=(const Array& other) = default;
  Array& operator=(Array&& other) = default;
  ~Array() = default;

  const T& operator[](size_t index) const { return data_[index]; }

  const T* data() const { return data_; }

  size_t size() const { return size_; }

  void resize(size_t size) { size_ = size; }

  void clear() {
    data_ = nullptr;
    size_ = 0;
  }

  bool empty() const { return size_ == 0; }

  void swap(Array& other) {
    std::swap(data_, other.data_);
    std::swap(size_, other.size_);
  }

  const T* begin() { return data_; }

  const T* end() { return data_ + size_; }

 private:
  const T* data_;
  size_t size_;
};

}  // namespace GAR_NAMESPACE_INTERNAL
#endif  // GAR_UTIL_UTIL_H_
