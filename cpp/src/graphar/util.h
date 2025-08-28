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
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "graphar/result.h"
#include "graphar/status.h"

#include "arrow/api.h"
#include "arrow/csv/api.h"
#include "arrow/filesystem/api.h"
#include "arrow/io/api.h"
#include "arrow/stl.h"
#include "arrow/util/uri.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#define REGULAR_SEPARATOR "_"

// forward declarations
namespace arrow {
class Table;
class ChunkedArray;
class Array;
}  // namespace arrow

namespace graphar {

template <typename T>
class Array final {
 public:
  using ValueType = T;
  Array() : data_(nullptr), size_(0) {}
  Array(const T* data, size_t size) : data_(data), size_(size) {}
  Array(const Array& other) = default;
  Array(Array&& other) = default;
  Array& operator=(const Array& other) = default;
  Array& operator=(Array&& other) = default;
  ~Array() = default;

  const T& operator[](size_t index) const { return data_[index]; }

  const T* data() const { return data_; }

  size_t size() const { return size_; }

  void clear() {
    data_ = nullptr;
    size_ = 0;
  }

  bool empty() const { return size_ == 0; }

  void swap(Array& other) {
    std::swap(data_, other.data_);
    std::swap(size_, other.size_);
  }

  const T* begin() const { return data_; }

  const T* end() const { return data_ + size_; }

 private:
  const T* data_;
  size_t size_;
};

template <>
class Array<std::string_view> final {
 public:
  using ValueType = std::string_view;

  class iterator {
   private:
    const int32_t* offsets_;
    const uint8_t* data_;
    size_t index_;

   public:
    explicit iterator(const int32_t* offsets, const uint8_t* data, size_t index)
        : offsets_(offsets), data_(data), index_(index) {}

    const std::string_view operator*() const {
      return std::string_view(
          reinterpret_cast<const char*>(data_ + offsets_[index_]),
          offsets_[index_ + 1] - offsets_[index_]);
    }

    iterator& operator++() {
      ++index_;
      return *this;
    }

    iterator operator++(int) { return iterator(offsets_, data_, index_++); }

    iterator operator+(size_t n) {
      return iterator(offsets_, data_, index_ + n);
    }

    bool operator==(const iterator& other) const {
      return index_ == other.index_;
    }
    bool operator!=(const iterator& other) const {
      return index_ != other.index_;
    }
  };
  Array() : offsets_(nullptr), data_(nullptr), size_(0) {}
  explicit Array(const int32_t* offsets, const uint8_t* data, size_t size)
      : offsets_(offsets), data_(data), size_(size) {}

  const std::string_view operator[](size_t index) const {
    return std::string_view(
        reinterpret_cast<const char*>(data_ + offsets_[index]),
        offsets_[index + 1] - offsets_[index]);
  }

  const int32_t* offsets() const { return offsets_; }
  const uint8_t* data() const { return data_; }

  size_t size() const { return size_; }

  void clear() {
    offsets_ = nullptr;
    data_ = nullptr;
    size_ = 0;
  }

  bool empty() const { return size_ == 0; }

  void swap(Array& other) {
    std::swap(offsets_, other.offsets_);
    std::swap(data_, other.data_);
    std::swap(size_, other.size_);
  }

  const iterator begin() const { return iterator(offsets_, data_, 0); }
  const iterator end() const { return iterator(offsets_, data_, size_); }

 private:
  const int32_t* offsets_;
  const uint8_t* data_;
  size_t size_;
};

using Int32Array = Array<int32_t>;
using Int64Array = Array<int64_t>;
using FloatArray = Array<float>;
using DoubleArray = Array<double>;
using StringArray = Array<std::string_view>;

}  // namespace graphar

namespace graphar::util {

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

static inline arrow::Status OpenParquetArrowReader(
    const std::string& file_path, arrow::MemoryPool* pool,
    std::unique_ptr<parquet::arrow::FileReader>* parquet_reader) {
  std::shared_ptr<arrow::io::RandomAccessFile> input;
  ARROW_ASSIGN_OR_RAISE(input, arrow::io::ReadableFile::Open(file_path));
#if defined(ARROW_VERSION) && ARROW_VERSION <= 20000000
  ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(input, pool, parquet_reader));
#else
  ARROW_ASSIGN_OR_RAISE(auto reader, parquet::arrow::OpenFile(input, pool));
  *parquet_reader = std::move(reader);
#endif
  return arrow::Status::OK();
}

}  // namespace graphar::util
