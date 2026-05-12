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

#include <memory>
#include <string>

#include "arrow/api.h"

#include "graphar/util.h"

namespace graphar::util {

std::shared_ptr<arrow::ChunkedArray> GetArrowColumnByName(
    std::shared_ptr<arrow::Table> const& table, const std::string& name) {
  return table->GetColumnByName(name);
}

std::shared_ptr<arrow::Array> GetArrowArrayByChunkIndex(
    std::shared_ptr<arrow::ChunkedArray> const& chunk_array,
    int64_t chunk_index) {
  return chunk_array->chunk(chunk_index);
}

Result<const void*> GetArrowArrayData(
    std::shared_ptr<arrow::Array> const& array) {
  if (array->type()->Equals(arrow::int8())) {
    return reinterpret_cast<const void*>(
        std::dynamic_pointer_cast<arrow::Int8Array>(array)->raw_values());
  } else if (array->type()->Equals(arrow::uint8())) {
    return reinterpret_cast<const void*>(
        std::dynamic_pointer_cast<arrow::UInt8Array>(array)->raw_values());
  } else if (array->type()->Equals(arrow::int16())) {
    return reinterpret_cast<const void*>(
        std::dynamic_pointer_cast<arrow::Int16Array>(array)->raw_values());
  } else if (array->type()->Equals(arrow::uint16())) {
    return reinterpret_cast<const void*>(
        std::dynamic_pointer_cast<arrow::UInt16Array>(array)->raw_values());
  } else if (array->type()->Equals(arrow::int32())) {
    return reinterpret_cast<const void*>(
        std::dynamic_pointer_cast<arrow::Int32Array>(array)->raw_values());
  } else if (array->type()->Equals(arrow::uint32())) {
    return reinterpret_cast<const void*>(
        std::dynamic_pointer_cast<arrow::UInt32Array>(array)->raw_values());
  } else if (array->type()->Equals(arrow::int64())) {
    return reinterpret_cast<const void*>(
        std::dynamic_pointer_cast<arrow::Int64Array>(array)->raw_values());
  } else if (array->type()->Equals(arrow::uint64())) {
    return reinterpret_cast<const void*>(
        std::dynamic_pointer_cast<arrow::UInt64Array>(array)->raw_values());
  } else if (array->type()->Equals(arrow::float32())) {
    return reinterpret_cast<const void*>(
        std::dynamic_pointer_cast<arrow::FloatArray>(array)->raw_values());
  } else if (array->type()->Equals(arrow::float64())) {
    return reinterpret_cast<const void*>(
        std::dynamic_pointer_cast<arrow::DoubleArray>(array)->raw_values());
  } else if (array->type()->Equals(arrow::utf8())) {
    return reinterpret_cast<const void*>(
        std::dynamic_pointer_cast<arrow::StringArray>(array).get());
  } else if (array->type()->Equals(arrow::large_utf8())) {
    return reinterpret_cast<const void*>(
        std::dynamic_pointer_cast<arrow::LargeStringArray>(array).get());
  } else if (array->type()->Equals(arrow::list(arrow::int32())) ||
             array->type()->Equals(arrow::large_list(arrow::uint32())) ||
             array->type()->Equals(arrow::large_list(arrow::int64())) ||
             array->type()->Equals(arrow::large_list(arrow::uint64())) ||
             array->type()->Equals(arrow::large_list(arrow::float32())) ||
             array->type()->Equals(arrow::large_list(arrow::float64()))) {
    return reinterpret_cast<const void*>(
        std::dynamic_pointer_cast<arrow::LargeListArray>(array).get());
  } else if (array->type()->Equals(arrow::null())) {
    return reinterpret_cast<const void*>(
        std::dynamic_pointer_cast<arrow::NullArray>(array).get());
  } else if (array->type()->Equals(arrow::boolean())) {
    return reinterpret_cast<const void*>(
        std::dynamic_pointer_cast<arrow::BooleanArray>(array).get());
  } else if (array->type()->Equals(arrow::date32())) {
    return reinterpret_cast<const void*>(
        std::dynamic_pointer_cast<arrow::Date32Array>(array)->raw_values());
  } else if (array->type()->Equals(arrow::timestamp(arrow::TimeUnit::MILLI))) {
    return reinterpret_cast<const void*>(
        std::dynamic_pointer_cast<arrow::TimestampArray>(array)->raw_values());
  } else {
    return Status::TypeError("Array type - ", array->type()->ToString(),
                             " is not supported yet...");
  }
}

std::string ValueGetter<std::string>::Value(const void* data, int64_t offset) {
  return std::string(
      reinterpret_cast<const arrow::LargeStringArray*>(data)->GetView(offset));
}

}  // namespace graphar::util
