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

#include "arrow/api.h"
#include "arrow/type.h"

#include "graphar/types.h"

namespace graphar {

/** Struct to convert DataType to arrow::DataType. */
template <typename T>
struct CTypeToArrowType {};

template <Type T>
struct TypeToArrowType {};

#define CONVERT_TO_ARROW_TYPE(type, c_type, arrow_type, array_type,            \
                              builder_type, type_value, str)                   \
  template <>                                                                  \
  struct TypeToArrowType<type> {                                               \
    using CType = c_type;                                                      \
    using ArrowType = arrow_type;                                              \
    using ArrayType = array_type;                                              \
    using BuilderType = builder_type;                                          \
    static std::shared_ptr<arrow::DataType> TypeValue() { return type_value; } \
    static const char* type_to_string() { return str; }                        \
  };                                                                           \
  template <>                                                                  \
  struct CTypeToArrowType<c_type> {                                            \
    using CType = c_type;                                                      \
    using ArrowType = arrow_type;                                              \
    using ArrayType = array_type;                                              \
    using BuilderType = builder_type;                                          \
    static std::shared_ptr<arrow::DataType> TypeValue() { return type_value; } \
    static const char* type_to_string() { return str; }                        \
  };

CONVERT_TO_ARROW_TYPE(Type::BOOL, bool, arrow::BooleanType, arrow::BooleanArray,
                      arrow::BooleanBuilder, arrow::boolean(), "boolean")
CONVERT_TO_ARROW_TYPE(Type::INT32, int32_t, arrow::Int32Type, arrow::Int32Array,
                      arrow::Int32Builder, arrow::int32(), "int32")
CONVERT_TO_ARROW_TYPE(Type::INT64, int64_t, arrow::Int64Type, arrow::Int64Array,
                      arrow::Int64Builder, arrow::int64(), "int64")
CONVERT_TO_ARROW_TYPE(Type::FLOAT, float, arrow::FloatType, arrow::FloatArray,
                      arrow::FloatBuilder, arrow::float32(), "float")
CONVERT_TO_ARROW_TYPE(Type::DOUBLE, double, arrow::DoubleType,
                      arrow::DoubleArray, arrow::DoubleBuilder,
                      arrow::float64(), "double")
CONVERT_TO_ARROW_TYPE(Type::STRING, std::string, arrow::LargeStringType,
                      arrow::LargeStringArray, arrow::LargeStringBuilder,
                      arrow::large_utf8(), "string")
CONVERT_TO_ARROW_TYPE(Type::TIMESTAMP, Timestamp, arrow::TimestampType,
                      arrow::TimestampArray, arrow::TimestampBuilder,
                      arrow::timestamp(arrow::TimeUnit::MILLI), "timestamp")
CONVERT_TO_ARROW_TYPE(Type::DATE, Date, arrow::Date32Type, arrow::Date32Array,
                      arrow::Date32Builder, arrow::date32(), "date")

}  // namespace graphar
