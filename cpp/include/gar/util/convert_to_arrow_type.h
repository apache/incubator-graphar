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

#ifndef GAR_UTIL_CONVERT_TO_ARROW_TYPE_H_
#define GAR_UTIL_CONVERT_TO_ARROW_TYPE_H_

#include <memory>
#include <string>

#include "arrow/api.h"
#include "arrow/type.h"

#include "gar/util/data_type.h"

namespace GAR_NAMESPACE_INTERNAL {

/** Struct to convert DataType to arrow::DataType. */
template <Type T>
struct ConvertToArrowType {};

#define CONVERT_TO_ARROW_TYPE(type, c_type, arrow_type, array_type,            \
                              builder_type, type_value, str)                   \
  template <>                                                                  \
  struct ConvertToArrowType<type> {                                            \
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

}  // namespace GAR_NAMESPACE_INTERNAL

#endif  // GAR_UTIL_CONVERT_TO_ARROW_TYPE_H_
