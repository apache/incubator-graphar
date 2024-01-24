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

#ifndef GAR_FWD_H_
#define GAR_FWD_H_

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "gar/external/result.hpp"

#include "gar/util/macros.h"
#include "gar/util/status.h"

namespace GAR_NAMESPACE_INTERNAL {

class Status;

/**
 * @A class for representing either a usable value, or an error.
 *
 * A Result object either contains a value of type `T` or a Status object
 * explaining why such a value is not present. The type `T` must be
 * copy-constructible and/or move-constructible.
 *
 * The state of a Result object may be determined by calling has_error() or
 * status(). The has_error() method returns false if the object contains a
 * valid value. The status() method returns the internal Status object. A
 * Result object that contains a valid value will return an OK Status for a
 * call to status().
 *
 * A value of type `T` may be extracted from a Result object through a call
 * to value(). This function should only be called if a call to has_error()
 * returns false. Sample usage:
 *
 * ```
 *   gar::Result<Foo> result = CalculateFoo();
 *   if (!result.has_error()) {
 *     Foo foo = result.value();
 *     foo.DoSomethingCool();
 *   } else {
 *     std::err << result.status();
 *  }
 * ```
 */
template <typename T>
using Result = cpp::result<T, Status>;

struct GeneralParams;
class Yaml;
class FileSystem;

/** Type of vertex id or vertex index. */
using IdType = int64_t;
enum class Type;
class DataType;
/** Type of file format */
enum FileType { CSV = 0, PARQUET = 1, ORC = 2 };
enum class AdjListType : uint8_t;

template <typename T>
class Array;

class InfoVersion;

class Property;
class PropertyGroup;
class AdjacentList;
class Expression;

class VertexInfo;
class EdgeInfo;
class GraphInfo;

using PropertyGroupVector = std::vector<std::shared_ptr<PropertyGroup>>;
using AdjacentListVector = std::vector<std::shared_ptr<AdjacentList>>;
using VertexInfoVector = std::vector<std::shared_ptr<VertexInfo>>;
using EdgeInfoVector = std::vector<std::shared_ptr<EdgeInfo>>;

std::shared_ptr<PropertyGroup> CreatePropertyGroup(
    const std::vector<Property>& properties, FileType file_type,
    const std::string& prefix = "");

std::shared_ptr<AdjacentList> CreateAdjacentList(
    AdjListType type, FileType file_type, const std::string& prefix = "");

std::shared_ptr<VertexInfo> CreateVertexInfo(
    const std::string& label, IdType chunk_size,
    const PropertyGroupVector& property_groups, const std::string& prefix = "",
    std::shared_ptr<const InfoVersion> version = nullptr);

std::shared_ptr<EdgeInfo> CreateEdgeInfo(
    const std::string& src_label, const std::string& edge_label,
    const std::string& dst_label, IdType chunk_size, IdType src_chunk_size,
    IdType dst_chunk_size, bool directed,
    const AdjacentListVector& adjacent_lists,
    const PropertyGroupVector& property_groups, const std::string& prefix = "",
    std::shared_ptr<const InfoVersion> version = nullptr);

std::shared_ptr<GraphInfo> CreateGraphInfo(
    const std::string& name, const VertexInfoVector& vertex_infos,
    const EdgeInfoVector& edge_infos, const std::string& prefix,
    std::shared_ptr<const InfoVersion> version = nullptr);

const std::shared_ptr<DataType>& boolean();
const std::shared_ptr<DataType>& int32();
const std::shared_ptr<DataType>& int64();
const std::shared_ptr<DataType>& float32();
const std::shared_ptr<DataType>& float64();
const std::shared_ptr<DataType>& string();
std::shared_ptr<DataType> list(const std::shared_ptr<DataType>& value_type);

namespace util {
struct FilterOptions;
using Filter = std::shared_ptr<Expression>;
using ColumnNames =
    std::optional<std::reference_wrapper<std::vector<std::string>>>;
}  // namespace util

}  // namespace GAR_NAMESPACE_INTERNAL

#endif  // GAR_FWD_H_
