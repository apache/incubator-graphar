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
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "result/result.hpp"

#include "graphar/macros.h"
#include "graphar/status.h"

namespace graphar {

class Status;

/**
 * @brief A class for representing either a usable value, or an error.
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
 *   graphar::Result<Foo> result = CalculateFoo();
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
class DataType;
/** Defines how multiple values are handled for a given property key */
enum Cardinality : int32_t { SINGLE, LIST, SET };
/** Type of file format */
enum FileType : int32_t { CSV = 0, PARQUET = 1, ORC = 2, JSON = 3 };
enum SelectType : int32_t { PROPERTIES = 0, LABELS = 1 };
/** GetChunkVersion: V1 use scanner, V2 use FileReader */
enum GetChunkVersion : int32_t { AUTO = 0, V1 = 1, V2 = 2 };
enum class AdjListType : int32_t;

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

/**
 * @brief Create a PropertyGroup instance
 *
 * @param properties Property list of the group
 * @param file_type File type of property group chunk file
 * @param prefix prefix of property group chunk file. The default
 *        prefix is the concatenation of property names with '_' as separator
 * @return property_group shared_ptr to PropertyGroup
 */
std::shared_ptr<PropertyGroup> CreatePropertyGroup(
    const std::vector<Property>& properties, FileType file_type,
    const std::string& prefix = "");

/**
 * @brief Create a AdjacentList instance
 *
 * @param type Type of adjacent list
 * @param file_type File type of adjacent list chunk file
 * @param prefix prefix of adjacent list chunk file, If left empty, the default
 *        prefix will be set to the name of adjacent list type.
 * @return adjacent_list shared_ptr to AdjacentList
 */
std::shared_ptr<AdjacentList> CreateAdjacentList(
    AdjListType type, FileType file_type, const std::string& prefix = "");

/**
 * @brief Create a VertexInfo instance
 *
 * @param type The type of the vertex
 * @param chunk_size The number of vertices in each vertex chunk
 * @param property_groups The property group vector of the vertex
 * @param labels The labels of the vertex.
 * @param prefix The prefix of the vertex info. If left empty, the default
 *        prefix will be set to the type of the vertex
 * @param version The format version of the vertex info
 * @return vertex_info shared_ptr to VertexInfo
 */
std::shared_ptr<VertexInfo> CreateVertexInfo(
    const std::string& type, IdType chunk_size,
    const PropertyGroupVector& property_groups,
    const std::vector<std::string>& labels = {}, const std::string& prefix = "",
    std::shared_ptr<const InfoVersion> version = nullptr);

/**
 * @brief Create an EdgeInfo instance
 *
 * @param src_type The type of the source vertex.
 * @param edge_type The type of the edge
 * @param dst_type The type of the destination vertex
 * @param chunk_size The number of edges in each edge chunk
 * @param src_chunk_size The number of source vertices in each vertex chunk
 * @param dst_chunk_size The number of destination vertices in each vertex
 * chunk
 * @param directed Whether the edge is directed
 * @param adjacent_lists The adjacency list vector of the edge
 * @param property_groups The property group vector of the edge
 * @param prefix The path prefix of the edge info
 * @param version The format version of the edge info
 * @return edge_info shared_ptr to EdgeInfo
 */
std::shared_ptr<EdgeInfo> CreateEdgeInfo(
    const std::string& src_type, const std::string& edge_type,
    const std::string& dst_type, IdType chunk_size, IdType src_chunk_size,
    IdType dst_chunk_size, bool directed,
    const AdjacentListVector& adjacent_lists,
    const PropertyGroupVector& property_groups, const std::string& prefix = "",
    std::shared_ptr<const InfoVersion> version = nullptr);

/**
 * @brief Create a GraphInfo instance
 *
 * @param name The name of the graph
 * @param vertex_infos The vertex info vector of the graph
 * @param edge_infos The edge info vector of the graph
 * @param labels The vertex labels of the graph.
 * @param prefix The absolute path prefix to store chunk files of the graph.
 *               Defaults to "./"
 * @param version The version of the graph info
 * @param extra_info The extra metadata of the graph info
 * @return graph_info shared_ptr to GraphInfo
 */
std::shared_ptr<GraphInfo> CreateGraphInfo(
    const std::string& name, const VertexInfoVector& vertex_infos,
    const EdgeInfoVector& edge_infos, const std::vector<std::string>& labels,
    const std::string& prefix,
    std::shared_ptr<const InfoVersion> version = nullptr,
    const std::unordered_map<std::string, std::string>& extra_info = {});

/// @brief Return a boolean DataType instance
const std::shared_ptr<DataType>& boolean();
/// @brief Return a int32 DataType instance
const std::shared_ptr<DataType>& int32();
/// @brief Return a int64 DataType instance
const std::shared_ptr<DataType>& int64();
/// @brief Return a float32 DataType instance
const std::shared_ptr<DataType>& float32();
/// @brief Return a float64 DataType instance
const std::shared_ptr<DataType>& float64();
/// @brief Return a string DataType instance
const std::shared_ptr<DataType>& string();
/// @brief Return a date DataType instance
const std::shared_ptr<DataType>& date();
/// @brief Return a timestamp DataType instance
const std::shared_ptr<DataType>& timestamp();
/**
 * @brief Return a list DataType instance
 *
 * @param value_type value type of the list
 */
std::shared_ptr<DataType> list(const std::shared_ptr<DataType>& value_type);
}  // namespace graphar

namespace graphar::util {
struct FilterOptions;
using Filter = std::shared_ptr<Expression>;
using ColumnNames =
    std::optional<std::reference_wrapper<std::vector<std::string>>>;
}  // namespace graphar::util
