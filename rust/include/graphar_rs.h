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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "graphar/fwd.h"
#include "graphar/graph_info.h"
#include "graphar/types.h"
#include "graphar/version_parser.h"
#include "rust/cxx.h"

namespace graphar {
using SharedVertexInfo = std::shared_ptr<VertexInfo>;
using SharedEdgeInfo = std::shared_ptr<EdgeInfo>;
using SharedPropertyGroup = std::shared_ptr<PropertyGroup>;
using SharedAdjacentList = std::shared_ptr<AdjacentList>;
using ConstInfoVersion = const InfoVersion;
}  // namespace graphar

namespace graphar_rs {
rust::String to_type_name(const graphar::DataType& type);

std::shared_ptr<graphar::ConstInfoVersion> new_const_info_version(
    int32_t version);

std::unique_ptr<graphar::Property> new_property(
    const std::string& name, std::shared_ptr<graphar::DataType> type,
    bool is_primary, bool is_nullable, graphar::Cardinality cardinality);
const std::string& property_get_name(const graphar::Property& prop);
const std::shared_ptr<graphar::DataType>& property_get_type(
    const graphar::Property& prop);
bool property_is_primary(const graphar::Property& prop);
bool property_is_nullable(const graphar::Property& prop);
graphar::Cardinality property_get_cardinality(const graphar::Property& prop);
std::unique_ptr<graphar::Property> property_clone(
    const graphar::Property& prop);

void property_vec_push_property(std::vector<graphar::Property>& properties,
                                std::unique_ptr<graphar::Property> prop);
void property_vec_emplace_property(std::vector<graphar::Property>& properties,
                                   const std::string& name,
                                   std::shared_ptr<graphar::DataType> type,
                                   bool is_primary, bool is_nullable,
                                   graphar::Cardinality cardinality);

std::unique_ptr<std::vector<graphar::Property>> property_vec_clone(
    const std::vector<graphar::Property>& properties);

void property_group_vec_push_property_group(
    std::vector<graphar::SharedPropertyGroup>& property_groups,
    std::shared_ptr<graphar::PropertyGroup> property_group);

std::unique_ptr<std::vector<graphar::SharedPropertyGroup>>
property_group_vec_clone(
    const std::vector<graphar::SharedPropertyGroup>& property_groups);

std::shared_ptr<graphar::VertexInfo> create_vertex_info(
    const std::string& type, graphar::IdType chunk_size,
    const std::vector<graphar::SharedPropertyGroup>& property_groups,
    const rust::Vec<rust::String>& labels, const std::string& prefix,
    std::shared_ptr<graphar::ConstInfoVersion> version);

std::shared_ptr<graphar::EdgeInfo> create_edge_info(
    const std::string& src_type, const std::string& edge_type,
    const std::string& dst_type, graphar::IdType chunk_size,
    graphar::IdType src_chunk_size, graphar::IdType dst_chunk_size,
    bool directed, const graphar::AdjacentListVector& adjacent_lists,
    const std::vector<graphar::SharedPropertyGroup>& property_groups,
    const std::string& prefix,
    std::shared_ptr<graphar::ConstInfoVersion> version);

std::shared_ptr<graphar::GraphInfo> load_graph_info(const std::string& path);
std::shared_ptr<graphar::GraphInfo> create_graph_info(
    const std::string& name,
    const std::vector<graphar::SharedVertexInfo>& vertex_infos,
    const std::vector<graphar::SharedEdgeInfo>& edge_infos,
    const rust::Vec<rust::String>& labels, const std::string& prefix,
    std::shared_ptr<graphar::ConstInfoVersion> version);
bool graph_info_has_vertex_info_index(const graphar::GraphInfo& graph_info,
                                      const std::string& type);
size_t graph_info_get_vertex_info_index(const graphar::GraphInfo& graph_info,
                                        const std::string& type);
bool graph_info_has_edge_info_index(const graphar::GraphInfo& graph_info,
                                    const std::string& src_type,
                                    const std::string& edge_type,
                                    const std::string& dst_type);
size_t graph_info_get_edge_info_index(const graphar::GraphInfo& graph_info,
                                      const std::string& src_type,
                                      const std::string& edge_type,
                                      const std::string& dst_type);

void vertex_info_vec_push_vertex_info(
    std::vector<graphar::SharedVertexInfo>& vertex_infos,
    std::shared_ptr<graphar::VertexInfo> vertex_info);
void edge_info_vec_push_edge_info(
    std::vector<graphar::SharedEdgeInfo>& edge_infos,
    std::shared_ptr<graphar::EdgeInfo> edge_info);

void vertex_info_save(const graphar::VertexInfo& vertex_info,
                      const std::string& path);
std::unique_ptr<std::string> vertex_info_dump(
    const graphar::VertexInfo& vertex_info);

std::unique_ptr<graphar::AdjacentListVector> new_adjacent_list_vec();
void push_adjacent_list(graphar::AdjacentListVector& v,
                        std::shared_ptr<graphar::AdjacentList> adjacent_list);

void edge_info_save(const graphar::EdgeInfo& edge_info,
                    const std::string& path);
std::unique_ptr<std::string> edge_info_dump(const graphar::EdgeInfo& edge_info);
void graph_info_save(const graphar::GraphInfo& graph_info,
                     const std::string& path);
std::unique_ptr<std::string> graph_info_dump(
    const graphar::GraphInfo& graph_info);
}  // namespace graphar_rs
