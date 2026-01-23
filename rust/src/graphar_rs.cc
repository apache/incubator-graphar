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

#include "graphar_rs.h"

#include <stdexcept>
#include <utility>

namespace graphar_rs {
rust::String to_type_name(const graphar::DataType &type) {
  return rust::String(type.ToTypeName());
}

std::shared_ptr<graphar::ConstInfoVersion>
new_const_info_version(int32_t version) {
  try {
    return std::make_shared<graphar::InfoVersion>(static_cast<int>(version));
  } catch (const std::exception &e) {
    throw std::runtime_error(e.what());
  }
}

std::unique_ptr<graphar::Property>
new_property(const std::string &name, std::shared_ptr<graphar::DataType> type,
             bool is_primary, bool is_nullable,
             graphar::Cardinality cardinality) {
  return std::make_unique<graphar::Property>(name, type, is_primary,
                                             is_nullable, cardinality);
}
const std::string &property_get_name(const graphar::Property &prop) {
  return prop.name;
}
const std::shared_ptr<graphar::DataType> &
property_get_type(const graphar::Property &prop) {
  return prop.type;
}
bool property_is_primary(const graphar::Property &prop) {
  return prop.is_primary;
}
bool property_is_nullable(const graphar::Property &prop) {
  return prop.is_nullable;
}
graphar::Cardinality property_get_cardinality(const graphar::Property &prop) {
  return prop.cardinality;
}
std::unique_ptr<graphar::Property>
property_clone(const graphar::Property &prop) {
  return std::make_unique<graphar::Property>(prop);
}

void property_vec_push_property(std::vector<graphar::Property> &properties,
                                std::unique_ptr<graphar::Property> prop) {
  properties.emplace_back(*prop);
}

void property_vec_emplace_property(std::vector<graphar::Property> &properties,
                                   const std::string &name,
                                   std::shared_ptr<graphar::DataType> type,
                                   bool is_primary, bool is_nullable,
                                   graphar::Cardinality cardinality) {
  properties.emplace_back(name, type, is_primary, is_nullable, cardinality);
}

std::unique_ptr<std::vector<graphar::Property>>
property_vec_clone(const std::vector<graphar::Property> &properties) {
  return std::make_unique<std::vector<graphar::Property>>(properties);
}

void property_group_vec_push_property_group(
    std::vector<graphar::SharedPropertyGroup> &property_groups,
    std::shared_ptr<graphar::PropertyGroup> property_group) {
  property_groups.emplace_back(std::move(property_group));
}

std::unique_ptr<std::vector<graphar::SharedPropertyGroup>>
property_group_vec_clone(
    const std::vector<graphar::SharedPropertyGroup> &property_groups) {
  return std::make_unique<std::vector<graphar::SharedPropertyGroup>>(
      property_groups);
}

std::shared_ptr<graphar::VertexInfo> create_vertex_info(
    const std::string &type, graphar::IdType chunk_size,
    const std::vector<graphar::SharedPropertyGroup> &property_groups,
    const rust::Vec<rust::String> &labels, const std::string &prefix,
    std::shared_ptr<graphar::ConstInfoVersion> version) {
  if (type.empty()) {
    throw std::runtime_error("CreateVertexInfo: type must not be empty");
  }
  if (chunk_size <= 0) {
    throw std::runtime_error("CreateVertexInfo: chunk_size must be > 0");
  }

  std::vector<std::string> label_vec;
  label_vec.reserve(labels.size());
  for (size_t i = 0; i < labels.size(); ++i) {
    label_vec.emplace_back(std::string(labels[i]));
  }

  auto vertex_info = graphar::CreateVertexInfo(type, chunk_size, property_groups,
                                               label_vec, prefix, std::move(version));
  if (vertex_info == nullptr) {
    throw std::runtime_error("CreateVertexInfo: returned nullptr");
  }
  return vertex_info;
}

void vertex_info_save(const graphar::VertexInfo &vertex_info,
                      const std::string &path) {
  auto status = vertex_info.Save(path);
  if (!status.ok()) {
    throw std::runtime_error(status.message());
  }
}

std::unique_ptr<std::string>
vertex_info_dump(const graphar::VertexInfo &vertex_info) {
  auto dumped = vertex_info.Dump();
  if (!dumped) {
    throw std::runtime_error(dumped.error().message());
  }
  return std::make_unique<std::string>(std::move(dumped).value());
}
} // namespace graphar_rs
