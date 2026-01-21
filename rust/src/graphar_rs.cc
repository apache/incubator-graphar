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

#include <utility>

namespace graphar_rs {
rust::String to_type_name(const graphar::DataType &type) {
  return rust::String(type.ToTypeName());
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

void property_group_vec_push_property_group(
    std::vector<graphar::SharedPropertyGroup> &property_groups,
    std::shared_ptr<graphar::PropertyGroup> property_group) {
  property_groups.emplace_back(std::move(property_group));
}
} // namespace graphar_rs
