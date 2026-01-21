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
#include <vector>

#include "graphar/fwd.h"
#include "graphar/graph_info.h"
#include "graphar/types.h"
#include "rust/cxx.h"

namespace graphar {
using SharedPropertyGroup = std::shared_ptr<PropertyGroup>;
}

namespace graphar_rs {
rust::String to_type_name(const graphar::DataType &type);

std::unique_ptr<graphar::Property>
new_property(const std::string &name, std::shared_ptr<graphar::DataType> type,
             bool is_primary, bool is_nullable,
             graphar::Cardinality cardinality);
const std::string &property_get_name(const graphar::Property &prop);
const std::shared_ptr<graphar::DataType> &
property_get_type(const graphar::Property &prop);
bool property_is_primary(const graphar::Property &prop);
bool property_is_nullable(const graphar::Property &prop);
graphar::Cardinality property_get_cardinality(const graphar::Property &prop);
std::unique_ptr<graphar::Property>
property_clone(const graphar::Property &prop);

void property_vec_push_property(std::vector<graphar::Property> &properties,
                                std::unique_ptr<graphar::Property> prop);
void property_vec_emplace_property(std::vector<graphar::Property> &properties,
                                   const std::string &name,
                                   std::shared_ptr<graphar::DataType> type,
                                   bool is_primary, bool is_nullable,
                                   graphar::Cardinality cardinality);

void property_group_vec_push_property_group(
    std::vector<graphar::SharedPropertyGroup> &property_groups,
    std::shared_ptr<graphar::PropertyGroup> property_group);
} // namespace graphar_rs
