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

#include "graphar/writer_util.h"
#include "pybind11/pybind11.h"
#include "pybind11/stl.h"

#include "graphar/types.h"

#define STRINGIFY(x) #x
#define MACRO_STRINGIFY(x) STRINGIFY(x)

namespace py = pybind11;

// Changed from PYBIND11_MODULE to a regular function
extern "C" void bind_types(pybind11::module_& m) {
  // Bind Type enum
  py::enum_<graphar::Type>(m, "Type")
      .value("BOOL", graphar::Type::BOOL)
      .value("INT32", graphar::Type::INT32)
      .value("INT64", graphar::Type::INT64)
      .value("FLOAT", graphar::Type::FLOAT)
      .value("DOUBLE", graphar::Type::DOUBLE)
      .value("STRING", graphar::Type::STRING)
      .value("LIST", graphar::Type::LIST)
      .value("DATE", graphar::Type::DATE)
      .value("TIMESTAMP", graphar::Type::TIMESTAMP)
      .value("USER_DEFINED", graphar::Type::USER_DEFINED)
      .export_values();

  // Bind FileType enum
  py::enum_<graphar::FileType>(m, "FileType")
      .value("CSV", graphar::FileType::CSV)
      .value("PARQUET", graphar::FileType::PARQUET)
      .value("ORC", graphar::FileType::ORC)
      .value("JSON", graphar::FileType::JSON)
      .export_values();

  // Bind AdjListType enum
  py::enum_<graphar::AdjListType>(m, "AdjListType")
      .value("unordered_by_source", graphar::AdjListType::unordered_by_source)
      .value("unordered_by_dest", graphar::AdjListType::unordered_by_dest)
      .value("ordered_by_source", graphar::AdjListType::ordered_by_source)
      .value("ordered_by_dest", graphar::AdjListType::ordered_by_dest)
      .export_values();

  // Bind Cardinality enum
  py::enum_<graphar::Cardinality>(m, "Cardinality")
      .value("SINGLE", graphar::Cardinality::SINGLE)
      .value("LIST", graphar::Cardinality::LIST)
      .value("SET", graphar::Cardinality::SET)
      .export_values();
      
    // Bind ValidateLevel enum
  py::enum_<graphar::ValidateLevel>(m, "ValidateLevel")
      .value("default_validate", graphar::ValidateLevel::default_validate)
      .value("no_validate", graphar::ValidateLevel::no_validate)
      .value("weak_validate", graphar::ValidateLevel::weak_validate)
      .value("strong_validate", graphar::ValidateLevel::strong_validate)
      .export_values();
    
}  // namespace graphar