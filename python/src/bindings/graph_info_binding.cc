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

#include "pybind11/pybind11.h"
#include "pybind11/stl.h"

#include "graphar/graph_info.h"
#include "graphar/types.h"
#include "graphar/version_parser.h"

#define STRINGIFY(x) #x
#define MACRO_STRINGIFY(x) STRINGIFY(x)

namespace py = pybind11;

// Changed from PYBIND11_MODULE to a regular function
extern "C" void bind_graph_info(pybind11::module_& m) {
  // Minimal binding for DataType so pybind11 recognizes
  // std::shared_ptr<graphar::DataType> used in Property constructor defaults.
  py::class_<graphar::DataType, std::shared_ptr<graphar::DataType>>(m, "DataType")
      .def(py::init<>())
      .def(py::init<graphar::Type>())
      .def("id", &graphar::DataType::id)
      .def("to_type_name", &graphar::DataType::ToTypeName);

}  // namespace graphar