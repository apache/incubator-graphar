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

#include <pybind11/pybind11.h>

// Declare binding functions
extern "C" void bind_types(pybind11::module_& m);
extern "C" void bind_graph_info(pybind11::module_& m);
extern "C" void bind_high_level_api(pybind11::module_& m);
extern "C" void bind_cli(pybind11::module_& m);

PYBIND11_MODULE(_core, m) {
  m.doc() = "GraphAr core Python bindings";

  bind_types(m);
  bind_graph_info(m);
  bind_high_level_api(m);
  bind_cli(m);
}