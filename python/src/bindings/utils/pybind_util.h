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

#include <pybind11/pybind11.h>
#include "graphar/fwd.h"

// Helper function to convert Status to Python exception
inline void CheckStatus(const graphar::Status& status) {
  if (!status.ok()) {
    PyErr_SetString(PyExc_ValueError, status.message().c_str());
    throw pybind11::error_already_set();
  }
}

template <typename T>
T ThrowOrReturn(const graphar::Result<T>& result) {
  if (result.has_error()) {
    // TODO(yxk) handle different error type
    PyErr_SetString(PyExc_ValueError, result.status().message().c_str());
    throw pybind11::error_already_set();
  }
  return result.value();
}