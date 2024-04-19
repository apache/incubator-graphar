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

#include <filesystem>
#include <string>

#include "gar/util/status.h"

namespace graphar {

// Return the value of the GAR_TEST_DATA environment variable or return error
// Status
Status GetTestResourceRoot(std::string* out) {
  const char* c_root = std::getenv("GAR_TEST_DATA");
  if (!c_root) {
    return Status::IOError(
        "Test resources not found, set GAR_TEST_DATA to <repo root>/testing");
  }
  *out = std::string(c_root);
  return Status::OK();
}

}  // namespace graphar
