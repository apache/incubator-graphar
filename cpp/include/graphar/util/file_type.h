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

#include <map>
#include <stdexcept>
#include <string>

#include "graphar/fwd.h"
#include "graphar/util/macros.h"

namespace graphar {

static inline FileType StringToFileType(const std::string& str) {
  static const std::map<std::string, FileType> str2file_type{
      {"csv", FileType::CSV},
      {"json", FileType::JSON},
      {"parquet", FileType::PARQUET},
      {"orc", FileType::ORC}};
  try {
    return str2file_type.at(str.c_str());
  } catch (const std::exception& e) {
    throw std::runtime_error("KeyError: " + str);
  }
}

static inline const char* FileTypeToString(FileType file_type) {
  static const std::map<FileType, const char*> file_type2string{
      {FileType::CSV, "csv"},
      {FileType::JSON, "json"},
      {FileType::PARQUET, "parquet"},
      {FileType::ORC, "orc"}};
  return file_type2string.at(file_type);
}

}  // namespace graphar
