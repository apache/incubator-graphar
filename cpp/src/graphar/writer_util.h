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

#include "graphar/fwd.h"
#include "graphar/macros.h"

#include <arrow/util/compression.h>
namespace graphar {
class WriterOptions {
 public:
  class Builder {
   public:
    FileType file_type;
    bool include_header = true;
    char delimiter = ',';
    arrow::Compression::type compression;
    int compression_level = 0;

    Builder(FileType file_type) : file_type(file_type) {}

    void set_include_header(bool value) {
      if (file_type != FileType::CSV) {
        throw Status::Invalid("include_header can only be set for CSV files");
      }
      include_header = value;
    }
    void set_delimiter(char delimiter) {
      if (file_type != FileType::CSV) {
        throw Status::Invalid("delimiter can only be set for CSV files");
      }
      if (delimiter == '\0') {
        throw Status::Invalid("delimiter cannot be null character");
      }
      this->delimiter = delimiter;
    }
    void set_compression(arrow::Compression::type type) {
      if (file_type != FileType::PARQUET && file_type != FileType::ORC) {
        throw Status::Invalid(
            "compression can only be set for PARQUET or ORC files");
      }
      compression = type;
    }
    void set_compression_level(int level) {
      if (file_type != FileType::PARQUET && file_type != FileType::ORC) {
        throw Status::Invalid(
            "compression_level can only be set for PARQUET or ORC files");
      }
      compression_level = level;
    }
    std::shared_ptr<WriterOptions> Build() {
      return std::make_shared<WriterOptions>(this);
    }
  };

 public:
  FileType file_type;
  bool include_header;
  char delimiter;
  arrow::Compression::type compression;
  int compression_level = 0;

  explicit WriterOptions(const class Builder* builder)
      : file_type(builder->file_type),
        include_header(builder->include_header),
        delimiter(builder->delimiter),
        compression(builder->compression),
        compression_level(builder->compression_level) {}
};

/**
 * @brief The level for validating writing operations.
 */
enum class ValidateLevel : char {
  /// To use the default validate level of the writer/builder.
  default_validate = 0,
  /// To skip the validation.
  no_validate = 1,
  /// Weak validation: check if the index, count, adj_list type, property group
  /// and the size of the table passed to the writer/builder are valid.
  weak_validate = 2,
  /// Strong validation: except for the weak validation, also check if the
  /// schema (including each property name and data type) of the intput data
  /// passed to the writer/builder is consistent with that defined in the info.
  strong_validate = 3
};

}  // namespace graphar
