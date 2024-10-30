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

#ifndef CPP_SRC_GRAPHAR_LABEL_H_
#define CPP_SRC_GRAPHAR_LABEL_H_

#include <arrow/io/file.h>
#include <arrow/util/logging.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <parquet/properties.h>

#include <iostream>
#include <set>
#include <vector>

using parquet::ConvertedType;
using parquet::Encoding;
using parquet::Repetition;
using parquet::Type;
using parquet::schema::GroupNode;
using parquet::schema::PrimitiveNode;

constexpr int BATCH_SIZE = 1024;  // the batch size

/// The query type
enum QUERY_TYPE {
  COUNT,    // return the number of valid vertices
  INDEX,    // return the indices of valid vertices
  BITMAP,   // return the bitmap of valid vertices
  ADAPTIVE  // adaptively return indices or bitmap
};

/// Set bit
static inline void SetBitmap(uint64_t* bitmap, const int index) {
  bitmap[index >> 6] |= (1ULL << (index & 63));
}

int read_parquet_file_and_get_valid_indices(
    const char* parquet_filename, const int row_num, const int tot_label_num,
    const int tested_label_num, std::vector<int> tested_label_ids,
    const std::function<bool(bool*, int)>& IsValid, int chunk_idx,
    int chunk_size, std::vector<int>* indices = nullptr,
    uint64_t* bitmap = nullptr, const QUERY_TYPE query_type = COUNT);

#endif  // CPP_SRC_GRAPHAR_LABEL_H_
