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

#ifndef PARQUET_EXAMPLES_GRAPHAR_LABEL_H
#define PARQUET_EXAMPLES_GRAPHAR_LABEL_H

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

/// Set bit in a range
static inline void SetBitmap(uint64_t* bitmap, const int start, const int end) {
  int pos1 = start >> 6, pos2 = end >> 6;
  if (pos1 == pos2) {
    bitmap[pos1] |= (1ULL << (end & 63)) - (1ULL << (start & 63));
  } else {
    bitmap[pos1] |= ~((1ULL << (start & 63)) - 1);
    bitmap[pos2] |= (1ULL << (end & 63)) - 1;
    for (int i = pos1 + 1; i < pos2; ++i) {
      bitmap[i] = ~0ULL;
    }
  }
}

/// Get bit
static inline bool GetBit(const uint64_t* bitmap, const int index) {
  return (bitmap[index >> 6]) & (1ULL << (index & 63));
}

int read_parquet_file_and_get_valid_indices(
    const char* parquet_filename, const int row_num, const int tot_label_num,
    const int tested_label_num, std::vector<int> tested_label_ids,
    const std::function<bool(bool*, int)>& IsValid, int chunk_idx,
    int chunk_size, std::vector<int>* indices = nullptr,
    uint64_t* bitmap = nullptr, const QUERY_TYPE query_type = COUNT);

#endif  // LABEL_H