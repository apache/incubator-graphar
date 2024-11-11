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

#include "graphar/label.h"

#include <cassert>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <set>

/// Read a parquet file by ParquetReader & get valid indices
/// The first column_num labels are concerned.
int read_parquet_file_and_get_valid_indices(
    const char* parquet_filename, const int row_num, const int tot_label_num,
    const int tested_label_num, std::vector<int> tested_label_ids,
    const std::function<bool(bool*, int)>& IsValid, int chunk_idx,
    int chunk_size, std::vector<int>* indices, uint64_t* bitmap,
    const QUERY_TYPE query_type) {
  // Create a ParquetReader instance
  std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
      parquet::ParquetFileReader::OpenFile(
          parquet_filename + std::to_string(chunk_idx), false);

  // Get the File MetaData
  std::shared_ptr<parquet::FileMetaData> file_metadata =
      parquet_reader->metadata();
  int row_group_count = file_metadata->num_row_groups();
  int num_columns = file_metadata->num_columns();

  // Initialize the column row counts
  std::vector<int> col_row_counts(num_columns, 0);
  bool** value = new bool*[num_columns];
  for (int i = 0; i < num_columns; i++) {
    value[i] = new bool[row_num];
  }

  // Iterate over all the RowGroups in the file
  for (int rg = 0; rg < row_group_count; ++rg) {
    // Get the RowGroup Reader
    std::shared_ptr<parquet::RowGroupReader> row_group_reader =
        parquet_reader->RowGroup(rg);

    int64_t values_read = 0;
    int64_t rows_read = 0;
    std::shared_ptr<parquet::ColumnReader> column_reader;

    ARROW_UNUSED(rows_read);  // prevent warning in release build

    // Read the label columns
    for (int k = 0; k < tested_label_num; k++) {
      int col_id = tested_label_ids[k];
      // Get the Column Reader for the Bool column
      column_reader = row_group_reader->Column(col_id);
      parquet::BoolReader* bool_reader =
          static_cast<parquet::BoolReader*>(column_reader.get());
      // Read all the rows in the column
      while (bool_reader->HasNext()) {
        // Read BATCH_SIZE values at a time. The number of rows read is
        // returned. values_read contains the number of non-null rows

        rows_read = bool_reader->ReadBatch(BATCH_SIZE, nullptr, nullptr,
                                           value[k] + col_row_counts[col_id],
                                           &values_read);

        // There are no NULL values in the rows written
        col_row_counts[col_id] += rows_read;
      }
    }
  }
  const int kTotLabelNum = tot_label_num;
  bool state[kTotLabelNum];
  int count = 0;
  int offset = chunk_idx * chunk_size;
  for (int i = 0; i < row_num; i++) {
    for (int j = 0; j < tested_label_num; j++) {
      state[j] = value[j][i];
    }
    if (IsValid(state, tested_label_num)) {
      count++;
      if (query_type == QUERY_TYPE::INDEX)

        indices->push_back(i + offset);
      else if (query_type == QUERY_TYPE::BITMAP)
        SetBitmap(bitmap, i);
    }
  }

  // destroy the allocated space
  for (int i = 0; i < num_columns; i++) {
    delete[] value[i];
  }
  delete[] value;

  return count;
}
