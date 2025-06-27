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
namespace graphar {
arrow::csv::WriteOptions WriterOptions::getCsvOption() const {
  if (csvOption_) {
    arrow::csv::WriteOptions csvWriteOptions;
    csvWriteOptions.include_header = csvOption_->include_header;
    csvWriteOptions.batch_size = csvOption_->batch_size;
    csvWriteOptions.delimiter = csvOption_->delimiter;
    csvWriteOptions.null_string = csvOption_->null_string;
    csvWriteOptions.io_context = csvOption_->io_context;
    csvWriteOptions.eol = csvOption_->eol;
    csvWriteOptions.quoting_style = csvOption_->quoting_style;
    return csvWriteOptions;
  } else {
    return arrow::csv::WriteOptions::Defaults();
  }
}

std::shared_ptr<parquet::WriterProperties>
WriterOptions::getParquetWriterProperties() const {
  parquet::WriterProperties::Builder builder;
  if (parquetOption_) {
    builder
        .dictionary_pagesize_limit(parquetOption_->dictionary_pagesize_limit)
        ->write_batch_size(parquetOption_->write_batch_size)
        ->max_row_group_length(parquetOption_->max_row_group_length)
        ->data_pagesize(parquetOption_->data_pagesize)
        ->data_page_version(parquetOption_->data_page_version)
        ->version(parquetOption_->version)
        ->encoding(parquetOption_->encoding)
        ->max_statistics_size(parquetOption_->max_statistics_size)
        ->compression(parquetOption_->compression)
        ->compression_level(parquetOption_->compression_level);
    for (const auto& kv : parquetOption_->column_encoding) {
      builder.encoding(kv.first, kv.second);
    }
    for (const auto& kv : parquetOption_->column_compression) {
      builder.compression(kv.first, kv.second);
    }
    for (const auto& kv : parquetOption_->column_compression_level) {
      builder.compression_level(kv.first, kv.second);
    }
    if (!parquetOption_->enable_dictionary) {
      builder.disable_dictionary();
    }
    if (parquetOption_->encryption_properties) {
      builder.encryption(parquetOption_->encryption_properties);
    }
    if (!parquetOption_->enable_statistics) {
      builder.disable_statistics();
    }
    for (const auto& path_st : parquetOption_->column_statistics) {
      if (!path_st.second) {
        builder.disable_statistics(path_st.first);
      }
    }
    if (!parquetOption_->sorting_columns.empty()) {
      builder.set_sorting_columns(parquetOption_->sorting_columns);
    }
    if (parquetOption_->enable_store_decimal_as_integer) {
      builder.enable_store_decimal_as_integer();
    }
    if (parquetOption_->enable_write_page_index) {
      builder.enable_write_page_index();
    }
  }
  return builder.build();
}

std::shared_ptr<parquet::ArrowWriterProperties>
WriterOptions::getArrowWriterProperties() const {
  parquet::ArrowWriterProperties::Builder builder;
  if (parquetOption_) {
    if (!parquetOption_->compliant_nested_types) {
      builder.disable_compliant_nested_types();
    }
    builder.set_use_threads(parquetOption_->use_threads);
    if (parquetOption_->enable_deprecated_int96_timestamps) {
      builder.enable_deprecated_int96_timestamps();
    }
    builder.coerce_timestamps(parquetOption_->coerce_timestamps);
    if (parquetOption_->allow_truncated_timestamps) {
      builder.allow_truncated_timestamps();
    }
    if (parquetOption_->store_schema) {
      builder.store_schema();
    }
    if (parquetOption_->executor) {
      builder.set_executor(parquetOption_->executor);
    }
  }
  return builder.build();
}

#ifdef ARROW_ORC
arrow::adapters::orc::WriteOptions WriterOptions::getOrcOption() const {
  auto writer_options = arrow::adapters::orc::WriteOptions();
  writer_options.compression = arrow::Compression::ZSTD;
  if (orcOption_) {
    writer_options.batch_size = orcOption_->batch_size;
    writer_options.compression = orcOption_->compression;
    writer_options.stripe_size = orcOption_->stripe_size;
    writer_options.file_version = orcOption_->file_version;
    writer_options.bloom_filter_columns = orcOption_->bloom_filter_columns;
    writer_options.bloom_filter_fpp = orcOption_->bloom_filter_fpp;
  }
  return writer_options;
}
#endif
}  // namespace graphar
