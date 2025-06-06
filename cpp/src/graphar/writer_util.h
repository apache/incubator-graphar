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
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#ifdef ARROW_ORC
#include "arrow/adapters/orc/adapter.h"
#endif
#include "arrow/api.h"
#include "arrow/csv/api.h"
#include "arrow/dataset/api.h"
#include "arrow/filesystem/api.h"
#include "parquet/arrow/writer.h"
namespace graphar {
class WriterOptions {
 public:
  WriterOptions(const WriterOptions& other)
      : csvOption(other.csvOption),
        parquetOption(other.parquetOption),
        orcOption(other.orcOption) {}

  WriterOptions(WriterOptions&& other) noexcept
      : csvOption(std::move(other.csvOption)),
        parquetOption(std::move(other.parquetOption)),
        orcOption(std::move(other.orcOption)) {}

  WriterOptions& operator=(const WriterOptions& other) {
    if (this != &other) {
      csvOption = other.csvOption;
      parquetOption = other.parquetOption;
      orcOption = other.orcOption;
    }
    return *this;
  }

  WriterOptions& operator=(WriterOptions&& other) noexcept {
    if (this != &other) {
      csvOption = std::move(other.csvOption);
      parquetOption = std::move(other.parquetOption);
      orcOption = std::move(other.orcOption);
    }
    return *this;
  }
  class CSVOption {
   public:
    bool include_header = true;
    int32_t batch_size = 1024;
    char delimiter = ',';
    std::string null_string;
    arrow::io::IOContext io_context;
    std::string eol = "\n";
    arrow::csv::QuotingStyle quoting_style = arrow::csv::QuotingStyle::Needed;
  };
  class ParquetOption {
   public:
    // WriterProperties::Builder options
    bool enable_dictionary = true;
    int64_t dictionary_pagesize_limit = 1024 * 1024;  // 1MB
    int64_t write_batch_size = 1024;
    int64_t max_row_group_length = 1024 * 1024;  // 1Mi rows
    int64_t data_pagesize = 1024 * 1024;         // 1MB
    ::parquet::ParquetDataPageVersion data_page_version =
        ::parquet::ParquetDataPageVersion::V1;
    ::parquet::ParquetVersion::type version =
        ::parquet::ParquetVersion::PARQUET_2_6;
    ::parquet::Encoding::type encoding = ::parquet::Encoding::PLAIN;
    std::unordered_map<std::string, ::parquet::Encoding::type> column_encoding;
    arrow::Compression::type compression = arrow::Compression::UNCOMPRESSED;
    std::unordered_map<std::string, arrow::Compression::type>
        column_compression;
    int compression_level = std::numeric_limits<int>::min();
    std::unordered_map<std::string, int> column_compression_level;
    size_t max_statistics_size = 4096;  // 4KB
    std::unordered_map<std::string, size_t> column_max_statistics_size;
    std::shared_ptr<::parquet::FileEncryptionProperties> encryption_properties;
    bool enable_statistics = true;
    std::unordered_map<std::string, bool> column_statistics;
    std::vector<::parquet::SortingColumn> sorting_columns;
    bool enable_store_decimal_as_integer = false;
    bool enable_write_page_index = false;
    std::unordered_map<std::string, bool> column_write_page_index;
    bool compliant_nested_types = true;
    bool use_threads = false;
    bool enable_deprecated_int96_timestamps = false;
    ::arrow::TimeUnit::type coerce_timestamps = ::arrow::TimeUnit::MICRO;
    bool allow_truncated_timestamps = false;
    bool store_schema = false;
    ::arrow::internal::Executor* executor = nullptr;
  };
  class ORCOption {
#ifdef ARROW_ORC
   public:
    int64_t batch_size = 1024;
    arrow::adapters::orc::FileVersion file_version =
        arrow::adapters::orc::FileVersion(0, 12);
    int64_t stripe_size = 64 * 1024 * 1024;
    arrow::Compression::type compression = arrow::Compression::UNCOMPRESSED;
    int64_t compression_block_size = 64 * 1024;
    arrow::adapters::orc::CompressionStrategy compression_strategy =
        arrow::adapters::orc::CompressionStrategy::kSpeed;
    int64_t row_index_stride = 10000;
    double padding_tolerance = 0.0;
    double dictionary_key_size_threshold = 0.0;
    std::vector<int64_t> bloom_filter_columns;
    double bloom_filter_fpp = 0.05;
#endif
  };
  class CSVBuilder {
    friend WriterOptions;
    std::shared_ptr<CSVOption> option;

   public:
    CSVBuilder() { option = std::make_shared<CSVOption>(); }
    void include_header(bool header) { option->include_header = header; }
    void batch_size(int32_t bs) { option->batch_size = bs; }
    void delimiter(char d) { option->delimiter = d; }
    void null_string(const std::string& ns) { option->null_string = ns; }
    void io_context(const arrow::io::IOContext& ctx) {
      option->io_context = ctx;
    }
    void eol(const std::string& e) { option->eol = e; }
    void quoting_style(arrow::csv::QuotingStyle qs) {
      option->quoting_style = qs;
    }
  };
  class ParquetBuilder {
    friend WriterOptions;
    std::shared_ptr<ParquetOption> option;

   public:
    ParquetBuilder() { option = std::make_shared<ParquetOption>(); }
    void enable_dictionary(bool enable) { option->enable_dictionary = enable; }
    void dictionary_pagesize_limit(int64_t limit) {
      option->dictionary_pagesize_limit = limit;
    }
    void write_batch_size(int64_t batch_size) {
      option->write_batch_size = batch_size;
    }
    void max_row_group_length(int64_t length) {
      option->max_row_group_length = length;
    }
    void data_pagesize(int64_t pagesize) { option->data_pagesize = pagesize; }
    void data_page_version(::parquet::ParquetDataPageVersion version) {
      option->data_page_version = version;
    }
    void version(::parquet::ParquetVersion::type ver) { option->version = ver; }
    void encoding(::parquet::Encoding::type enc) { option->encoding = enc; }
    void column_encoding(
        const std::unordered_map<std::string, ::parquet::Encoding::type>&
            encodings) {
      option->column_encoding = encodings;
    }
    void compression(arrow::Compression::type comp) {
      option->compression = comp;
    }
    void column_compression(
        const std::unordered_map<std::string, arrow::Compression::type>&
            compressions) {
      option->column_compression = compressions;
    }
    void compression_level(int level) { option->compression_level = level; }
    void column_compression_level(
        const std::unordered_map<std::string, int>& levels) {
      option->column_compression_level = levels;
    }
    void max_statistics_size(size_t size) {
      option->max_statistics_size = size;
    }
    void column_max_statistics_size(
        const std::unordered_map<std::string, size_t>& sizes) {
      option->column_max_statistics_size = sizes;
    }
    void encryption_properties(
        const std::shared_ptr<::parquet::FileEncryptionProperties>& props) {
      option->encryption_properties = props;
    }
    void enable_statistics(bool enable) { option->enable_statistics = enable; }
    void column_statistics(const std::unordered_map<std::string, bool>& stats) {
      option->column_statistics = stats;
    }
    void sorting_columns(const std::vector<::parquet::SortingColumn>& columns) {
      option->sorting_columns = columns;
    }
    void enable_store_decimal_as_integer(bool enable) {
      option->enable_store_decimal_as_integer = enable;
    }
    void enable_write_page_index(bool enable) {
      option->enable_write_page_index = enable;
    }
    void column_write_page_index(
        const std::unordered_map<std::string, bool>& indices) {
      option->column_write_page_index = indices;
    }
    void compliant_nested_types(bool compliant) {
      option->compliant_nested_types = compliant;
    }
    void use_threads(bool use) { option->use_threads = use; }
    void enable_deprecated_int96_timestamps(bool enable) {
      option->enable_deprecated_int96_timestamps = enable;
    }
    void coerce_timestamps(::arrow::TimeUnit::type unit) {
      option->coerce_timestamps = unit;
    }
    void allow_truncated_timestamps(bool allow) {
      option->allow_truncated_timestamps = allow;
    }
    void store_schema(bool store) { option->store_schema = store; }
    void executor(::arrow::internal::Executor* exec) {
      option->executor = exec;
    }
  };
  class OrcBuilder {
    friend WriterOptions;
    std::shared_ptr<ORCOption> option;

   public:
    OrcBuilder() { option = std::make_shared<ORCOption>(); }
#ifdef ARROW_ORC
    void batch_size(int64_t bs) { option->batch_size = bs; }
    void file_version(arrow::adapters::orc::FileVersion fv) {
      option->file_version = fv;
    }
    void stripe_size(int64_t ss) { option->stripe_size = ss; }
    void compression(arrow::Compression::type comp) {
      option->compression = comp;
    }
    void compression_block_size(int64_t cbs) {
      option->compression_block_size = cbs;
    }
    void compression_strategy(arrow::adapters::orc::CompressionStrategy cs) {
      option->compression_strategy = cs;
    }
    void row_index_stride(int64_t ris) { option->row_index_stride = ris; }
    void padding_tolerance(double pt) { option->padding_tolerance = pt; }
    void dictionary_key_size_threshold(double dkst) {
      option->dictionary_key_size_threshold = dkst;
    }
    void bloom_filter_columns(const int64_t bfc) {
      option->bloom_filter_columns.push_back(bfc);
    }
    void bloom_filter_fpp(double bffpp) { option->bloom_filter_fpp = bffpp; }
#endif
  };

  class Builder {
    friend WriterOptions;

   public:
    Builder() = default;
    Builder(const Builder& other)
        : csvBuilder(other.csvBuilder),
          parquetBuilder(other.parquetBuilder),
          orcBuilder(other.orcBuilder) {}

    Builder(Builder&& other) noexcept
        : csvBuilder(std::move(other.csvBuilder)),
          parquetBuilder(std::move(other.parquetBuilder)),
          orcBuilder(std::move(other.orcBuilder)) {}

    Builder& operator=(const Builder& other) {
      if (this != &other) {
        csvBuilder = other.csvBuilder;
        parquetBuilder = other.parquetBuilder;
        orcBuilder = other.orcBuilder;
      }
      return *this;
    }

    Builder& operator=(Builder&& other) noexcept {
      if (this != &other) {
        csvBuilder = std::move(other.csvBuilder);
        parquetBuilder = std::move(other.parquetBuilder);
        orcBuilder = std::move(other.orcBuilder);
      }
      return *this;
    }

    std::shared_ptr<CSVBuilder> getCsvOptionBuilder();
    std::shared_ptr<ParquetBuilder> getParquetOptionBuilder();
    std::shared_ptr<OrcBuilder> getOrcOptionBuilder();
    std::shared_ptr<WriterOptions> Build();
    virtual ~Builder() = default;

   private:
    std::shared_ptr<CSVBuilder> csvBuilder;
    std::shared_ptr<ParquetBuilder> parquetBuilder;
    std::shared_ptr<OrcBuilder> orcBuilder;
  };

 public:
  explicit WriterOptions(Builder* builder);
  std::shared_ptr<CSVOption> csvOption;
  std::shared_ptr<ParquetOption> parquetOption;
  std::shared_ptr<ORCOption> orcOption;
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
