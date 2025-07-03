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
#include <parquet/properties.h>
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
/**
 * @class WriterOptions
 * @brief Provides configuration options for different file format writers (CSV,
 * Parquet, ORC) in GraphAr. A WriterOptions instance can simultaneously contain
 * options for all three formats (CSV, Parquet, ORC). The actual file format
 * used for writing is determined by the FileType specified in the graph_Info.
 *
 * The configuration parameters and their default values are aligned with those
 * in Arrow.
 *
 * CSVOptionBuilder, ParquetOptionBuilder, and ORCOptionBuilder are used to
 * construct format-specific options for CSV, Parquet, and ORC, respectively.
 * An existing WriterOptions instance can be passed to a builder’s constructor
 * to incrementally add or combine options across different formats.
 * Example:
 * CSVOptionBuilder csvBuilder;
 * auto wopt = csvBuilder.build();
 * ParquetOptionBuilder parquetBuilder(wopt);
 * wopt = parquetBuilder.build();
 */
class WriterOptions {
 private:
  /**
   * @class CSVOption
   * @brief Configuration options for CSV Writer.
   * The complete list of supported parameters can be found in:
   * https://arrow.apache.org/docs/cpp/api/formats.html#csv-writer
   */
  class CSVOption {
   public:
    arrow::io::IOContext io_context;
    std::string null_string;
    std::string eol = "\n";
    bool include_header = true;
    char delimiter = ',';
    int32_t batch_size = 1024;
    arrow::csv::QuotingStyle quoting_style = arrow::csv::QuotingStyle::Needed;
  };
  /**
   * @class ParquetOption
   * @brief Configuration options for Parquet Writer.
   * This class includes parameters from both `WriterProperties` and
   * `ArrowWriterProperties`. The complete list of supported parameters can be
   * found in: https://arrow.apache.org/docs/cpp/api/formats.html#parquet-writer
   */
  class ParquetOption {
   public:
    std::shared_ptr<::parquet::FileEncryptionProperties> encryption_properties;
    std::unordered_map<std::string, ::parquet::Encoding::type> column_encoding;
    std::unordered_map<std::string, arrow::Compression::type>
        column_compression;
    std::unordered_map<std::string, int> column_compression_level;
    std::unordered_map<std::string, size_t> column_max_statistics_size;
    std::unordered_map<std::string, bool> column_statistics;
    std::unordered_map<std::string, bool> column_write_page_index;
    std::vector<::parquet::SortingColumn> sorting_columns;
    int64_t dictionary_pagesize_limit = 1024 * 1024;
    int64_t write_batch_size = 1024;
    int64_t max_row_group_length = 1024 * 1024;
    int64_t data_pagesize = 1024 * 1024;
    size_t max_statistics_size = 4096;
    int compression_level = std::numeric_limits<int>::min();
    ::parquet::ParquetDataPageVersion data_page_version =
        ::parquet::ParquetDataPageVersion::V1;
    ::parquet::ParquetVersion::type version =
        ::parquet::ParquetVersion::PARQUET_2_6;
    ::parquet::Encoding::type encoding = ::parquet::Encoding::PLAIN;
    arrow::Compression::type compression = arrow::Compression::ZSTD;
    ::arrow::TimeUnit::type coerce_timestamps = ::arrow::TimeUnit::MICRO;
    ::arrow::internal::Executor* executor = nullptr;
    bool enable_dictionary = true;
    bool enable_statistics = true;
    bool enable_store_decimal_as_integer = false;
    bool enable_write_page_index = false;
    bool compliant_nested_types = true;
    bool use_threads = false;
    bool enable_deprecated_int96_timestamps = false;
    bool allow_truncated_timestamps = false;
    bool store_schema = false;
  };
  /**
   * @class ORCOption
   * @brief Configuration options for ORC Writer.
   * The complete list of supported parameters can be found in:
   * https://arrow.apache.org/docs/cpp/api/formats.html#_CPPv4N5arrow8adapters3orc12WriteOptionsE
   */
  class ORCOption {
#ifdef ARROW_ORC
   public:
    std::vector<int64_t> bloom_filter_columns;
    arrow::adapters::orc::FileVersion file_version =
        arrow::adapters::orc::FileVersion(0, 12);
    arrow::adapters::orc::CompressionStrategy compression_strategy =
        arrow::adapters::orc::CompressionStrategy::kSpeed;
    arrow::Compression::type compression = arrow::Compression::UNCOMPRESSED;
    int64_t stripe_size = 64 * 1024 * 1024;
    int64_t batch_size = 1024;
    int64_t compression_block_size = 64 * 1024;
    int64_t row_index_stride = 10000;
    double padding_tolerance = 0.0;
    double dictionary_key_size_threshold = 0.0;
    double bloom_filter_fpp = 0.05;
#endif
  };

 public:
  // Builder for CSVOption
  class CSVOptionBuilder {
   public:
    CSVOptionBuilder() : option_(std::make_shared<CSVOption>()) {}
    explicit CSVOptionBuilder(std::shared_ptr<WriterOptions> wopt)
        : writerOptions_(wopt),
          option_(wopt && wopt->csvOption_ ? wopt->csvOption_
                                           : std::make_shared<CSVOption>()) {}
    CSVOptionBuilder& include_header(bool header) {
      option_->include_header = header;
      return *this;
    }
    CSVOptionBuilder& batch_size(int32_t bs) {
      option_->batch_size = bs;
      return *this;
    }
    CSVOptionBuilder& delimiter(char d) {
      option_->delimiter = d;
      return *this;
    }
    CSVOptionBuilder& null_string(const std::string& ns) {
      option_->null_string = ns;
      return *this;
    }
    CSVOptionBuilder& io_context(const arrow::io::IOContext& ctx) {
      option_->io_context = ctx;
      return *this;
    }
    CSVOptionBuilder& eol(const std::string& e) {
      option_->eol = e;
      return *this;
    }
    CSVOptionBuilder& quoting_style(arrow::csv::QuotingStyle qs) {
      option_->quoting_style = qs;
      return *this;
    }
    std::shared_ptr<WriterOptions> build() {
      if (!writerOptions_) {
        writerOptions_ = std::make_shared<WriterOptions>();
      }
      writerOptions_->setCsvOption(option_);
      return writerOptions_;
    }

   private:
    std::shared_ptr<WriterOptions> writerOptions_;
    std::shared_ptr<CSVOption> option_;
  };

  // Builder for ParquetOption
  class ParquetOptionBuilder {
   public:
    ParquetOptionBuilder() : option_(std::make_shared<ParquetOption>()) {}
    explicit ParquetOptionBuilder(std::shared_ptr<WriterOptions> wopt)
        : writerOptions_(wopt),
          option_(wopt && wopt->parquetOption_
                      ? wopt->parquetOption_
                      : std::make_shared<ParquetOption>()) {}
    ParquetOptionBuilder& enable_dictionary(bool enable) {
      option_->enable_dictionary = enable;
      return *this;
    }
    ParquetOptionBuilder& dictionary_pagesize_limit(int64_t limit) {
      option_->dictionary_pagesize_limit = limit;
      return *this;
    }
    ParquetOptionBuilder& write_batch_size(int64_t batch_size) {
      option_->write_batch_size = batch_size;
      return *this;
    }
    ParquetOptionBuilder& max_row_group_length(int64_t length) {
      option_->max_row_group_length = length;
      return *this;
    }
    ParquetOptionBuilder& data_pagesize(int64_t pagesize) {
      option_->data_pagesize = pagesize;
      return *this;
    }
    ParquetOptionBuilder& data_page_version(
        ::parquet::ParquetDataPageVersion version) {
      option_->data_page_version = version;
      return *this;
    }
    ParquetOptionBuilder& version(::parquet::ParquetVersion::type ver) {
      option_->version = ver;
      return *this;
    }
    ParquetOptionBuilder& encoding(::parquet::Encoding::type enc) {
      option_->encoding = enc;
      return *this;
    }
    ParquetOptionBuilder& column_encoding(
        const std::unordered_map<std::string, ::parquet::Encoding::type>&
            encodings) {
      option_->column_encoding = encodings;
      return *this;
    }
    ParquetOptionBuilder& compression(arrow::Compression::type comp) {
      option_->compression = comp;
      return *this;
    }
    ParquetOptionBuilder& column_compression(
        const std::unordered_map<std::string, arrow::Compression::type>&
            compressions) {
      option_->column_compression = compressions;
      return *this;
    }
    ParquetOptionBuilder& compression_level(int level) {
      option_->compression_level = level;
      return *this;
    }
    ParquetOptionBuilder& column_compression_level(
        const std::unordered_map<std::string, int>& levels) {
      option_->column_compression_level = levels;
      return *this;
    }
    ParquetOptionBuilder& max_statistics_size(size_t size) {
      option_->max_statistics_size = size;
      return *this;
    }
    ParquetOptionBuilder& column_max_statistics_size(
        const std::unordered_map<std::string, size_t>& sizes) {
      option_->column_max_statistics_size = sizes;
      return *this;
    }
    ParquetOptionBuilder& encryption_properties(
        const std::shared_ptr<::parquet::FileEncryptionProperties>& props) {
      option_->encryption_properties = props;
      return *this;
    }
    ParquetOptionBuilder& enable_statistics(bool enable) {
      option_->enable_statistics = enable;
      return *this;
    }
    ParquetOptionBuilder& column_statistics(
        const std::unordered_map<std::string, bool>& stats) {
      option_->column_statistics = stats;
      return *this;
    }
    ParquetOptionBuilder& sorting_columns(
        const std::vector<::parquet::SortingColumn>& columns) {
      option_->sorting_columns = columns;
      return *this;
    }
    ParquetOptionBuilder& enable_store_decimal_as_integer(bool enable) {
      option_->enable_store_decimal_as_integer = enable;
      return *this;
    }
    ParquetOptionBuilder& enable_write_page_index(bool enable) {
      option_->enable_write_page_index = enable;
      return *this;
    }
    ParquetOptionBuilder& column_write_page_index(
        const std::unordered_map<std::string, bool>& indices) {
      option_->column_write_page_index = indices;
      return *this;
    }
    ParquetOptionBuilder& compliant_nested_types(bool compliant) {
      option_->compliant_nested_types = compliant;
      return *this;
    }
    ParquetOptionBuilder& use_threads(bool use) {
      option_->use_threads = use;
      return *this;
    }
    ParquetOptionBuilder& enable_deprecated_int96_timestamps(bool enable) {
      option_->enable_deprecated_int96_timestamps = enable;
      return *this;
    }
    ParquetOptionBuilder& coerce_timestamps(::arrow::TimeUnit::type unit) {
      option_->coerce_timestamps = unit;
      return *this;
    }
    ParquetOptionBuilder& allow_truncated_timestamps(bool allow) {
      option_->allow_truncated_timestamps = allow;
      return *this;
    }
    ParquetOptionBuilder& store_schema(bool store) {
      option_->store_schema = store;
      return *this;
    }
    ParquetOptionBuilder& executor(::arrow::internal::Executor* exec) {
      option_->executor = exec;
      return *this;
    }
    std::shared_ptr<WriterOptions> build() {
      if (!writerOptions_) {
        writerOptions_ = std::make_shared<WriterOptions>();
      }
      writerOptions_->setParquetOption(option_);
      return writerOptions_;
    }

   private:
    std::shared_ptr<WriterOptions> writerOptions_;
    std::shared_ptr<ParquetOption> option_;
  };

  // Builder for ORCOption
  class ORCOptionBuilder {
   public:
    ORCOptionBuilder() : option_(std::make_shared<ORCOption>()) {}
    explicit ORCOptionBuilder(std::shared_ptr<WriterOptions> wopt)
        : writerOptions_(wopt),
          option_(wopt && wopt->orcOption_ ? wopt->orcOption_
                                           : std::make_shared<ORCOption>()) {}
#ifdef ARROW_ORC
    ORCOptionBuilder& batch_size(int64_t bs) {
      option_->batch_size = bs;
      return *this;
    }
    ORCOptionBuilder& file_version(arrow::adapters::orc::FileVersion fv) {
      option_->file_version = fv;
      return *this;
    }
    ORCOptionBuilder& stripe_size(int64_t ss) {
      option_->stripe_size = ss;
      return *this;
    }
    ORCOptionBuilder& compression(arrow::Compression::type comp) {
      option_->compression = comp;
      return *this;
    }
    ORCOptionBuilder& compression_block_size(int64_t cbs) {
      option_->compression_block_size = cbs;
      return *this;
    }
    ORCOptionBuilder& compression_strategy(
        arrow::adapters::orc::CompressionStrategy cs) {
      option_->compression_strategy = cs;
      return *this;
    }
    ORCOptionBuilder& row_index_stride(int64_t ris) {
      option_->row_index_stride = ris;
      return *this;
    }
    ORCOptionBuilder& padding_tolerance(double pt) {
      option_->padding_tolerance = pt;
      return *this;
    }
    ORCOptionBuilder& dictionary_key_size_threshold(double dkst) {
      option_->dictionary_key_size_threshold = dkst;
      return *this;
    }
    ORCOptionBuilder& bloom_filter_columns(const int64_t bfc) {
      option_->bloom_filter_columns.push_back(bfc);
      return *this;
    }
    ORCOptionBuilder& bloom_filter_fpp(double bffpp) {
      option_->bloom_filter_fpp = bffpp;
      return *this;
    }
#endif
    std::shared_ptr<WriterOptions> build() {
      if (!writerOptions_) {
        writerOptions_ = std::make_shared<WriterOptions>();
      }
      writerOptions_->setOrcOption(option_);
      return writerOptions_;
    }

   private:
    std::shared_ptr<WriterOptions> writerOptions_;
    std::shared_ptr<ORCOption> option_;
  };

  WriterOptions() = default;
  WriterOptions(std::shared_ptr<CSVOption> csv,
                std::shared_ptr<ParquetOption> parquet,
                std::shared_ptr<ORCOption> orc)
      : csvOption_(csv), parquetOption_(parquet), orcOption_(orc) {}
  static std::shared_ptr<WriterOptions> DefaultWriterOption() {
    return std::make_shared<WriterOptions>();
  }
  void setCsvOption(std::shared_ptr<CSVOption> csv_option) {
    csvOption_ = csv_option;
  }
  void setParquetOption(std::shared_ptr<ParquetOption> parquet_option) {
    parquetOption_ = parquet_option;
  }
  void setOrcOption(std::shared_ptr<ORCOption> orc_option) {
    orcOption_ = orc_option;
  }
  arrow::csv::WriteOptions getCsvOption() const;
  std::shared_ptr<parquet::WriterProperties> getParquetWriterProperties() const;
  std::shared_ptr<parquet::ArrowWriterProperties> getArrowWriterProperties()
      const;
#ifdef ARROW_ORC
  arrow::adapters::orc::WriteOptions getOrcOption() const;
#endif

 private:
  std::shared_ptr<CSVOption> csvOption_;
  std::shared_ptr<ParquetOption> parquetOption_;
  std::shared_ptr<ORCOption> orcOption_;
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
