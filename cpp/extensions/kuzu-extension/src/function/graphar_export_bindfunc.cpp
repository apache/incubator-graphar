#include "function/graphar_export.h"

namespace kuzu {
namespace graphar_extension {

using namespace function;
using namespace common;

void GrapharExportOptions::setCSVNullString(common::Value& value) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::STRING) {
        throw common::RuntimeException{
            common::stringFormat("CSV null string option expects a string value, got: {}.",
                value.getDataType().toString())};
    }
    csvOptionBuilder->null_string(value.getValue<std::string>());
}

void GrapharExportOptions::setCSVDelimiter(common::Value& value) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::STRING) {
        throw common::RuntimeException{
            common::stringFormat("CSV delimiter option expects a string value, got: {}.",
                value.getDataType().toString())};
    }
    auto strVal = value.getValue<std::string>();
    if (strVal.length() != 1) {
        throw common::RuntimeException{common::stringFormat(
            "CSV delimiter option expects a single character, got: {}.", strVal)};
    }
    csvOptionBuilder->delimiter(strVal[0]);
}

void GrapharExportOptions::setCSVIncludeHeader(common::Value& value) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::BOOL) {
        throw common::RuntimeException{
            common::stringFormat("CSV include header option expects a boolean value, got: {}.",
                value.getDataType().toString())};
    }
    csvOptionBuilder->include_header(value.getValue<bool>());
}

void GrapharExportOptions::setCSVBatchSize(common::Value& value) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::INT64) {
        throw common::RuntimeException{
            common::stringFormat("CSV batch size option expects an integer value, got: {}.",
                value.getDataType().toString())};
    }
    auto intVal = value.getValue<int64_t>();
    if (intVal <= 0) {
        throw common::RuntimeException{common::stringFormat(
            "CSV batch size option expects a positive integer value, got: {}.", intVal)};
    }
    csvOptionBuilder->batch_size(static_cast<int32_t>(intVal));
}

void GrapharExportOptions::setCSVEOL(common::Value& value) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::STRING) {
        throw common::RuntimeException{common::stringFormat(
            "CSV EOL option expects a string value, got: {}.", value.getDataType().toString())};
    }
    csvOptionBuilder->eol(value.getValue<std::string>());
}

void GrapharExportOptions::setCSVQuotingStyle(common::Value& value) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::STRING) {
        throw common::RuntimeException{
            common::stringFormat("CSV quoting style option expects a string value, got: {}.",
                value.getDataType().toString())};
    }
    auto strVal = common::StringUtils::getUpper(value.getValue<std::string>());
    if (strVal == "ALLVALID") {
        csvOptionBuilder->quoting_style(arrow::csv::QuotingStyle::AllValid);
    } else if (strVal == "NONE") {
        csvOptionBuilder->quoting_style(arrow::csv::QuotingStyle::None);
    } else if (strVal == "NEEDED") {
        csvOptionBuilder->quoting_style(arrow::csv::QuotingStyle::Needed);
    } else {
        throw common::RuntimeException{
            common::stringFormat("Unrecognized csv quoting style option: {}.", value.toString())};
    }
}

void GrapharExportOptions::setParquetDictionaryPageSizeLimit(common::Value& value) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::INT64) {
        throw common::RuntimeException{common::stringFormat(
            "Parquet dictionary pagesize limit option expects an integer value, got: {}.",
            value.getDataType().toString())};
    }
    auto intVal = value.getValue<int64_t>();
    if (intVal <= 0) {
        throw common::RuntimeException{
            common::stringFormat("Parquet dictionary pagesize limit option expects a positive "
                                 "integer value, got: {}.",
                intVal)};
    }
    parquetOptionBuilder->dictionary_pagesize_limit(intVal);
}

void GrapharExportOptions::setParquetWriteBatchSize(common::Value& value) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::INT64) {
        throw common::RuntimeException{common::stringFormat(
            "Parquet write batch size option expects an integer value, got: {}.",
            value.getDataType().toString())};
    }
    auto intVal = value.getValue<int64_t>();
    if (intVal <= 0) {
        throw common::RuntimeException{common::stringFormat(
            "Parquet write batch size option expects a positive integer value, got: {}.", intVal)};
    }
    parquetOptionBuilder->write_batch_size(intVal);
}

void GrapharExportOptions::setParquetMaxRowGroupLength(common::Value& value) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::INT64) {
        throw common::RuntimeException{common::stringFormat(
            "Parquet max row group length option expects an integer value, got: {}.",
            value.getDataType().toString())};
    }
    auto intVal = value.getValue<int64_t>();
    if (intVal <= 0) {
        throw common::RuntimeException{common::stringFormat(
            "Parquet max row group length option expects a positive integer value, got: {}.",
            intVal)};
    }
    parquetOptionBuilder->max_row_group_length(intVal);
}

void GrapharExportOptions::setParquetDataPageSize(common::Value& value) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::INT64) {
        throw common::RuntimeException{
            common::stringFormat("Parquet data pagesize option expects an integer value, got: {}.",
                value.getDataType().toString())};
    }
    auto intVal = value.getValue<int64_t>();
    if (intVal <= 0) {
        throw common::RuntimeException{common::stringFormat(
            "Parquet data pagesize option expects a positive integer value, got: {}.", intVal)};
    }
    parquetOptionBuilder->data_pagesize(intVal);
}

void GrapharExportOptions::setParquetMaxStatisticsSize(common::Value& value) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::INT64) {
        throw common::RuntimeException{common::stringFormat(
            "Parquet max statistics size option expects an integer value, got: {}.",
            value.getDataType().toString())};
    }
    auto intVal = value.getValue<int64_t>();
    if (intVal <= 0) {
        throw common::RuntimeException{common::stringFormat(
            "Parquet max statistics size option expects a positive integer value, got: {}.",
            intVal)};
    }
    parquetOptionBuilder->max_statistics_size(static_cast<size_t>(intVal));
}

void GrapharExportOptions::setParquetCompressionLevel(common::Value& value) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::INT64) {
        throw common::RuntimeException{common::stringFormat(
            "Parquet compression level option expects an integer value, got: {}.",
            value.getDataType().toString())};
    }
    parquetOptionBuilder->compression_level(static_cast<int>(value.getValue<int64_t>()));
}

void GrapharExportOptions::setParquetDataPageVersion(common::Value& value) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::STRING) {
        throw common::RuntimeException{common::stringFormat(
            "Parquet data page version option expects a string value, got: {}.",
            value.getDataType().toString())};
    }
    auto strVal = common::StringUtils::getUpper(value.getValue<std::string>());
    if (strVal == "V1") {
        parquetOptionBuilder->data_page_version(::parquet::ParquetDataPageVersion::V1);
    } else if (strVal == "V2") {
        parquetOptionBuilder->data_page_version(::parquet::ParquetDataPageVersion::V2);
    } else {
        throw common::RuntimeException{common::stringFormat(
            "Unrecognized parquet data page version option: {}.", value.toString())};
    }
}

void GrapharExportOptions::setParquetVersion(common::Value& value) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::STRING) {
        throw common::RuntimeException{
            common::stringFormat("Parquet version option expects a string value, got: {}.",
                value.getDataType().toString())};
    }
    auto strVal = common::StringUtils::getUpper(value.getValue<std::string>());
    if (strVal == "PARQUET_1_0") {
        parquetOptionBuilder->version(::parquet::ParquetVersion::PARQUET_1_0);
    } else if (strVal == "PARQUET_2_0") {
        parquetOptionBuilder->version(::parquet::ParquetVersion::PARQUET_2_0);
    } else if (strVal == "PARQUET_2_4") {
        parquetOptionBuilder->version(::parquet::ParquetVersion::PARQUET_2_4);
    } else if (strVal == "PARQUET_2_6") {
        parquetOptionBuilder->version(::parquet::ParquetVersion::PARQUET_2_6);
    } else if (strVal == "PARQUET_2_LATEST") {
        parquetOptionBuilder->version(::parquet::ParquetVersion::PARQUET_2_LATEST);
    } else {
        throw common::RuntimeException{
            common::stringFormat("Unrecognized parquet version option: {}.", value.toString())};
    }
}

void GrapharExportOptions::setParquetEncoding(common::Value& value) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::STRING) {
        throw common::RuntimeException{
            common::stringFormat("Parquet encoding option expects a string value, got: {}.",
                value.getDataType().toString())};
    }
    auto strVal = common::StringUtils::getUpper(value.getValue<std::string>());
    if (strVal == "PLAIN") {
        parquetOptionBuilder->encoding(::parquet::Encoding::PLAIN);
    } else if (strVal == "RLE") {
        parquetOptionBuilder->encoding(::parquet::Encoding::RLE);
    } else if (strVal == "BIT_PACKED") {
        parquetOptionBuilder->encoding(::parquet::Encoding::BIT_PACKED);
    } else if (strVal == "DELTA_BINARY_PACKED") {
        parquetOptionBuilder->encoding(::parquet::Encoding::DELTA_BINARY_PACKED);
    } else if (strVal == "DELTA_LENGTH_BYTE_ARRAY") {
        parquetOptionBuilder->encoding(::parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY);
    } else if (strVal == "DELTA_BYTE_ARRAY") {
        parquetOptionBuilder->encoding(::parquet::Encoding::DELTA_BYTE_ARRAY);
    } else if (strVal == "RLE_DICTIONARY") {
        parquetOptionBuilder->encoding(::parquet::Encoding::RLE_DICTIONARY);
    } else if (strVal == "BYTE_STREAM_SPLIT") {
        parquetOptionBuilder->encoding(::parquet::Encoding::BYTE_STREAM_SPLIT);
    } else {
        throw common::RuntimeException{
            common::stringFormat("Unrecognized parquet encoding option: {}.", value.toString())};
    }
}

void GrapharExportOptions::setParquetCompression(common::Value& value) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::STRING) {
        throw common::RuntimeException{
            common::stringFormat("Parquet compression option expects a string value, got: {}.",
                value.getDataType().toString())};
    }
    auto strVal = common::StringUtils::getUpper(value.getValue<std::string>());
    if (strVal == "UNCOMPRESSED") {
        parquetOptionBuilder->compression(arrow::Compression::UNCOMPRESSED);
    } else if (strVal == "SNAPPY") {
        parquetOptionBuilder->compression(arrow::Compression::SNAPPY);
    } else if (strVal == "GZIP") {
        parquetOptionBuilder->compression(arrow::Compression::GZIP);
    } else if (strVal == "BROTLI") {
        parquetOptionBuilder->compression(arrow::Compression::BROTLI);
    } else if (strVal == "LZ4") {
        parquetOptionBuilder->compression(arrow::Compression::LZ4);
    } else if (strVal == "ZSTD") {
        parquetOptionBuilder->compression(arrow::Compression::ZSTD);
    } else {
        throw common::RuntimeException{
            common::stringFormat("Unrecognized parquet compression option: {}.", value.toString())};
    }
}

void GrapharExportOptions::setParquetCoerceTimestamps(common::Value& value) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::STRING) {
        throw common::RuntimeException{common::stringFormat(
            "Parquet coerce timestamps option expects a string value, got: {}.",
            value.getDataType().toString())};
    }
    auto strVal = common::StringUtils::getUpper(value.getValue<std::string>());
    if (strVal == "SECOND") {
        parquetOptionBuilder->coerce_timestamps(::arrow::TimeUnit::SECOND);
    } else if (strVal == "MILLI") {
        parquetOptionBuilder->coerce_timestamps(::arrow::TimeUnit::MILLI);
    } else if (strVal == "MICRO") {
        parquetOptionBuilder->coerce_timestamps(::arrow::TimeUnit::MICRO);
    } else if (strVal == "NANO") {
        parquetOptionBuilder->coerce_timestamps(::arrow::TimeUnit::NANO);
    } else {
        throw common::RuntimeException{common::stringFormat(
            "Unrecognized parquet coerce timestamps option: {}.", value.toString())};
    }
}

void GrapharExportOptions::setParquetEnableDictionary(common::Value& value) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::BOOL) {
        throw common::RuntimeException{common::stringFormat(
            "Parquet enable dictionary option expects a boolean value, got: {}.",
            value.getDataType().toString())};
    }
    parquetOptionBuilder->enable_dictionary(value.getValue<bool>());
}

void GrapharExportOptions::setParquetEnableStatistics(common::Value& value) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::BOOL) {
        throw common::RuntimeException{common::stringFormat(
            "Parquet enable statistics option expects a boolean value, got: {}.",
            value.getDataType().toString())};
    }
    parquetOptionBuilder->enable_statistics(value.getValue<bool>());
}

void GrapharExportOptions::setParquetEnableStoreDecimalAsInteger(common::Value& value) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::BOOL) {
        throw common::RuntimeException{common::stringFormat(
            "Parquet enable store decimal as integer option expects a boolean value, got: {}.",
            value.getDataType().toString())};
    }
    parquetOptionBuilder->enable_store_decimal_as_integer(value.getValue<bool>());
}

void GrapharExportOptions::setParquetEnableWritePageIndex(common::Value& value) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::BOOL) {
        throw common::RuntimeException{common::stringFormat(
            "Parquet enable write page index option expects a boolean value, got: {}.",
            value.getDataType().toString())};
    }
    parquetOptionBuilder->enable_write_page_index(value.getValue<bool>());
}

void GrapharExportOptions::setParquetCompliantNestedTypes(common::Value& value) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::BOOL) {
        throw common::RuntimeException{common::stringFormat(
            "Parquet compliant nested types option expects a boolean value, got: {}.",
            value.getDataType().toString())};
    }
    parquetOptionBuilder->compliant_nested_types(value.getValue<bool>());
}

void GrapharExportOptions::setParquetUseThreads(common::Value& value) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::BOOL) {
        throw common::RuntimeException{
            common::stringFormat("Parquet use threads option expects a boolean value, got: {}.",
                value.getDataType().toString())};
    }
    parquetOptionBuilder->use_threads(value.getValue<bool>());
}

void GrapharExportOptions::setParquetEnableDeprecatedInt96Timestamps(common::Value& value) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::BOOL) {
        throw common::RuntimeException{common::stringFormat(
            "Parquet enable deprecated int96 timestamps option expects a boolean value, got: {}.",
            value.getDataType().toString())};
    }
    parquetOptionBuilder->enable_deprecated_int96_timestamps(value.getValue<bool>());
}

void GrapharExportOptions::setParquetAllowTruncatedTimestamps(common::Value& value) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::BOOL) {
        throw common::RuntimeException{common::stringFormat(
            "Parquet allow truncated timestamps option expects a boolean value, got: {}.",
            value.getDataType().toString())};
    }
    parquetOptionBuilder->allow_truncated_timestamps(value.getValue<bool>());
}

void GrapharExportOptions::setParquetStoreSchema(common::Value& value) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::BOOL) {
        throw common::RuntimeException{
            common::stringFormat("Parquet store schema option expects a boolean value, got: {}.",
                value.getDataType().toString())};
    }
    parquetOptionBuilder->store_schema(value.getValue<bool>());
}

GrapharExportOptions::GrapharExportOptions(case_insensitive_map_t<common::Value> parsingOptions) {
    wopt = std::make_shared<WriterOptions>();

    // get CSV/Parquet/ORC writer options
    csvOptionBuilder = std::make_shared<WriterOptions::CSVOptionBuilder>(wopt);
    parquetOptionBuilder = std::make_shared<WriterOptions::ParquetOptionBuilder>(wopt);
    orcOptionBuilder = std::make_shared<WriterOptions::ORCOptionBuilder>(wopt);

    for (auto& [name, value] : parsingOptions) {
        if (name == "CSV-NULL_STRING") {
            setCSVNullString(value);
        } else if (name == "CSV-DELIMITER") {
            setCSVDelimiter(value);
        } else if (name == "CSV-INCLUDE_HEADER") {
            setCSVIncludeHeader(value);
        } else if (name == "CSV-BATCH_SIZE") {
            setCSVBatchSize(value);
        } else if (name == "CSV-EOL") {
            setCSVEOL(value);
        } else if (name == "CSV-QUOTING_STYLE") {
            setCSVQuotingStyle(value);
        } else if (name == "PARQUET-DICTIONARY_PAGESIZE_LIMIT") {
            setParquetDictionaryPageSizeLimit(value);
        } else if (name == "PARQUET-WRITE_BATCH_SIZE") {
            setParquetWriteBatchSize(value);
        } else if (name == "PARQUET-MAX_ROW_GROUP_LENGTH") {
            setParquetMaxRowGroupLength(value);
        } else if (name == "PARQUET-DATA_PAGESIZE") {
            setParquetDataPageSize(value);
        } else if (name == "PARQUET-MAX_STATISTICS_SIZE") {
            setParquetMaxStatisticsSize(value);
        } else if (name == "PARQUET-COMPRESSION_LEVEL") {
            setParquetCompressionLevel(value);
        } else if (name == "PARQUET-DATA_PAGE_VERSION") {
            setParquetDataPageVersion(value);
        } else if (name == "PARQUET-VERSION") {
            setParquetVersion(value);
        } else if (name == "PARQUET-ENCODING") {
            setParquetEncoding(value);
        } else if (name == "PARQUET-COMPRESSION") {
            setParquetCompression(value);
        } else if (name == "PARQUET-COERCE_TIMESTAMPS") {
            setParquetCoerceTimestamps(value);
        } else if (name == "PARQUET-ENABLE_DICTIONARY") {
            setParquetEnableDictionary(value);
        } else if (name == "PARQUET-ENABLE_STATISTICS") {
            setParquetEnableStatistics(value);
        } else if (name == "PARQUET-ENABLE_STORE_DECIMAL_AS_INTEGER") {
            setParquetEnableStoreDecimalAsInteger(value);
        } else if (name == "PARQUET-ENABLE_WRITE_PAGE_INDEX") {
            setParquetEnableWritePageIndex(value);
        } else if (name == "PARQUET-COMPLIANT_NESTED_TYPES") {
            setParquetCompliantNestedTypes(value);
        } else if (name == "PARQUET-USE_THREADS") {
            setParquetUseThreads(value);
        } else if (name == "PARQUET-ENABLE_DEPRECATED_INT96_TIMESTAMPS") {
            setParquetEnableDeprecatedInt96Timestamps(value);
        } else if (name == "PARQUET-ALLOW_TRUNCATED_TIMESTAMPS") {
            setParquetAllowTruncatedTimestamps(value);
        } else if (name == "PARQUET-STORE_SCHEMA") {
            setParquetStoreSchema(value);
        }
    }

    csvOptionBuilder->build();
    parquetOptionBuilder->build();
    orcOptionBuilder->build();
}

ExportGrapharBindData::ExportGrapharBindData(std::vector<std::string> columnNames,
    const std::string& fileName, GrapharExportOptions grapharExportOptions, std::string tableName,
    std::string targetDir, ValidateLevel validateLevel)
    : ExportFuncBindData(std::move(columnNames), std::move(fileName)),
      exportOptions(std::move(grapharExportOptions)), tableName(std::move(tableName)),
      targetDir(std::move(targetDir)), validateLevel(validateLevel) {
    auto absolute_path = this->fileName;
    // Load graph info from the file path
    graphInfo = graphar::GraphInfo::Load(absolute_path).value();
    if (!graphInfo) {
        throw BinderException("GraphAr's GraphInfo could not be loaded from " + absolute_path);
    }
}

std::unique_ptr<ExportFuncBindData> bindFunc(ExportFuncBindInput& bindInput) {
    GrapharExportOptions grapharExportOptions{bindInput.parsingOptions};
    // get table name.
    auto table_it = bindInput.parsingOptions.find("TABLE_NAME");
    if (table_it != bindInput.parsingOptions.end()) {
        if (table_it->second.getDataType().getLogicalTypeID() != LogicalTypeID::STRING) {
            throw common::RuntimeException{
                common::stringFormat("Table name option expects a string value, got: {}.",
                    table_it->second.getDataType().toString())};
        }
    } else {
        throw BinderException("Table name must be specified in the parsing options with key "
                              "'TABLE_NAME' for GraphAr export.");
    }
    std::string tableName = table_it->second.getValue<std::string>();

    // get target directory.
    std::string targetDir = DEFAULT_TARGET_DIR;
    auto dir_it = bindInput.parsingOptions.find("TARGET_DIR");
    if (dir_it != bindInput.parsingOptions.end()) {
        if (dir_it->second.getDataType().getLogicalTypeID() != LogicalTypeID::STRING) {
            throw common::RuntimeException{
                common::stringFormat("Target directory option expects a string value, got: {}.",
                    dir_it->second.getDataType().toString())};
        }
        targetDir = dir_it->second.getValue<std::string>();
    }

    // get validate level.
    ValidateLevel validateLevel = ValidateLevel::strong_validate;
    auto validate_it = bindInput.parsingOptions.find("VALIDATE_LEVEL");
    if (validate_it != bindInput.parsingOptions.end()) {
        if (validate_it->second.getDataType().getLogicalTypeID() != LogicalTypeID::STRING) {
            throw common::RuntimeException{
                common::stringFormat("Validate level option expects a string value, got: {}.",
                    validate_it->second.getDataType().toString())};
        }
        auto strVal = common::StringUtils::getUpper(validate_it->second.getValue<std::string>());
        if (strVal == "DEFAULT") {
            validateLevel = ValidateLevel::default_validate;
        } else if (strVal == "NO") {
            validateLevel = ValidateLevel::no_validate;
        } else if (strVal == "WEAK") {
            validateLevel = ValidateLevel::weak_validate;
        } else if (strVal == "STRONG") {
            validateLevel = ValidateLevel::strong_validate;
        } else {
            throw common::RuntimeException{common::stringFormat(
                "Unrecognized validate level option: {}.", validate_it->second.toString())};
        }
    }

    // convert '.../ldbc_sample.graph.yml.graphar' to '.../ldbc_sample.graph.yml'.
    getYamlNameWithoutGrapharLabelInPlace(bindInput.filePath);
    return std::make_unique<ExportGrapharBindData>(bindInput.columnNames, bindInput.filePath,
        grapharExportOptions, tableName, targetDir, validateLevel);
}

} // namespace graphar_extension
} // namespace kuzu