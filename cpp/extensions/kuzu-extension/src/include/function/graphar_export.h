#pragma once

#include <optional>
#include <variant>

#include "common/exception/binder.h"
#include "common/exception/not_implemented.h"
#include "common/exception/runtime.h"
#include "common/string_utils.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "function/export/export_function.h"
#include "function/function.h"
#include "function/table/bind_data.h"
#include "graphar/api/high_level_reader.h"
#include "graphar/api/high_level_writer.h"
#include "graphar/writer_util.h"
#include "main/client_context.h"
#include "utils/graphar_utils.h"

namespace kuzu {
namespace graphar_extension {

using namespace kuzu::function;
using namespace kuzu::common;
using namespace graphar;

// Scalar is a single atomic value type.
using Scalar = std::variant<int64_t, int32_t, double, float, std::string, bool>;

// Prop can be empty (monostate), a single scalar, or a list of scalars.
using Prop = std::variant<std::monostate, Scalar, std::vector<Scalar>>;

// schema metadata
struct PropMeta {
    std::string name;
    Type type;
    Cardinality card;
};

// row structure: store properties by schema index
struct WriteRow {
    std::vector<Prop> props; // length == schema.size()

    WriteRow(size_t n) : props(n) {}
};

// helper: check whether a Scalar holds the exact Type (strict match)
inline bool scalarMatchesType(const Scalar& scalar, Type type) {
    switch (type) {
    case Type::INT64:
    case Type::TIMESTAMP:
        return std::holds_alternative<int64_t>(scalar);
    case Type::INT32:
    case Type::DATE:
        return std::holds_alternative<int32_t>(scalar);
    case Type::DOUBLE:
        return std::holds_alternative<double>(scalar);
    case Type::FLOAT:
        return std::holds_alternative<float>(scalar);
    case Type::STRING:
        return std::holds_alternative<std::string>(scalar);
    case Type::BOOL:
        return std::holds_alternative<bool>(scalar);
    default:
        throw common::NotImplementedException{"scalarMatchesType not implemented for type"};
    }
    return false;
}

// Convert a Scalar to a human-readable string for debugging.
inline std::string scalarToString(const Scalar& s) {
    return std::visit(
        [](auto&& v) -> std::string {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, std::string>)
                return v;
            else if constexpr (std::is_same_v<T, bool>)
                return v ? "true" : "false";
            else {
                std::ostringstream oss;
                oss << v;
                return oss.str();
            }
        },
        s);
}

// Manage schema + row collection
class WriteRowsBuffer {
public:
    // Construct with schema. Build name -> index map.
    WriteRowsBuffer(const std::vector<PropMeta>& schema_) : schema(schema_) {
        for (size_t i = 0; i < schema.size(); ++i)
            name_to_idx[schema[i].name] = i;
    }

    // Create a new empty row and return its index.
    size_t NewRow() {
        rows.emplace_back(schema.size());
        return rows.size() - 1;
    }

    // Set a single scalar property for a row. Enforce cardinality and type.
    void SetProperty(size_t row_idx, const std::string& name, const Scalar& scalar) {
        size_t idx = idxOf(name);
        const auto& meta = schema[idx];
        if (meta.card != Cardinality::SINGLE)
            throw common::RuntimeException(
                common::stringFormat("property '{}' is not SCALAR", name));
        if (!scalarMatchesType(scalar, meta.type))
            throw common::RuntimeException(
                common::stringFormat("type mismatch for property '{}'", name));
        rows.at(row_idx).props[idx] = scalar;
    }

    // Append a single element into a list-valued property.
    void AddListElement(size_t row_idx, const std::string& name, const Scalar& scalar) {
        size_t idx = idxOf(name);
        const auto& meta = schema[idx];
        if (meta.card != Cardinality::LIST)
            throw common::RuntimeException(
                common::stringFormat("Property '{}' is not LIST.", name));

        if (!scalarMatchesType(scalar, meta.type))
            throw common::RuntimeException(
                common::stringFormat("Type mismatch for list element of property '{}'.", name));

        auto& cell = rows.at(row_idx).props[idx];
        if (std::holds_alternative<std::monostate>(cell)) {
            cell = std::vector<Scalar>{}; // initialize list
        }
        auto& vec = std::get<std::vector<Scalar>>(cell);
        vec.push_back(scalar);
    }

    // Return number of rows currently stored.
    size_t RowCount() const noexcept { return rows.size(); }

    // Return a const reference to the schema vector.
    const std::vector<PropMeta>& Schema() const noexcept { return schema; }

    // Return a const reference to an entire row. Throws if index OOB.
    const WriteRow& GetRow(size_t row_idx) const {
        if (row_idx >= rows.size())
            throw common::RuntimeException{
                common::stringFormat("row index {} out of range.", row_idx)};
        return rows.at(row_idx);
    }

    // Return rows
    const std::vector<WriteRow>& GetRows() const noexcept { return rows; }

    // Return a scalar property by row index and property name.
    // If the property is absent or null, return std::nullopt.
    // If the property exists but is not SINGLE, throw.
    std::optional<Scalar> GetScalarProperty(size_t row_idx, const std::string& name) const {
        size_t idx = idxOf(name);
        const auto& meta = schema[idx];
        if (meta.card != Cardinality::SINGLE)
            throw common::RuntimeException{
                common::stringFormat("property '{}' is not SINGLE.", name)};
        const auto& cell = rows.at(row_idx).props[idx];
        if (std::holds_alternative<std::monostate>(cell))
            return std::nullopt;
        if (!std::holds_alternative<Scalar>(cell))
            throw common::RuntimeException{
                common::stringFormat("internal data error: '{}' not SINGLE.", name)};
        return std::get<Scalar>(cell);
    }

    // Return a list property by row index and property name.
    // If the property is absent or null, returns empty vector (not nullopt).
    // If the property exists but is not LIST, throw.
    std::vector<Scalar> GetListProperty(size_t row_idx, const std::string& name) const {
        size_t idx = idxOf(name);
        const auto& meta = schema[idx];
        if (meta.card != Cardinality::LIST)
            throw common::RuntimeException{
                common::stringFormat("property '{}' is not LIST.", name)};
        const auto& cell = rows.at(row_idx).props[idx];
        if (std::holds_alternative<std::monostate>(cell))
            return {};
        if (!std::holds_alternative<std::vector<Scalar>>(cell))
            throw common::RuntimeException{
                common::stringFormat("internal data error: '{}' not LIST.", name)};
        return std::get<std::vector<Scalar>>(cell);
    }

    // Return whether a given property in a row is unset/null.
    bool IsNullProperty(size_t row_idx, const std::string& name) const {
        size_t idx = idxOf(name);
        const auto& cell = rows.at(row_idx).props[idx];
        return std::holds_alternative<std::monostate>(cell);
    }

private:
    std::vector<PropMeta> schema;
    case_insensitive_map_t<size_t> name_to_idx;
    std::vector<WriteRow> rows;

    // Resolve property name to schema index. Throws if not found.
    size_t idxOf(const std::string& name) const {
        auto it = name_to_idx.find(name);
        if (it == name_to_idx.end())
            throw common::RuntimeException{common::stringFormat("No such property: {}.", name)};
        return it->second;
    }
};

struct GrapharExportFunction {
    static constexpr const char* name = "COPY_GRAPHAR";

    static function_set getFunctionSet();
};

enum class GrapharWriterFormat { CSV, PARQUET /*, ORC*/, ALL };

struct GrapharExportOptions {
    std::shared_ptr<WriterOptions> wopt;

    std::shared_ptr<WriterOptions::CSVOptionBuilder> csvOptionBuilder;

    std::shared_ptr<WriterOptions::ParquetOptionBuilder> parquetOptionBuilder;

    std::shared_ptr<WriterOptions::ORCOptionBuilder> orcOptionBuilder;

    explicit GrapharExportOptions() = delete;

    explicit GrapharExportOptions(case_insensitive_map_t<common::Value> parsingOptions);

    // CSV write options
    void setCSVNullString(common::Value& value);

    void setCSVDelimiter(common::Value& value);

    void setCSVIncludeHeader(common::Value& value);

    void setCSVBatchSize(common::Value& value);

    void setCSVEOL(common::Value& value);

    void setCSVQuotingStyle(common::Value& value);

    // Parquet write options
    void setParquetDictionaryPageSizeLimit(common::Value& value);

    void setParquetWriteBatchSize(common::Value& value);

    void setParquetMaxRowGroupLength(common::Value& value);

    void setParquetDataPageSize(common::Value& value);

    void setParquetMaxStatisticsSize(common::Value& value);

    void setParquetCompressionLevel(common::Value& value);

    void setParquetDataPageVersion(common::Value& value);

    void setParquetVersion(common::Value& value);

    void setParquetEncoding(common::Value& value);

    void setParquetCompression(common::Value& value);

    void setParquetCoerceTimestamps(common::Value& value);

    void setParquetEnableDictionary(common::Value& value);

    void setParquetEnableStatistics(common::Value& value);

    void setParquetEnableStoreDecimalAsInteger(common::Value& value);

    void setParquetEnableWritePageIndex(common::Value& value);

    void setParquetCompliantNestedTypes(common::Value& value);

    void setParquetUseThreads(common::Value& value);

    void setParquetEnableDeprecatedInt96Timestamps(common::Value& value);

    void setParquetAllowTruncatedTimestamps(common::Value& value);

    void setParquetStoreSchema(common::Value& value);
};

struct ExportGrapharBindData : public ExportFuncBindData {
    GrapharExportOptions exportOptions;
    std::shared_ptr<GraphInfo> graphInfo;
    std::string tableName;
    std::string targetDir;
    ValidateLevel validateLevel;

    ExportGrapharBindData(std::vector<std::string> columnNames, const std::string& fileName,
        GrapharExportOptions grapharWriterOptions, std::string tableName, std::string targetDir,
        ValidateLevel validateLevel);

    std::unique_ptr<ExportFuncBindData> copy() const override {
        return std::make_unique<ExportGrapharBindData>(columnNames, fileName, exportOptions,
            tableName, targetDir, validateLevel);
    }
};

struct ExportGrapharSharedState : public ExportFuncSharedState {
    // mutex for thread safety in combineFunc.
    std::mutex mtx;

    // vertex info and builder.
    std::shared_ptr<VertexInfo> vertexInfo;
    std::shared_ptr<builder::VerticesBuilder> verticesBuilder;

    // edge info and builder.
    std::shared_ptr<EdgeInfo> edgeInfo;
    std::shared_ptr<builder::EdgesBuilder> edgesBuilder;

    // determine if export vertex or edge.
    bool is_edge = false;

    void init(main::ClientContext& context, const ExportFuncBindData& bindData) override;
};

struct ExportGrapharLocalState : public ExportFuncLocalState {
    std::shared_ptr<WriteRowsBuffer> buffer = nullptr;

    ExportGrapharLocalState() = default;

    explicit ExportGrapharLocalState(const std::vector<PropMeta>& schema)
        : buffer(std::make_shared<WriteRowsBuffer>(schema)) {}
};

std::unique_ptr<ExportFuncBindData> bindFunc(ExportFuncBindInput& bindInput);

void initSharedState(ExportFuncSharedState& sharedState, main::ClientContext& context,
    const ExportFuncBindData& bindData);

std::shared_ptr<ExportFuncSharedState> createSharedStateFunc();

std::unique_ptr<ExportFuncLocalState> initLocalState(main::ClientContext&,
    const ExportFuncBindData&, std::vector<bool>);

void sinkFunc(ExportFuncSharedState&, ExportFuncLocalState& localState,
    const ExportFuncBindData& bindData, std::vector<std::shared_ptr<ValueVector>> inputVectors);

void combineFunc(ExportFuncSharedState& sharedState, ExportFuncLocalState& localState);

void finalizeFunc(ExportFuncSharedState& sharedState);

} // namespace graphar_extension
} // namespace kuzu
