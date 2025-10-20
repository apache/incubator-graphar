#pragma once

#include <mutex>

#include "binder/binder.h"
#include "common/case_insensitive_map.h"
#include "common/copy_constructors.h"
#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "common/string_utils.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "function/table/bind_data.h"
#include "function/table/bind_input.h"
#include "function/table/scan_file_function.h"
#include "function/table/table_function.h"
#include "graphar/api/high_level_reader.h"
#include "graphar/types.h"
#include "main/client_context.h"
#include "utils/graphar_utils.h"

namespace kuzu {
namespace graphar_extension {

using namespace kuzu::function;
using namespace kuzu::common;

#define METADATA_SEPARATOR "|"

enum class GrapharMetadataType {
    GRAPH,
    VERTEX,
    EDGE,
};

struct GrapharMetadataFunction {
    static constexpr const char* name = "GRAPHAR_METADATA";

    static function_set getFunctionSet();
};

struct GrapharMetadataBindData final : function::ScanFileBindData {
    std::shared_ptr<graphar::GraphInfo> graph_info;
    GrapharMetadataType metadata_type;
    std::string table_name;
    std::vector<LogicalType> column_types;
    std::vector<std::string> column_names;
    std::shared_ptr<graphar::VertexInfo> vertex_info;
    std::shared_ptr<graphar::EdgeInfo> edge_info;

    GrapharMetadataBindData(binder::expression_vector columns, main::ClientContext* context,
        std::shared_ptr<graphar::GraphInfo> graph_info, GrapharMetadataType metadata_type,
        std::string table_name, std::vector<LogicalType> column_types,
        std::vector<std::string> column_names, std::shared_ptr<graphar::VertexInfo> vertex_info,
        std::shared_ptr<graphar::EdgeInfo> edge_info)
        : ScanFileBindData{std::move(columns), 0 /* numRows */, FileScanInfo{}, context},
          graph_info{std::move(graph_info)}, metadata_type{metadata_type},
          table_name{std::move(table_name)}, column_types{std::move(column_types)},
          column_names{std::move(column_names)}, vertex_info{std::move(vertex_info)},
          edge_info{std::move(edge_info)} {}

    GrapharMetadataBindData(const GrapharMetadataBindData& other)
        : ScanFileBindData{other}, graph_info{other.graph_info}, metadata_type{other.metadata_type},
          table_name{other.table_name}, column_types{copyVector(other.column_types)},
          column_names{other.column_names}, vertex_info{other.vertex_info},
          edge_info{other.edge_info} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<GrapharMetadataBindData>(*this);
    }
};

struct GrapharMetadataSharedState : public TableFuncSharedState {
    // table func can be called once only.
    bool finished;

    explicit GrapharMetadataSharedState(bool finished) : finished(finished) {}
};

// Functions and structs exposed for use
std::unique_ptr<function::TableFuncSharedState> initGrapharMetadataSharedState(
    const function::TableFuncInitSharedStateInput& input);

std::unique_ptr<TableFuncBindData> metadataBindFunc(main::ClientContext* context,
    const TableFuncBindInput* input);

offset_t metadataTableFunc(const TableFuncInput& input, TableFuncOutput& output);

} // namespace graphar_extension
} // namespace kuzu
