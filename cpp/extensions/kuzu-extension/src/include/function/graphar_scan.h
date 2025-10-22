#pragma once

#include <atomic>
#include <functional>

#include "binder/binder.h"
#include "common/case_insensitive_map.h"
#include "common/copy_constructors.h"
#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "common/exception/not_implemented.h"
#include "common/string_utils.h"
#include "common/types/types.h"
#include "function/table/bind_data.h"
#include "function/table/bind_input.h"
#include "function/table/scan_file_function.h"
#include "function/table/table_function.h"
#include "graphar/api/high_level_reader.h"
#include "main/client_context.h"
#include "utils/graphar_utils.h"

namespace kuzu {
namespace graphar_extension {

struct GrapharScanFunction {
    static constexpr const char* name = "GRAPHAR_SCAN";

    static function::function_set getFunctionSet();
};

// Setter for vertex iterator
using VertexColumnSetter =
    std::function<void(graphar::VertexIter&, function::TableFuncOutput&, kuzu::common::idx_t)>;

// Setter for edge iterator
using EdgeColumnSetter = std::function<void(graphar::EdgeIter&, function::TableFuncOutput&,
    kuzu::common::idx_t, std::shared_ptr<graphar::VerticesCollection>)>;

using column_name_idx_map_t = std::unordered_map<std::string, uint64_t>;

class KuzuColumnInfo {
public:
    explicit KuzuColumnInfo(std::vector<std::string> columnNames);
    DELETE_COPY_DEFAULT_MOVE(KuzuColumnInfo);

    uint64_t getFieldIdx(std::string fieldName) const;

private:
    column_name_idx_map_t colNameToIdx;
    std::vector<std::string> colNames;
};

struct GrapharScanBindData final : function::ScanFileBindData {
    std::shared_ptr<graphar::GraphInfo> graph_info;
    std::shared_ptr<KuzuColumnInfo> column_info;
    std::string table_name;
    std::vector<std::string> column_names;
    std::vector<kuzu::common::LogicalType> column_types;
    std::vector<VertexColumnSetter> vertex_column_setters;
    std::vector<EdgeColumnSetter> edge_column_setters;
    bool is_edge = false;
    uint64_t max_threads;
    std::unordered_map<std::string, std::string> edges_from_to_mapping;

    uint64_t getFieldIdx(std::string fieldName) const {
        return column_info->getFieldIdx(fieldName);
    }

    GrapharScanBindData(binder::expression_vector columns, common::FileScanInfo fileScanInfo,
        main::ClientContext* context, std::shared_ptr<graphar::GraphInfo> graph_info,
        std::string table_name, std::vector<std::string> column_names,
        std::vector<kuzu::common::LogicalType> column_types, bool is_edge);

    GrapharScanBindData(const GrapharScanBindData& other)
        : ScanFileBindData(other), graph_info(other.graph_info), column_info(other.column_info),
          table_name(other.table_name), column_names(other.column_names),
          column_types(copyVector(other.column_types)),
          vertex_column_setters(other.vertex_column_setters),
          edge_column_setters(other.edge_column_setters), is_edge(other.is_edge),
          max_threads(other.max_threads) {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<GrapharScanBindData>(*this);
    }
};

struct GrapharScanSharedState : public function::TableFuncSharedState {
    graphar::Result<std::shared_ptr<graphar::VerticesCollection>> maybe_vertices_collection;
    graphar::Result<std::shared_ptr<graphar::EdgesCollection>> maybe_edges_collection;
    graphar::Result<std::shared_ptr<graphar::VerticesCollection>> maybe_from_vertices_collection;
    graphar::Result<std::shared_ptr<graphar::VerticesCollection>> maybe_to_vertices_collection;
    std::atomic<size_t> next_index{0};
    size_t collection_count;
    size_t batch_size;
    std::vector<graphar::EdgeIter> edge_batch_iters; // if is_edge, store batch start iterators

    GrapharScanSharedState(
        graphar::Result<std::shared_ptr<graphar::VerticesCollection>> maybeVerticesCollection,
        graphar::Result<std::shared_ptr<graphar::EdgesCollection>> maybeEdgesCollection,
        graphar::Result<std::shared_ptr<graphar::VerticesCollection>>
            maybe_from_vertices_collection,
        graphar::Result<std::shared_ptr<graphar::VerticesCollection>> maybe_to_vertices_collection,
        uint64_t max_threads, bool is_edge);
};

// Functions and structs exposed for use
std::unique_ptr<function::TableFuncSharedState> initGrapharScanSharedState(
    const function::TableFuncInitSharedStateInput& input);

std::unique_ptr<function::TableFuncBindData> bindFunc(main::ClientContext* context,
    const function::TableFuncBindInput* input);

common::offset_t tableFunc(const function::TableFuncInput& input,
    function::TableFuncOutput& output);

} // namespace graphar_extension
} // namespace kuzu
