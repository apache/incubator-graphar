#include "function/graphar_scan.h"

namespace kuzu {
namespace graphar_extension {

using namespace function;
using namespace common;

GrapharScanSharedState::GrapharScanSharedState(
    graphar::Result<std::shared_ptr<graphar::VerticesCollection>> maybeVerticesCollection,
    graphar::Result<std::shared_ptr<graphar::EdgesCollection>> maybeEdgesCollection,
    graphar::Result<std::shared_ptr<graphar::VerticesCollection>> maybeFromVerticesCollection,
    graphar::Result<std::shared_ptr<graphar::VerticesCollection>> maybeToVerticesCollection,
    uint64_t max_threads, bool is_edge)
    : maybe_vertices_collection{std::move(maybeVerticesCollection)},
      maybe_edges_collection{std::move(maybeEdgesCollection)},
      maybe_from_vertices_collection{std::move(maybeFromVerticesCollection)},
      maybe_to_vertices_collection{std::move(maybeToVerticesCollection)} {
    if (!is_edge) {
        collection_count = maybe_vertices_collection.value()->size();
    } else {
        collection_count = maybe_edges_collection.value()->size();
    }
    KU_ASSERT(collection_count != 0);
    batch_size = max_threads % collection_count == 0 ?
                     std::max<size_t>(1, collection_count / max_threads) :
                     std::max<size_t>(1, collection_count / max_threads + 1);
    batch_size = batch_size > DEFAULT_VECTOR_CAPACITY ? DEFAULT_VECTOR_CAPACITY : batch_size;
    next_index.store(0);

    // If it is edge, pre-compute the starting point of each batch's EdgeIter in the init stage.
    if (is_edge && maybe_edges_collection.has_value()) {
        auto edges = maybe_edges_collection.value();
        size_t num_batches = (collection_count + batch_size - 1) / batch_size;
        edge_batch_iters.reserve(num_batches);

        // Single sequential scan: Walk from begin() to end(), and push_back a copy at the first
        // index of each batch.
        auto iter = edges->begin();
        for (size_t idx = 0; idx < collection_count; ++idx) {
            if (idx % batch_size == 0) {
                edge_batch_iters.emplace_back(iter); // copy constructor
            }
            ++iter;
        }
    }
}

std::unique_ptr<TableFuncSharedState> initGrapharScanSharedState(
    const TableFuncInitSharedStateInput& input) {
    auto grapharScanBindData = input.bindData->constPtrCast<GrapharScanBindData>();
    graphar::Result<std::shared_ptr<graphar::VerticesCollection>> maybe_vertices_collection = {};
    graphar::Result<std::shared_ptr<graphar::EdgesCollection>> maybe_edges_collection = {};
    graphar::Result<std::shared_ptr<graphar::VerticesCollection>> maybe_from_vertices_collection =
        {};
    graphar::Result<std::shared_ptr<graphar::VerticesCollection>> maybe_to_vertices_collection = {};

    if (!grapharScanBindData->is_edge) {
        maybe_vertices_collection = graphar::VerticesCollection::Make(
            grapharScanBindData->graph_info, grapharScanBindData->table_name);
    } else {
        // parse table_name into src.edge.dst
        std::string src, edge, dst;
        if (!tryParseEdgeTableName(grapharScanBindData->table_name, src, edge, dst)) {
            throw BinderException("Edge table name " + grapharScanBindData->table_name +
                                  " is invalid. It should be in the format of "
                                  "<source>_<edge>_<destination>.");
        }

        maybe_edges_collection = graphar::EdgesCollection::Make(grapharScanBindData->graph_info,
            src, edge, dst, graphar::AdjListType::ordered_by_source);
        maybe_from_vertices_collection =
            graphar::VerticesCollection::Make(grapharScanBindData->graph_info, src);
        maybe_to_vertices_collection =
            graphar::VerticesCollection::Make(grapharScanBindData->graph_info, dst);
    }
    return std::make_unique<graphar_extension::GrapharScanSharedState>(
        std::move(maybe_vertices_collection), std::move(maybe_edges_collection),
        std::move(maybe_from_vertices_collection), std::move(maybe_to_vertices_collection),
        grapharScanBindData->max_threads, grapharScanBindData->is_edge);
}

offset_t tableFunc(const TableFuncInput& input, TableFuncOutput& output) {
    auto grapharSharedState =
        input.sharedState->ptrCast<graphar_extension::GrapharScanSharedState>();
    auto grapharScanBindData =
        input.bindData->constPtrCast<graphar_extension::GrapharScanBindData>();

    if (!grapharScanBindData->is_edge) {
        auto& column_setters = grapharScanBindData->vertex_column_setters;
        auto vertices = grapharSharedState->maybe_vertices_collection.value();
        size_t vertices_count = grapharSharedState->collection_count;
        size_t batch_size = grapharSharedState->batch_size;

        // Reserve a batch of indices atomically
        size_t start =
            grapharSharedState->next_index.fetch_add(batch_size, std::memory_order_relaxed);
        if (start >= vertices_count) {
            // no more rows
            output.dataChunk.state->getSelVectorUnsafe().setSelSize(0);
            return 0;
        }
        size_t end = std::min(start + batch_size, vertices_count);

        idx_t count = 0;
        // create a local iterator for this index (each worker/thread has its own local iterator)
        auto it = vertices->begin() + start;
        for (size_t idx = start; idx < end; ++idx) {
            // Complete all column writes using the setter generated in the bind stage (without
            // switch)
            for (size_t ci = 0; ci < column_setters.size(); ++ci) {
                column_setters[ci](it, output, count);
            }
            ++it;
            count++;
        }

        output.dataChunk.state->getSelVectorUnsafe().setSelSize(count);
        return output.dataChunk.state->getSelVector().getSelSize();
    } else {
        auto& column_names = grapharScanBindData->column_names;
        auto& column_setters = grapharScanBindData->edge_column_setters;
        auto edges = grapharSharedState->maybe_edges_collection.value();
        size_t edges_count = grapharSharedState->collection_count;
        size_t batch_size = grapharSharedState->batch_size;

        // Reserve a batch of indices atomically
        size_t start =
            grapharSharedState->next_index.fetch_add(batch_size, std::memory_order_relaxed);
        if (start >= edges_count) {
            // no more rows
            output.dataChunk.state->getSelVectorUnsafe().setSelSize(0);
            return 0;
        }
        size_t end = std::min(start + batch_size, edges_count);

        // Use the pre-computed batch start iter in SharedState to avoid advancing from the
        // beginning each time.
        size_t batch_id = start / batch_size;
        KU_ASSERT(batch_id < grapharSharedState->edge_batch_iters.size());

        idx_t count = 0;
        auto from_vertices = grapharSharedState->maybe_from_vertices_collection.value();
        auto to_vertices = grapharSharedState->maybe_to_vertices_collection.value();

        auto it = grapharSharedState->edge_batch_iters[batch_id];
        for (size_t idx = start; idx < end; ++idx) {
            for (size_t ci = 0; ci < column_setters.size(); ++ci) {
                if (StringUtils::caseInsensitiveEquals(column_names[ci], FROM_COL_NAME) ||
                    StringUtils::caseInsensitiveEquals(column_names[ci], INTERNAL_FROM_COL_NAME)) {
                    column_setters[ci](it, output, count, from_vertices); // from setter
                } else if (StringUtils::caseInsensitiveEquals(column_names[ci], TO_COL_NAME) ||
                           StringUtils::caseInsensitiveEquals(column_names[ci],
                               INTERNAL_TO_COL_NAME)) {
                    column_setters[ci](it, output, count, to_vertices); // to setter
                } else {
                    column_setters[ci](it, output, count, nullptr); // other setter
                }
            }
            ++it;
            count++;
        }

        output.dataChunk.state->getSelVectorUnsafe().setSelSize(count);
        return output.dataChunk.state->getSelVector().getSelSize();
    }
}

function_set GrapharScanFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name, std::vector{LogicalTypeID::STRING});
    function->tableFunc = tableFunc;
    function->bindFunc = bindFunc;
    function->initSharedStateFunc = initGrapharScanSharedState;
    function->initLocalStateFunc = TableFunction::initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace graphar_extension
} // namespace kuzu
