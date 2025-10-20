#include "function/graphar_metadata.h"

#include <iostream>

using namespace kuzu::function;
using namespace kuzu::common;

namespace kuzu {
namespace graphar_extension {

std::unique_ptr<TableFuncBindData> metadataBindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    std::string absolute_path = input->getLiteralVal<std::string>(0);

    if (absolute_path.empty()) {
        throw BinderException("GraphAr metadata requires a valid graphInfo file path.");
    }

    auto optional_params = input->optionalParams;
    if (optional_params.find("vertex_type") != optional_params.end() &&
        optional_params.find("edge_type") != optional_params.end()) {
        throw BinderException("GraphAr metadata Cannot specify both vertex_type and edge_type.");
    }

    // Load graph info from the file path
    auto graph_info = graphar::GraphInfo::Load(absolute_path).value();
    if (!graph_info) {
        throw BinderException("GraphAr's GraphInfo could not be loaded from " + absolute_path);
    }
    auto vertex_infos = graph_info->GetVertexInfos();
    auto edge_infos = graph_info->GetEdgeInfos();
    std::shared_ptr<graphar::VertexInfo> vertex_info = nullptr;
    std::shared_ptr<graphar::EdgeInfo> edge_info = nullptr;

    GrapharMetadataType metadata_type;
    std::string table_name;
    if (optional_params.find("vertex_type") != optional_params.end()) {
        metadata_type = GrapharMetadataType::VERTEX;
        table_name = optional_params.at("vertex_type").strVal;
        for (const auto& v_info : vertex_infos) {
            if (v_info->GetType() == table_name) {
                vertex_info = v_info;
                goto METADATA_TYPE_DEDUCED;
            }
        }
        throw BinderException("Vertex type " + table_name + " not found in the GraphAr graph.");
    } else if (optional_params.find("edge_type") != optional_params.end()) {
        metadata_type = GrapharMetadataType::EDGE;
        table_name = optional_params.at("edge_type").strVal;
        for (const auto& e_info : edge_infos) {
            std::string src_type = e_info->GetSrcType();
            std::string edge_type = e_info->GetEdgeType();
            std::string dst_type = e_info->GetDstType();
            std::string full_edge_name =
                src_type + REGULAR_SEPARATOR + edge_type + REGULAR_SEPARATOR + dst_type;
            if (full_edge_name == table_name) {
                edge_info = e_info;
                goto METADATA_TYPE_DEDUCED;
            }
        }
        throw BinderException("Edge type " + table_name + " not found in the GraphAr graph.");
    } else {
        metadata_type = GrapharMetadataType::GRAPH;
    }

METADATA_TYPE_DEDUCED:
    std::vector<LogicalType> column_types;
    std::vector<std::string> column_names;

    // Wanted metadata of the graph
    switch (metadata_type) {
    case GrapharMetadataType::GRAPH: {
        column_names.push_back("graph_name");
        column_types.push_back(LogicalType::STRING());
        column_names.push_back("vertex_type_num");
        column_types.push_back(LogicalType::INT32());
        column_names.push_back("edge_types_num");
        column_types.push_back(LogicalType::INT32());
        column_names.push_back("vertex_types");
        column_types.push_back(LogicalType::LIST(LogicalType::STRING()));
        column_names.push_back("edge_types");
        column_types.push_back(LogicalType::LIST(LogicalType::STRING()));
        column_names.push_back("prefix");
        column_types.push_back(LogicalType::STRING());
        column_names.push_back("version");
        column_types.push_back(LogicalType::STRING());
        break;
    }
    case GrapharMetadataType::VERTEX: {
        column_names.push_back("vertex_type");
        column_types.push_back(LogicalType::STRING());
        column_names.push_back("vertex_count");
        column_types.push_back(LogicalType::INT64());
        column_names.push_back("property_names|types|is_primary");
        column_types.push_back(LogicalType::LIST(LogicalType::STRING()));
        column_names.push_back("chunk_size");
        column_types.push_back(LogicalType::INT64());
        column_names.push_back("prefix");
        column_types.push_back(LogicalType::STRING());
        column_names.push_back("version");
        column_types.push_back(LogicalType::STRING());
        break;
    }
    case GrapharMetadataType::EDGE: {
        column_names.push_back("edge_type");
        column_types.push_back(LogicalType::STRING());
        column_names.push_back("src_type");
        column_types.push_back(LogicalType::STRING());
        column_names.push_back("dst_type");
        column_types.push_back(LogicalType::STRING());
        column_names.push_back("directed");
        column_types.push_back(LogicalType::BOOL());
        column_names.push_back("edge_count");
        column_types.push_back(LogicalType::INT64());
        column_names.push_back("property_names|types|is_primary");
        column_types.push_back(LogicalType::LIST(LogicalType::STRING()));
        column_names.push_back("chunk_size");
        column_types.push_back(LogicalType::INT64());
        column_names.push_back("src_chunk_size");
        column_types.push_back(LogicalType::INT64());
        column_names.push_back("dst_chunk_size");
        column_types.push_back(LogicalType::INT64());
        column_names.push_back("prefix");
        column_types.push_back(LogicalType::STRING());
        column_names.push_back("version");
        column_types.push_back(LogicalType::STRING());
        break;
    }
    default:
        throw BinderException("Unsupported GraphAr metadata type.");
    }

    column_names = TableFunction::extractYieldVariables(column_names, input->yieldVariables);
    auto columns = input->binder->createVariables(column_names, column_types);
    return std::make_unique<GrapharMetadataBindData>(std::move(columns), context,
        std::move(graph_info), metadata_type, table_name, std::move(column_types),
        std::move(column_names), std::move(vertex_info), std::move(edge_info));
}

std::unique_ptr<TableFuncSharedState> initGrapharMetadataSharedState(
    [[maybe_unused]] const TableFuncInitSharedStateInput& input) {
    return std::make_unique<GrapharMetadataSharedState>(false);
}

offset_t metadataTableFunc(const TableFuncInput& input, TableFuncOutput& output) {
    auto bindData = input.bindData->constPtrCast<GrapharMetadataBindData>();
    auto sharedState = input.sharedState->ptrCast<GrapharMetadataSharedState>();
    auto& table_name = bindData->table_name;
    auto& graph_info = bindData->graph_info;
    auto& vertex_info = bindData->vertex_info;
    auto& edge_info = bindData->edge_info;
    auto metadata_type = bindData->metadata_type;

    std::lock_guard<std::mutex> lock{sharedState->mtx};
    if (sharedState->finished) {
        output.dataChunk.state->getSelVectorUnsafe().setSelSize(0);
        return output.dataChunk.state->getSelVector().getSelSize();
    }
    sharedState->finished = true;

    // we come here only once.
    switch (metadata_type) {
    case GrapharMetadataType::GRAPH: {
        if (graph_info == nullptr) {
            throw BinderException("GraphAr's GraphInfo is null.");
        }

        // graph_name
        auto& graph_name_vec = output.dataChunk.getValueVectorMutable(0);
        graph_name_vec.setValue(0, graph_info->GetName());

        // vertex_type_num
        auto& vertex_type_num_vec = output.dataChunk.getValueVectorMutable(1);
        size_t vertex_type_num = graph_info->GetVertexInfos().size();
        vertex_type_num_vec.setValue(0, static_cast<int32_t>(vertex_type_num));

        // edge_types_num
        auto& edge_type_num_vec = output.dataChunk.getValueVectorMutable(2);
        size_t edge_type_num = graph_info->GetEdgeInfos().size();
        edge_type_num_vec.setValue(0, static_cast<int32_t>(edge_type_num));

        // vertex_types

        // Get the List value vector for the "vertex_types" column.
        // A ListVector stores, for each row, a list_entry_t {offset, length}
        // that points into a shared "data vector" containing the flattened elements.
        auto& vertex_types_vec = output.dataChunk.getValueVectorMutable(3);

        // Clear any previous auxiliary state (offsets/lengths, etc.) so we can
        // populate the list from scratch for this execution.
        vertex_types_vec.resetAuxiliaryBuffer();

        // Obtain the underlying data vector that holds all list elements
        // (for string lists this will be a StringVector). We must also reset
        // its auxiliary buffer to start writing fresh element entries.
        auto vertex_types_data_vec = ListVector::getDataVector(&vertex_types_vec);
        vertex_types_data_vec->resetAuxiliaryBuffer();

        // Retrieve the vertex info entries from graph_info and determine
        // how many vertex type strings we need to store.
        auto vertexInfos = graph_info->GetVertexInfos();
        size_t num_vertex_types = vertexInfos.size();

        // Reserve a contiguous slot in the data vector for this row's list
        // elements. addList returns a list_entry_t containing the starting
        // offset in the data vector and the length (num_vertex_types).
        // If num_vertex_types == 0, the returned entry will have length 0.
        auto resultList = ListVector::addList(&vertex_types_vec, num_vertex_types);

        // Associate the list_entry_t with the output row 0 so that the row
        // will reference the allocated slice in the shared data vector.
        vertex_types_vec.setValue<list_entry_t>(0, resultList);

        // Fill the reserved slice in the data vector with the per-vertex-type strings.
        // We write each string at index (resultList.offset + i).
        // Note: vertexInfos elements are accessed via pointer (->); adjust if they are objects.
        for (size_t i = 0; i < num_vertex_types; ++i) {
            // Convert the vertex type to std::string (or appropriate string view)
            // and append it into the shared data vector at the correct position.
            StringVector::addString(vertex_types_data_vec, resultList.offset + i,
                vertexInfos[i]->GetType());
        }

        // End of vertex_types population. The ListVector now has, for row 0,
        // an entry pointing to a contiguous range in vertex_types_data_vec
        // containing the vertex type strings.

        // edge_types
        auto& edge_types_vec = output.dataChunk.getValueVectorMutable(4);
        edge_types_vec.resetAuxiliaryBuffer();
        auto edge_types_data_vec = ListVector::getDataVector(&edge_types_vec);
        edge_types_data_vec->resetAuxiliaryBuffer();
        auto edgeInfos = graph_info->GetEdgeInfos();
        size_t num_edge_types = edgeInfos.size();
        auto edgeResultList = ListVector::addList(&edge_types_vec, num_edge_types);
        edge_types_vec.setValue<list_entry_t>(0, edgeResultList);
        for (size_t i = 0; i < num_edge_types; ++i) {
            StringVector::addString(edge_types_data_vec, edgeResultList.offset + i,
                edgeInfos[i]->GetSrcType() + REGULAR_SEPARATOR + edgeInfos[i]->GetEdgeType() +
                    REGULAR_SEPARATOR + edgeInfos[i]->GetDstType());
        }

        // prefix
        auto& prefix_vec = output.dataChunk.getValueVectorMutable(5);
        prefix_vec.setValue(0, graph_info->GetPrefix());

        // version
        auto& version_vec = output.dataChunk.getValueVectorMutable(6);
        version_vec.setValue(0, graph_info->version()->ToString());

        break;
    }
    case GrapharMetadataType::VERTEX: {
        if (graph_info == nullptr || vertex_info == nullptr) {
            throw BinderException("GraphAr's GraphInfo or VertexInfo is null.");
        }

        auto vertex_collection = VerticesCollection::Make(graph_info, table_name).value();

        // vertex_type
        auto& vertex_type_vec = output.dataChunk.getValueVectorMutable(0);
        vertex_type_vec.setValue(0, bindData->table_name);

        // vertex_count
        auto& vertex_count_vec = output.dataChunk.getValueVectorMutable(1);
        vertex_count_vec.setValue(0, static_cast<int64_t>(vertex_collection->size()));

        // property_names|types|is_primary
        auto& property_names_vec = output.dataChunk.getValueVectorMutable(2);
        property_names_vec.resetAuxiliaryBuffer();
        auto property_names_data_vec = ListVector::getDataVector(&property_names_vec);
        property_names_data_vec->resetAuxiliaryBuffer();
        const auto property_groups = vertex_info->GetPropertyGroups();
        size_t num_properties = property_groups.size();
        for (const auto& group : property_groups) {
            const auto& properties = group->GetProperties();
            num_properties += properties.size();
        }
        auto propertyResultList = ListVector::addList(&property_names_vec, num_properties);
        property_names_vec.setValue<list_entry_t>(0, propertyResultList);
        for (const auto& group : property_groups) {
            const auto& properties = group->GetProperties();
            for (const auto& property : properties) {
                std::string property_str = property.name + METADATA_SEPARATOR +
                                           property.type->ToTypeName() + METADATA_SEPARATOR +
                                           (property.is_primary ? "true" : "false");
                StringVector::addString(property_names_data_vec, propertyResultList.offset,
                    property_str);
                propertyResultList.offset++;
            }
        }

        // chunk_size
        auto& chunk_size_vec = output.dataChunk.getValueVectorMutable(3);
        chunk_size_vec.setValue(0, static_cast<int64_t>(vertex_info->GetChunkSize()));

        // prefix
        auto& prefix_vec = output.dataChunk.getValueVectorMutable(4);
        prefix_vec.setValue(0, graph_info->GetPrefix());

        // version
        auto& version_vec = output.dataChunk.getValueVectorMutable(5);
        version_vec.setValue(0, graph_info->version()->ToString());

        break;
    }
    case GrapharMetadataType::EDGE: {
        if (graph_info == nullptr || edge_info == nullptr) {
            throw BinderException("GraphAr's GraphInfo or EdgeInfo is null.");
        }

        auto edge_collection = EdgesCollection::Make(graph_info, edge_info->GetSrcType(),
            edge_info->GetEdgeType(), edge_info->GetDstType(), AdjListType::ordered_by_source)
                                   .value();

        // edge_type
        auto& edge_type_vec = output.dataChunk.getValueVectorMutable(0);
        edge_type_vec.setValue(0, bindData->table_name);

        // src_type
        auto& src_type_vec = output.dataChunk.getValueVectorMutable(1);
        src_type_vec.setValue(0, edge_info->GetSrcType());

        // dst_type
        auto& dst_type_vec = output.dataChunk.getValueVectorMutable(2);
        dst_type_vec.setValue(0, edge_info->GetDstType());

        // directed
        auto& directed_vec = output.dataChunk.getValueVectorMutable(3);
        directed_vec.setValue(0, edge_info->IsDirected());

        // edge_count
        auto& edge_count_vec = output.dataChunk.getValueVectorMutable(4);
        edge_count_vec.setValue(0, static_cast<int64_t>(edge_collection->size()));

        // property_names|types|is_primary
        auto& property_names_vec = output.dataChunk.getValueVectorMutable(5);
        property_names_vec.resetAuxiliaryBuffer();
        auto property_names_data_vec = ListVector::getDataVector(&property_names_vec);
        property_names_data_vec->resetAuxiliaryBuffer();
        const auto property_groups = edge_info->GetPropertyGroups();
        size_t num_properties = property_groups.size();
        for (const auto& group : property_groups) {
            const auto& properties = group->GetProperties();
            num_properties += properties.size();
        }
        auto propertyResultList = ListVector::addList(&property_names_vec, num_properties);
        property_names_vec.setValue<list_entry_t>(0, propertyResultList);
        for (const auto& group : property_groups) {
            const auto& properties = group->GetProperties();
            for (const auto& property : properties) {
                std::string property_str = property.name + METADATA_SEPARATOR +
                                           property.type->ToTypeName() + METADATA_SEPARATOR +
                                           (property.is_primary ? "true" : "false");
                StringVector::addString(property_names_data_vec, propertyResultList.offset,
                    property_str);
                propertyResultList.offset++;
            }
        }

        // chunk_size
        auto& chunk_size_vec = output.dataChunk.getValueVectorMutable(6);
        chunk_size_vec.setValue(0, static_cast<int64_t>(edge_info->GetChunkSize()));

        // src_chunk_size
        auto& src_chunk_size_vec = output.dataChunk.getValueVectorMutable(7);
        src_chunk_size_vec.setValue(0, static_cast<int64_t>(edge_info->GetSrcChunkSize()));

        // dst_chunk_size
        auto& dst_chunk_size_vec = output.dataChunk.getValueVectorMutable(8);
        dst_chunk_size_vec.setValue(0, static_cast<int64_t>(edge_info->GetDstChunkSize()));

        // prefix
        auto& prefix_vec = output.dataChunk.getValueVectorMutable(9);
        prefix_vec.setValue(0, graph_info->GetPrefix());

        // version
        auto& version_vec = output.dataChunk.getValueVectorMutable(10);
        version_vec.setValue(0, graph_info->version()->ToString());

        break;
    }
    default:
        throw BinderException("Unsupported GraphAr metadata type.");
    }

    output.dataChunk.state->getSelVectorUnsafe().setSelSize(1);
    return output.dataChunk.state->getSelVector().getSelSize();
}

function_set GrapharMetadataFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name, std::vector{LogicalTypeID::STRING});
    function->tableFunc = metadataTableFunc;
    function->bindFunc = metadataBindFunc;
    function->initSharedStateFunc = initGrapharMetadataSharedState;
    function->initLocalStateFunc = function::TableFunction::initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace graphar_extension
} // namespace kuzu
