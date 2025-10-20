#include "common/exception/not_implemented.h"
#include "function/graphar_scan.h"

namespace kuzu {
namespace graphar_extension {

using namespace function;
using namespace common;

// Vertex setter maker for properties
template<typename T>
VertexColumnSetter makeTypedVertexSetter(uint64_t fieldIdx, std::string colName) {
    return [fieldIdx, colName = std::move(colName)](graphar::VertexIter& it,
               function::TableFuncOutput& output, kuzu::common::idx_t row) {
        auto res = it.property<T>(colName);
        auto& vec = output.dataChunk.getValueVectorMutable(fieldIdx);
        vec.setValue(row, res.value());
    };
}

// Vertex setter maker for internal_id
VertexColumnSetter makeInternalIdVertexSetter(uint64_t fieldIdx, std::string colName) {
    return [fieldIdx, colName = std::move(colName)](graphar::VertexIter& it,
               function::TableFuncOutput& output, kuzu::common::idx_t row) {
        auto id = it.id();
        auto& vec = output.dataChunk.getValueVectorMutable(fieldIdx);
        vec.setValue(row, static_cast<int64_t>(id));
    };
}

template<>
VertexColumnSetter makeTypedVertexSetter<list_entry_t>([[maybe_unused]] uint64_t fieldIdx,
    [[maybe_unused]] std::string colName) {
    throw NotImplementedException("List type is not supported in graphar scan.");
}

// Edge setter maker for properties
template<typename T>
EdgeColumnSetter makeTypedEdgeSetter(uint64_t fieldIdx, std::string colName) {
    return [fieldIdx, colName = std::move(colName)](graphar::EdgeIter& it,
               function::TableFuncOutput& output, kuzu::common::idx_t row,
               [[maybe_unused]] std::shared_ptr<graphar::VerticesCollection> unused) {
        auto res = it.property<T>(colName);
        auto& vec = output.dataChunk.getValueVectorMutable(fieldIdx);
        vec.setValue(row, res.value());
    };
}

template<>
EdgeColumnSetter makeTypedEdgeSetter<list_entry_t>([[maybe_unused]] uint64_t fieldIdx,
    [[maybe_unused]] std::string colName) {
    throw NotImplementedException("List type is not supported in graphar scan.");
}

// Edge setter for "from" (source) and "to" (destination)
template<typename T>
EdgeColumnSetter makeFromSetter(uint64_t fieldIdx, std::string colName) {
    return [fieldIdx, colName = std::move(colName)](graphar::EdgeIter& it,
               function::TableFuncOutput& output, kuzu::common::idx_t row,
               std::shared_ptr<graphar::VerticesCollection> from_vertices) {
        graphar::IdType src = it.source();
        auto vertex_it = from_vertices->find(src);
        auto res = vertex_it.property<T>(colName);
        auto& vec = output.dataChunk.getValueVectorMutable(fieldIdx);
        vec.setValue(row, res.value());
    };
}

template<typename T>
EdgeColumnSetter makeToSetter(uint64_t fieldIdx, std::string colName) {
    return [fieldIdx, colName = std::move(colName)](graphar::EdgeIter& it,
               function::TableFuncOutput& output, kuzu::common::idx_t row,
               std::shared_ptr<graphar::VerticesCollection> to_vertices) {
        graphar::IdType dst = it.destination();
        auto vertex_it = to_vertices->find(dst);
        auto res = vertex_it.property<T>(colName);
        auto& vec = output.dataChunk.getValueVectorMutable(fieldIdx);
        vec.setValue(row, res.value());
    };
}

// Edge setter for "internal_from" (source) and "internal_to" (destination)
EdgeColumnSetter makeInternalFromSetter(uint64_t fieldIdx) {
    return
        [fieldIdx](graphar::EdgeIter& it, function::TableFuncOutput& output,
            kuzu::common::idx_t row, std::shared_ptr<graphar::VerticesCollection> from_vertices) {
            graphar::IdType src = it.source();
            auto vertex_it = from_vertices->find(src);
            auto res = vertex_it.id();
            auto& vec = output.dataChunk.getValueVectorMutable(fieldIdx);
            vec.setValue(row, static_cast<int64_t>(res));
        };
}

EdgeColumnSetter makeInternalToSetter(uint64_t fieldIdx) {
    return [fieldIdx](graphar::EdgeIter& it, function::TableFuncOutput& output,
               kuzu::common::idx_t row, std::shared_ptr<graphar::VerticesCollection> to_vertices) {
        graphar::IdType dst = it.destination();
        auto vertex_it = to_vertices->find(dst);
        auto res = vertex_it.id();
        auto& vec = output.dataChunk.getValueVectorMutable(fieldIdx);
        vec.setValue(row, static_cast<int64_t>(res));
    };
}

static const std::unordered_map<LogicalTypeID,
    std::function<VertexColumnSetter(uint64_t, std::string)>>
    vertexSetterFactory = {
        {LogicalTypeID::INT64,
            [](uint64_t idx, std::string col) {
                return makeTypedVertexSetter<int64_t>(idx, std::move(col));
            }},
        {LogicalTypeID::INT32,
            [](uint64_t idx, std::string col) {
                return makeTypedVertexSetter<int32_t>(idx, std::move(col));
            }},
        {LogicalTypeID::DOUBLE,
            [](uint64_t idx, std::string col) {
                return makeTypedVertexSetter<double>(idx, std::move(col));
            }},
        {LogicalTypeID::FLOAT,
            [](uint64_t idx, std::string col) {
                return makeTypedVertexSetter<float>(idx, std::move(col));
            }},
        {LogicalTypeID::STRING,
            [](uint64_t idx, std::string col) {
                return makeTypedVertexSetter<std::string>(idx, std::move(col));
            }},
        {LogicalTypeID::BOOL,
            [](uint64_t idx, std::string col) {
                return makeTypedVertexSetter<bool>(idx, std::move(col));
            }},
        {LogicalTypeID::DATE,
            [](uint64_t idx, std::string col) {
                return makeTypedVertexSetter<date_t>(idx, std::move(col));
            }},
        {LogicalTypeID::TIMESTAMP,
            [](uint64_t idx, std::string col) {
                return makeTypedVertexSetter<timestamp_t>(idx, std::move(col));
            }},
        {LogicalTypeID::LIST,
            [](uint64_t idx, std::string col) {
                return makeTypedVertexSetter<list_entry_t>(idx, std::move(col));
            }},
};

static const std::unordered_map<LogicalTypeID,
    std::function<EdgeColumnSetter(uint64_t, std::string)>>
    edgeSetterFactory = {
        {LogicalTypeID::INT64,
            [](uint64_t idx, std::string col) {
                return makeTypedEdgeSetter<int64_t>(idx, std::move(col));
            }},
        {LogicalTypeID::INT32,
            [](uint64_t idx, std::string col) {
                return makeTypedEdgeSetter<int32_t>(idx, std::move(col));
            }},
        {LogicalTypeID::DOUBLE,
            [](uint64_t idx, std::string col) {
                return makeTypedEdgeSetter<double>(idx, std::move(col));
            }},
        {LogicalTypeID::FLOAT,
            [](uint64_t idx, std::string col) {
                return makeTypedEdgeSetter<float>(idx, std::move(col));
            }},
        {LogicalTypeID::STRING,
            [](uint64_t idx, std::string col) {
                return makeTypedEdgeSetter<std::string>(idx, std::move(col));
            }},
        {LogicalTypeID::BOOL,
            [](uint64_t idx, std::string col) {
                return makeTypedEdgeSetter<bool>(idx, std::move(col));
            }},
        {LogicalTypeID::DATE,
            [](uint64_t idx, std::string col) {
                return makeTypedEdgeSetter<date_t>(idx, std::move(col));
            }},
        {LogicalTypeID::TIMESTAMP,
            [](uint64_t idx, std::string col) {
                return makeTypedEdgeSetter<timestamp_t>(idx, std::move(col));
            }},
        {LogicalTypeID::LIST,
            [](uint64_t idx, std::string col) {
                return makeTypedEdgeSetter<list_entry_t>(idx, std::move(col));
            }},
};

static const std::unordered_map<LogicalTypeID,
    std::function<EdgeColumnSetter(uint64_t, std::string)>>
    fromSetterFactory = {
        {LogicalTypeID::INT64,
            [](uint64_t idx, std::string col) {
                return makeFromSetter<int64_t>(idx, std::move(col));
            }},
        {LogicalTypeID::INT32,
            [](uint64_t idx, std::string col) {
                return makeFromSetter<int32_t>(idx, std::move(col));
            }},
        {LogicalTypeID::DOUBLE,
            [](uint64_t idx, std::string col) {
                return makeFromSetter<double>(idx, std::move(col));
            }},
        {LogicalTypeID::FLOAT,
            [](uint64_t idx, std::string col) {
                return makeFromSetter<float>(idx, std::move(col));
            }},
        {LogicalTypeID::STRING,
            [](uint64_t idx, std::string col) {
                return makeFromSetter<std::string>(idx, std::move(col));
            }},
};

static const std::unordered_map<LogicalTypeID,
    std::function<EdgeColumnSetter(uint64_t, std::string)>>
    toSetterFactory = {
        {LogicalTypeID::INT64,
            [](uint64_t idx, std::string col) {
                return makeToSetter<int64_t>(idx, std::move(col));
            }},
        {LogicalTypeID::INT32,
            [](uint64_t idx, std::string col) {
                return makeToSetter<int32_t>(idx, std::move(col));
            }},
        {LogicalTypeID::DOUBLE,
            [](uint64_t idx, std::string col) {
                return makeToSetter<double>(idx, std::move(col));
            }},
        {LogicalTypeID::FLOAT,
            [](uint64_t idx, std::string col) { return makeToSetter<float>(idx, std::move(col)); }},
        {LogicalTypeID::STRING,
            [](uint64_t idx, std::string col) {
                return makeToSetter<std::string>(idx, std::move(col));
            }},
};

static void autoDetectVertexSchema([[maybe_unused]] main::ClientContext* context,
    std::shared_ptr<graphar::GraphInfo> graph_info, std::string table_name,
    std::vector<LogicalType>& types, std::vector<std::string>& names) {
    auto vertexInfo = graph_info->GetVertexInfo(table_name);
    if (!vertexInfo) {
        throw BinderException("GraphAr's VertexInfo " + table_name + " does not exist as vertex.");
    }

    names.push_back("internal_id");
    types.push_back(LogicalType::INT64());

    // Construct the types and names from the vertex info.
    for (auto& property_group : vertexInfo->GetPropertyGroups()) {
        for (const auto& property : property_group->GetProperties()) {
            names.push_back(property.name);
            types.push_back(grapharTypeToKuzuType(property.type));
        }
    }
}

static void autoDetectEdgeSchema([[maybe_unused]] main::ClientContext* context,
    std::shared_ptr<graphar::GraphInfo> graph_info, const std::string& table_name,
    std::vector<LogicalType>& types, std::vector<std::string>& names,
    std::unordered_map<std::string, std::string>& edges_from_to_mapping) {
    std::string src_type, edge_type, dst_type;
    if (!tryParseEdgeTableName(table_name, src_type, edge_type, dst_type)) {
        throw BinderException("Edge table_name must be specified in `src_edge_dst` format "
                              "(supported separators: '_'). Given: " +
                              table_name);
    }

    // Use GraphInfo to get EdgeInfo
    auto edgeInfo = graph_info->GetEdgeInfo(src_type, edge_type, dst_type);
    if (!edgeInfo) {
        throw BinderException("GraphAr's EdgeInfo does not exist for " + table_name +
                              " (parsed as " + src_type + REGULAR_SEPARATOR + edge_type +
                              REGULAR_SEPARATOR + dst_type + ").");
    }

    auto from_vertex_info = graph_info->GetVertexInfo(src_type);
    auto to_vertex_info = graph_info->GetVertexInfo(dst_type);
    if (!from_vertex_info) {
        throw BinderException(
            "GraphAr's VertexInfo does not exist for edge source type " + src_type + ".");
    }
    if (!to_vertex_info) {
        throw BinderException(
            "GraphAr's VertexInfo does not exist for edge destination type " + dst_type + ".");
    }

    // Prepend from/to columns
    names.push_back(FROM_COL_NAME);
    // Find from column mapping name in the from_vertex_info's primary keys
    for (const auto& propertyGroup : from_vertex_info->GetPropertyGroups()) {
        for (const auto& property : propertyGroup->GetProperties()) {
            if (property.is_primary) {
                edges_from_to_mapping.insert({FROM_COL_NAME, property.name});
                break;
            }
        }
    }
    types.push_back(LogicalType::INT64());

    names.push_back(TO_COL_NAME);
    // Find to column mapping name in the to_vertex_info's primary keys
    for (const auto& propertyGroup : to_vertex_info->GetPropertyGroups()) {
        for (const auto& property : propertyGroup->GetProperties()) {
            if (property.is_primary) {
                edges_from_to_mapping.insert({TO_COL_NAME, property.name});
                break;
            }
        }
    }
    types.push_back(LogicalType::INT64());

    // Add internal_from/internal_to edge columns (optional)
    // internal_from/to is optional, because they are stored in internal_id of vertices.
    // names.push_back(INTERNAL_FROM_COL_NAME);
    // types.push_back(LogicalType::INT64());
    // names.push_back(INTERNAL_TO_COL_NAME);
    // types.push_back(LogicalType::INT64());

    // Add the edge properties except from/to
    for (auto& property_group : edgeInfo->GetPropertyGroups()) {
        for (const auto& property : property_group->GetProperties()) {
            names.push_back(property.name);
            types.push_back(grapharTypeToKuzuType(property.type));
        }
    }
}

GrapharScanBindData::GrapharScanBindData(binder::expression_vector columns,
    common::FileScanInfo fileScanInfo, main::ClientContext* context,
    std::shared_ptr<graphar::GraphInfo> graph_info, std::string table_name,
    std::vector<std::string> column_names, std::vector<kuzu::common::LogicalType> column_types,
    bool is_edge)
    : ScanFileBindData{std::move(columns), 0 /* numRows */, std::move(fileScanInfo), context},
      graph_info{std::move(graph_info)},
      column_info{std::make_shared<KuzuColumnInfo>(column_names)},
      table_name{std::move(table_name)}, column_names{std::move(column_names)},
      column_types{std::move(column_types)}, is_edge(is_edge) {
    if (!is_edge) {
        this->vertex_column_setters.reserve(this->column_names.size());
        for (size_t i = 0; i < this->column_names.size(); ++i) {
            if (StringUtils::caseInsensitiveEquals(this->column_names[i], "internal_id")) {
                // special-case internal_id
                this->vertex_column_setters.push_back(makeInternalIdVertexSetter(
                    getFieldIdx(this->column_names[i]), this->column_names[i]));
                continue;
            }
            auto typeID = this->column_types[i].getLogicalTypeID();
            uint64_t fieldIdx = getFieldIdx(this->column_names[i]);
            KU_ASSERT(fieldIdx != UINT64_MAX);
            auto it = vertexSetterFactory.find(typeID);
            if (it == vertexSetterFactory.end()) {
                throw NotImplementedException{
                    "Unsupported column type in GrapharScan bind (vertex): " +
                    std::to_string((int)typeID)};
            }
            this->vertex_column_setters.push_back(it->second(fieldIdx, this->column_names[i]));
        }
    } else {
        this->edge_column_setters.reserve(this->column_names.size());
        for (size_t i = 0; i < this->column_names.size(); ++i) {
            uint64_t fieldIdx = getFieldIdx(this->column_names[i]);
            KU_ASSERT(fieldIdx != UINT64_MAX);
            // special-case from/to
            if (StringUtils::caseInsensitiveEquals(this->column_names[i], FROM_COL_NAME)) {
                LogicalTypeID typeID = this->column_types[i].getLogicalTypeID();
                auto it = fromSetterFactory.find(typeID);
                if (it == fromSetterFactory.end()) {
                    throw NotImplementedException{
                        "Unsupported column type in GrapharScan bind (from edge): " +
                        std::to_string((int)typeID)};
                }
                this->edge_column_setters.push_back(it->second(fieldIdx, "id"));
                continue;
            }

            if (StringUtils::caseInsensitiveEquals(this->column_names[i], TO_COL_NAME)) {
                LogicalTypeID typeID = this->column_types[i].getLogicalTypeID();
                auto it = toSetterFactory.find(typeID);
                if (it == toSetterFactory.end()) {
                    throw NotImplementedException{
                        "Unsupported column type in GrapharScan bind (to edge): " +
                        std::to_string((int)typeID)};
                }
                this->edge_column_setters.push_back(it->second(fieldIdx, "id"));
                continue;
            }

            if (StringUtils::caseInsensitiveEquals(this->column_names[i], INTERNAL_FROM_COL_NAME)) {
                // special-case internal_from
                this->edge_column_setters.push_back(
                    makeInternalFromSetter(getFieldIdx(this->column_names[i])));
                continue;
            }

            if (StringUtils::caseInsensitiveEquals(this->column_names[i], INTERNAL_TO_COL_NAME)) {
                // special-case internal_to
                this->edge_column_setters.push_back(
                    makeInternalToSetter(getFieldIdx(this->column_names[i])));
                continue;
            }

            // regular edge property
            LogicalTypeID typeID = this->column_types[i].getLogicalTypeID();
            auto it = edgeSetterFactory.find(typeID);
            if (it == edgeSetterFactory.end()) {
                throw NotImplementedException{
                    "Unsupported column type in GrapharScan bind (edge): " +
                    std::to_string((int)typeID)};
            }
            this->edge_column_setters.push_back(it->second(fieldIdx, this->column_names[i]));
        }
    }
    this->max_threads = context->getMaxNumThreadForExec();
}

KuzuColumnInfo::KuzuColumnInfo(std::vector<std::string> column_names) {
    this->colNames = std::move(column_names);
    idx_t colIdx = 0;
    for (auto& colName : this->colNames) {
        colNameToIdx.insert({colName, colIdx++});
    }
}

uint64_t KuzuColumnInfo::getFieldIdx(std::string fieldName) const {
    // For a small number of keys, probing a vector is faster than lookups in an unordered_map.
    if (colNames.size() < 24) {
        auto iter = std::find(colNames.begin(), colNames.end(), fieldName);
        if (iter != colNames.end()) {
            return iter - colNames.begin();
        }
    } else {
        auto itr = colNameToIdx.find(fieldName);
        if (itr != colNameToIdx.end()) {
            return itr->second;
        }
    }
    // From and to are case-insensitive for backward compatibility.
    if (StringUtils::caseInsensitiveEquals(fieldName, FROM_COL_NAME)) {
        return colNameToIdx.at(FROM_COL_NAME);
    } else if (StringUtils::caseInsensitiveEquals(fieldName, TO_COL_NAME)) {
        return colNameToIdx.at(TO_COL_NAME);
    }
    return UINT64_MAX;
}

std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto scanInput = ku_dynamic_cast<ExtraScanTableFuncBindInput*>(input->extraInput.get());
    std::string absolute_path = scanInput->fileScanInfo.getFilePath(0);
    if (absolute_path.empty()) {
        throw BinderException("GraphAr scan requires a valid file path.");
    }

    std::string table_name = scanInput->fileScanInfo.options.at("table_name").strVal;

    // Load graph info from the file path
    auto graph_info = graphar::GraphInfo::Load(absolute_path).value();
    if (!graph_info) {
        throw BinderException("GraphAr's GraphInfo could not be loaded from " + absolute_path);
    }

    std::vector<LogicalType> column_types;
    std::vector<std::string> column_names;
    std::unordered_map<std::string, std::string> edges_from_to_mapping;
    bool is_edge = false;

    auto vertex_infos = graph_info->GetVertexInfos();
    auto edge_infos = graph_info->GetEdgeInfos();

    // Try vertex first
    for (const auto& v_info : vertex_infos) {
        if (v_info->GetType() == table_name) {
            autoDetectVertexSchema(context, graph_info, table_name, column_types, column_names);
            is_edge = false;
            goto TAIL;
        }
    }

    // Try edge if not vertex
    for (const auto& e_info : edge_infos) {
        std::string src_type = e_info->GetSrcType();
        std::string edge_type = e_info->GetEdgeType();
        std::string dst_type = e_info->GetDstType();
        std::string full_edge_name =
            src_type + REGULAR_SEPARATOR + edge_type + REGULAR_SEPARATOR + dst_type;
        if (full_edge_name == table_name) {
            autoDetectEdgeSchema(context, graph_info, table_name, column_types, column_names,
                edges_from_to_mapping);
            is_edge = true;
            goto TAIL;
        }
    }

    KU_ASSERT(true);

TAIL:
    KU_ASSERT(column_types.size() == column_names.size());

    // ignore property type suffixes in column names.
    // Examples:
    // 'person-date' (vertex) -getFirstToken-> 'person'
    // 'knows-timestamp' (edge) -getFirstToken-> 'knows'
    std::vector<std::string> base_column_names;
    for (auto& name : column_names) {
        base_column_names.push_back(getFirstToken(name));
    }

    base_column_names =
        TableFunction::extractYieldVariables(base_column_names, input->yieldVariables);
    auto columns = input->binder->createVariables(base_column_names, column_types);
    return std::make_unique<GrapharScanBindData>(std::move(columns), scanInput->fileScanInfo.copy(),
        context, std::move(graph_info), std::move(table_name), std::move(column_names),
        std::move(column_types), is_edge);
}

} // namespace graphar_extension
} // namespace kuzu
