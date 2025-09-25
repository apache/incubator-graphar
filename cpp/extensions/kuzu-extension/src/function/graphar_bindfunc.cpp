#include "function/graphar_scan.h"

#include "common/exception/not_implemented.h"

namespace kuzu {
namespace graphar_extension {

using namespace function;
using namespace common;

// Vertex setter maker for properties
template<typename T>
VertexColumnSetter makeTypedVertexSetter(uint64_t fieldIdx, std::string colName) {
    return [fieldIdx, colName = std::move(colName)](graphar::VertexIter& it, function::TableFuncOutput& output, kuzu::common::idx_t row) {
        auto res = it.property<T>(colName);
        auto &vec = output.dataChunk.getValueVectorMutable(fieldIdx);
        vec.setValue(row, res.value());
    };
}

template<>
VertexColumnSetter makeTypedVertexSetter<list_entry_t>(uint64_t fieldIdx, std::string colName) {
    throw NotImplementedException("List type is not supported in graphar scan.");
}

// Edge setter maker for properties
template<typename T>
EdgeColumnSetter makeTypedEdgeSetter(uint64_t fieldIdx, std::string colName) {
    return [fieldIdx, colName = std::move(colName)](graphar::EdgeIter& it, function::TableFuncOutput& output, kuzu::common::idx_t row, [[maybe_unused]] std::shared_ptr<graphar::VerticesCollection> unused) {
        auto res = it.property<T>(colName);
        auto &vec = output.dataChunk.getValueVectorMutable(fieldIdx);
        vec.setValue(row, res.value());
    };
}

template<>
EdgeColumnSetter makeTypedEdgeSetter<list_entry_t>(uint64_t fieldIdx, std::string colName) {
    throw NotImplementedException("List type is not supported in graphar scan.");
}

// Edge setter for "from" (source) and "to" (destination)
template<typename T>
EdgeColumnSetter makeFromSetter(uint64_t fieldIdx, std::string colName) {
    return [fieldIdx, colName = std::move(colName)](graphar::EdgeIter& it, function::TableFuncOutput& output, kuzu::common::idx_t row, std::shared_ptr<graphar::VerticesCollection> from_vertices) {
        graphar::IdType src = it.source();
        auto vertex_it = from_vertices->find(src);
        auto res = vertex_it.property<T>(colName);
        auto &vec = output.dataChunk.getValueVectorMutable(fieldIdx);
        vec.setValue(row, res.value());
    };
}

template<typename T>
EdgeColumnSetter makeToSetter(uint64_t fieldIdx, std::string colName) {
    return [fieldIdx, colName = std::move(colName)](graphar::EdgeIter& it, function::TableFuncOutput& output, kuzu::common::idx_t row, std::shared_ptr<graphar::VerticesCollection> to_vertices) {
        graphar::IdType dst = it.destination();
        auto vertex_it = to_vertices->find(dst);
        auto res = vertex_it.property<T>(colName);
        auto &vec = output.dataChunk.getValueVectorMutable(fieldIdx);
        vec.setValue(row, res.value());
    };
}

static const std::unordered_map<LogicalTypeID, std::function<VertexColumnSetter(uint64_t, std::string)>> vertexSetterFactory = {
    { LogicalTypeID::INT64,   [](uint64_t idx, std::string col){ return makeTypedVertexSetter<int64_t>(idx, std::move(col)); } },
    { LogicalTypeID::INT32,   [](uint64_t idx, std::string col){ return makeTypedVertexSetter<int32_t>(idx, std::move(col)); } },
    { LogicalTypeID::DOUBLE,  [](uint64_t idx, std::string col){ return makeTypedVertexSetter<double>(idx, std::move(col)); } },
    { LogicalTypeID::FLOAT,   [](uint64_t idx, std::string col){ return makeTypedVertexSetter<float>(idx, std::move(col)); } },
    { LogicalTypeID::STRING,  [](uint64_t idx, std::string col){ return makeTypedVertexSetter<std::string>(idx, std::move(col)); } },
    { LogicalTypeID::BOOL,    [](uint64_t idx, std::string col){ return makeTypedVertexSetter<bool>(idx, std::move(col)); } },
    { LogicalTypeID::DATE,    [](uint64_t idx, std::string col){ return makeTypedVertexSetter<date_t>(idx, std::move(col)); } },
    { LogicalTypeID::TIMESTAMP, [](uint64_t idx, std::string col){ return makeTypedVertexSetter<timestamp_t>(idx, std::move(col)); } },
    { LogicalTypeID::LIST,    [](uint64_t idx, std::string col){ return makeTypedVertexSetter<list_entry_t>(idx, std::move(col)); } },
};

static const std::unordered_map<LogicalTypeID, std::function<EdgeColumnSetter(uint64_t, std::string)>> edgeSetterFactory = {
    { LogicalTypeID::INT64,   [](uint64_t idx, std::string col){ return makeTypedEdgeSetter<int64_t>(idx, std::move(col)); } },
    { LogicalTypeID::INT32,   [](uint64_t idx, std::string col){ return makeTypedEdgeSetter<int32_t>(idx, std::move(col)); } },
    { LogicalTypeID::DOUBLE,  [](uint64_t idx, std::string col){ return makeTypedEdgeSetter<double>(idx, std::move(col)); } },
    { LogicalTypeID::FLOAT,   [](uint64_t idx, std::string col){ return makeTypedEdgeSetter<float>(idx, std::move(col)); } },
    { LogicalTypeID::STRING,  [](uint64_t idx, std::string col){ return makeTypedEdgeSetter<std::string>(idx, std::move(col)); } },
    { LogicalTypeID::BOOL,    [](uint64_t idx, std::string col){ return makeTypedEdgeSetter<bool>(idx, std::move(col)); } },
    { LogicalTypeID::DATE,    [](uint64_t idx, std::string col){ return makeTypedEdgeSetter<date_t>(idx, std::move(col)); } },
    { LogicalTypeID::TIMESTAMP, [](uint64_t idx, std::string col){ return makeTypedEdgeSetter<timestamp_t>(idx, std::move(col)); } },
    { LogicalTypeID::LIST,    [](uint64_t idx, std::string col){ return makeTypedEdgeSetter<list_entry_t>(idx, std::move(col)); } },
};

static const std::unordered_map<LogicalTypeID, std::function<EdgeColumnSetter(uint64_t, std::string)>> fromSetterFactory = {
    { LogicalTypeID::INT64,   [](uint64_t idx, std::string col){ return makeFromSetter<int64_t>(idx, std::move(col)); } },
    { LogicalTypeID::INT32,   [](uint64_t idx, std::string col){ return makeFromSetter<int32_t>(idx, std::move(col)); } },
    { LogicalTypeID::DOUBLE,  [](uint64_t idx, std::string col){ return makeFromSetter<double>(idx, std::move(col)); } },
    { LogicalTypeID::FLOAT,   [](uint64_t idx, std::string col){ return makeFromSetter<float>(idx, std::move(col)); } },
    { LogicalTypeID::STRING,  [](uint64_t idx, std::string col){ return makeFromSetter<std::string>(idx, std::move(col)); } },
};

static const std::unordered_map<LogicalTypeID, std::function<EdgeColumnSetter(uint64_t, std::string)>> toSetterFactory = {
    { LogicalTypeID::INT64,   [](uint64_t idx, std::string col){ return makeToSetter<int64_t>(idx, std::move(col)); } },
    { LogicalTypeID::INT32,   [](uint64_t idx, std::string col){ return makeToSetter<int32_t>(idx, std::move(col)); } },
    { LogicalTypeID::DOUBLE,  [](uint64_t idx, std::string col){ return makeToSetter<double>(idx, std::move(col)); } },
    { LogicalTypeID::FLOAT,   [](uint64_t idx, std::string col){ return makeToSetter<float>(idx, std::move(col)); } },
    { LogicalTypeID::STRING,  [](uint64_t idx, std::string col){ return makeToSetter<std::string>(idx, std::move(col)); } },
};

static LogicalType GrapharTypeToKuzuTypeFunc(std::shared_ptr<graphar::DataType> type) {
    graphar::Type type_id = type->id();
    switch (type_id) {
        case graphar::Type::BOOL:
            return LogicalType::BOOL();
        case graphar::Type::INT64:
            return LogicalType::INT64();
        case graphar::Type::INT32:
            return LogicalType::INT32();
        case graphar::Type::FLOAT:
            return LogicalType::FLOAT();
        case graphar::Type::STRING:
            return LogicalType::STRING();
        case graphar::Type::DOUBLE:
            return LogicalType::DOUBLE();
        case graphar::Type::DATE:
            return LogicalType::DATE();
        case graphar::Type::TIMESTAMP:
            return LogicalType::TIMESTAMP();
        case graphar::Type::LIST: {
            auto value_type_id = type->value_type()->id();
            switch (value_type_id) {
                case graphar::Type::BOOL:
                    return LogicalType::LIST(LogicalType::BOOL());
                case graphar::Type::INT64:
                    return LogicalType::LIST(LogicalType::INT64());
                case graphar::Type::INT32:
                    return LogicalType::LIST(LogicalType::INT32());
                case graphar::Type::FLOAT:
                    return LogicalType::LIST(LogicalType::FLOAT());
                case graphar::Type::STRING:
                    return LogicalType::LIST(LogicalType::STRING());
                case graphar::Type::DOUBLE:
                    return LogicalType::LIST(LogicalType::DOUBLE());
                case graphar::Type::DATE:
                    return LogicalType::LIST(LogicalType::DATE());
                case graphar::Type::TIMESTAMP:
                    return LogicalType::LIST(LogicalType::TIMESTAMP());
                default:
                    throw NotImplementedException{"GraphAr's List Type with value type " + std::to_string(static_cast<int>(value_type_id)) + " is not implemented."};
            }
        }
        default:
            throw NotImplementedException{"GraphAr's Type " + std::to_string(static_cast<int>(type_id)) + " is not implemented."};
    }
}

static void autoDetectVertexSchema([[maybe_unused]] main::ClientContext* context, std::shared_ptr<graphar::GraphInfo> graph_info, std::string table_name,
    std::vector<LogicalType>& types, std::vector<std::string>& names) {
    auto vertex_info = graph_info->GetVertexInfo(table_name);
    if (!vertex_info) {
        throw BinderException("GraphAr's Type " + table_name + " does not exist as vertex.");
    }

    // Construct the types and names from the vertex info.
    for (auto& property_group : vertex_info->GetPropertyGroups()) {
        for (const auto& property : property_group->GetProperties()) {
            names.push_back(property.name);
            types.push_back(GrapharTypeToKuzuTypeFunc(property.type));
        }
    }
}

static bool tryParseEdgeTableName(const std::string &table_name, std::string &src, std::string &edge, std::string &dst) {
    // Try '.' then ':' then '_'
    std::vector<char> seps = {'.', ':', '_'};
    for (char sep : seps) {
        std::vector<std::string> parts;
        size_t start = 0;
        for (size_t i = 0; i <= table_name.size(); ++i) {
            if (i == table_name.size() || table_name[i] == sep) {
                parts.push_back(table_name.substr(start, i - start));
                start = i + 1;
            }
        }
        if (parts.size() == 3 && !parts[0].empty() && !parts[1].empty() && !parts[2].empty()) {
            src = parts[0];
            edge = parts[1];
            dst = parts[2];
            return true;
        }
    }
    return false;
}

static void autoDetectEdgeSchema([[maybe_unused]] main::ClientContext* context, std::shared_ptr<graphar::GraphInfo> graph_info, const std::string &table_name,
    std::vector<LogicalType>& types, std::vector<std::string>& names, std::unordered_map<std::string, std::string>& edges_from_to_mapping) {
    std::string src_type, edge_type, dst_type;
    if (!tryParseEdgeTableName(table_name, src_type, edge_type, dst_type)) {
        throw BinderException("Edge table_name must be specified in `src.edge.dst` format (supported separators: '.', ':', '_'). Given: " + table_name);
    }

    // Use GraphInfo to get EdgeInfo
    auto edge_info = graph_info->GetEdgeInfo(src_type, edge_type, dst_type);
    if (!edge_info) {
        throw BinderException("GraphAr's EdgeInfo does not exist for " + table_name +
                            " (parsed as " + src_type + "." + edge_type + "." + dst_type + ").");
    }

    auto from_vertex_info = graph_info->GetVertexInfo(src_type);
    auto to_vertex_info = graph_info->GetVertexInfo(dst_type);
    if (!from_vertex_info) {
        throw BinderException("GraphAr's VertexInfo does not exist for edge source type " + src_type + ".");
    }
    if (!to_vertex_info) {
        throw BinderException("GraphAr's VertexInfo does not exist for edge destination type " + dst_type + ".");
    }

    // Prepend from/to columns
    names.push_back("from");
    // Find from column mapping name in the from_vertex_info's primary keys
    for (const auto& propertyGroup : from_vertex_info->GetPropertyGroups()) {
        for (const auto& property : propertyGroup->GetProperties()) {
            if (property.is_primary) {
                edges_from_to_mapping.insert({"from", property.name});
                break;
            }
        }
    }
    types.push_back(LogicalType::INT64());

    names.push_back("to");
    // Find to column mapping name in the to_vertex_info's primary keys
    for (const auto& propertyGroup : to_vertex_info->GetPropertyGroups()) {
        for (const auto& property : propertyGroup->GetProperties()) {
            if (property.is_primary) {
                edges_from_to_mapping.insert({"to", property.name});
                break;
            }
        }
    }
    types.push_back(LogicalType::INT64());

    // Add the edge properties except from/to
    for (auto& property_group : edge_info->GetPropertyGroups()) {
        for (const auto& property : property_group->GetProperties()) {
            names.push_back(property.name);
            types.push_back(GrapharTypeToKuzuTypeFunc(property.type));
        }
    }
}

GrapharScanBindData::GrapharScanBindData(binder::expression_vector columns, common::FileScanInfo fileScanInfo, main::ClientContext* context,
    std::shared_ptr<graphar::GraphInfo> graph_info, std::string table_name, std::vector<std::string> column_names, std::vector<kuzu::common::LogicalType> column_types, bool is_edge)
    : ScanFileBindData{std::move(columns), 0 /* numRows */, std::move(fileScanInfo), context},
        graph_info{std::move(graph_info)}, column_info{std::make_shared<KuzuColumnInfo>(column_names)},
        table_name{std::move(table_name)}, column_names{std::move(column_names)}, column_types{std::move(column_types)}, is_edge(is_edge) {
            if (!is_edge) {
                this->vertex_column_setters.reserve(this->column_names.size());
                for (size_t i = 0; i < this->column_names.size(); ++i) {
                    auto typeID = this->column_types[i].getLogicalTypeID();
                    uint64_t fieldIdx = getFieldIdx(this->column_names[i]);
                    KU_ASSERT(fieldIdx != UINT64_MAX);
                    auto it = vertexSetterFactory.find(typeID);
                    if (it == vertexSetterFactory.end()) {
                        throw NotImplementedException{"Unsupported column type in GrapharScan bind (vertex): " + std::to_string((int)typeID)};
                    }
                    this->vertex_column_setters.push_back(it->second(fieldIdx, this->column_names[i]));
                }
            } else {
                this->edge_column_setters.reserve(this->column_names.size());
                for (size_t i = 0; i < this->column_names.size(); ++i) {
                    uint64_t fieldIdx = getFieldIdx(this->column_names[i]);
                    KU_ASSERT(fieldIdx != UINT64_MAX);
                    // special-case from/to
                    if (StringUtils::caseInsensitiveEquals(this->column_names[i], "from")) {
                        LogicalTypeID typeID = this->column_types[i].getLogicalTypeID();
                        auto it = fromSetterFactory.find(typeID);
                        if (it == fromSetterFactory.end()) {
                            throw NotImplementedException{"Unsupported column type in GrapharScan bind (from edge): " + std::to_string((int)typeID)};
                        }
                        this->edge_column_setters.push_back(it->second(fieldIdx, "id"));
                        continue;
                    } else if (StringUtils::caseInsensitiveEquals(this->column_names[i], "to")) {
                        LogicalTypeID typeID = this->column_types[i].getLogicalTypeID();
                        auto it = toSetterFactory.find(typeID);
                        if (it == toSetterFactory.end()) {
                            throw NotImplementedException{"Unsupported column type in GrapharScan bind (to edge): " + std::to_string((int)typeID)};
                        }
                        this->edge_column_setters.push_back(it->second(fieldIdx, "id"));
                        continue;
                    }
                    LogicalTypeID typeID = this->column_types[i].getLogicalTypeID();
                    auto it = edgeSetterFactory.find(typeID);
                    if (it == edgeSetterFactory.end()) {
                        throw NotImplementedException{"Unsupported column type in GrapharScan bind (edge): " + std::to_string((int)typeID)};
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
    if (StringUtils::caseInsensitiveEquals(fieldName, "from")) {
        return colNameToIdx.at("from");
    } else if (StringUtils::caseInsensitiveEquals(fieldName, "to")) {
        return colNameToIdx.at("to");
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

    // Try vertex first
    try {
        autoDetectVertexSchema(context, graph_info, table_name, column_types, column_names);
        is_edge = false;
    } catch (BinderException&) {
        // not a vertex, try edge
        column_types.clear();
        column_names.clear();
        edges_from_to_mapping.clear();
        autoDetectEdgeSchema(context, graph_info, table_name, column_types, column_names, edges_from_to_mapping);
        is_edge = true;
    }

    KU_ASSERT(column_types.size() == column_names.size());
    
    column_names =
        TableFunction::extractYieldVariables(column_names, input->yieldVariables);
    auto columns = input->binder->createVariables(column_names, column_types);
    return std::make_unique<GrapharScanBindData>(std::move(columns), scanInput->fileScanInfo.copy(), context,
        std::move(graph_info), std::move(table_name), std::move(column_names), std::move(column_types), is_edge);
}

} // namespace graphar_extension
} // namespace kuzu
