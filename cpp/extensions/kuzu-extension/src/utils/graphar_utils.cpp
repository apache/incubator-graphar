#include "utils/graphar_utils.h"

namespace kuzu {
namespace graphar_extension {

std::string getFirstToken(const std::string& input) {
    size_t pos = input.find('-');
    if (pos == std::string::npos) {
        return input;
    }
    return input.substr(0, pos);
}

void getYamlNameWithoutGrapharLabel(const std::string& filePath) {
    const std::string grapharLabel = DEFAULT_GRAPHAR_LABEL;
    if (ends_with(filePath, grapharLabel)) {
        // remove the trailing ".graphar"
        const_cast<std::string&>(filePath).erase(filePath.size() - grapharLabel.size());
    }
}

bool ends_with(const std::string& s, const std::string& suffix) {
    if (s.size() < suffix.size())
        return false;
    return s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
}

bool parse_is_edge(const std::string& path) {
    if (path.empty()) {
        throw std::invalid_argument("empty path");
    }

    // Extract the file name (substring after the last '/' or '\\')
    const size_t pos = path.find_last_of("/\\");
    const std::string filename = (pos == std::string::npos) ? path : path.substr(pos + 1);

    if (filename.empty()) {
        throw std::invalid_argument("path has no filename");
    }

    // Convert to uppercase for case-insensitive comparison
    const std::string up = common::StringUtils::getUpper(filename);

    // Uppercase suffix constants
    constexpr const char* EDGE_YAML = ".EDGE.YAML";
    constexpr const char* EDGE_YML = ".EDGE.YML";
    constexpr const char* VERTEX_YAML = ".VERTEX.YAML";
    constexpr const char* VERTEX_YML = ".VERTEX.YML";

    if (ends_with(up, EDGE_YAML) || ends_with(up, EDGE_YML)) {
        return true;
    }
    if (ends_with(up, VERTEX_YAML) || ends_with(up, VERTEX_YML)) {
        return false;
    }

    throw std::invalid_argument(
        "filename does not end with .vertex.(yml|yaml) or .edge.(yml|yaml)");
}

bool tryParseEdgeTableName(const std::string& table_name, std::string& src,
    std::string& edge, std::string& dst) {
    // Split by '_'.
    std::vector<std::string> parts;
    size_t start = 0;
    for (size_t i = 0; i <= table_name.size(); ++i) {
        if (i == table_name.size() || table_name[i] == SEPARATOR) {
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
    return false;
}

LogicalType grapharTypeToKuzuType(std::shared_ptr<graphar::DataType> type) {
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
            throw NotImplementedException{"GraphAr's List Type with value type " +
                                          std::to_string(static_cast<int>(value_type_id)) +
                                          " is not implemented."};
        }
    }
    default:
        throw NotImplementedException{
            "GraphAr's Type " + std::to_string(static_cast<int>(type_id)) + " is not implemented."};
    }
}

graphar::Type kuzuTypeToGrapharType(const LogicalType& type) {
    switch (type.getLogicalTypeID()) {
    case LogicalTypeID::BOOL:
        return graphar::Type::BOOL;
    case LogicalTypeID::INT64:
        return graphar::Type::INT64;
    case LogicalTypeID::INT32:
        return graphar::Type::INT32;
    case LogicalTypeID::FLOAT:
        return graphar::Type::FLOAT;
    case LogicalTypeID::STRING:
        return graphar::Type::STRING;
    case LogicalTypeID::DOUBLE:
        return graphar::Type::DOUBLE;
    case LogicalTypeID::DATE:
        return graphar::Type::DATE;
    case LogicalTypeID::TIMESTAMP:
        return graphar::Type::TIMESTAMP;
    case LogicalTypeID::LIST: 
        return graphar::Type::LIST;
    default:
        throw NotImplementedException{"Kuzu's Type " +
                                      std::to_string(static_cast<int>(type.getLogicalTypeID())) +
                                      " is not implemented."};
    }
}

} // namespace graphar_extension
} // namespace kuzu
