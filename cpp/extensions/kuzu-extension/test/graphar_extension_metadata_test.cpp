#include <iostream>
#include <filesystem>
#include <memory>
#include "kuzu.hpp"

using namespace kuzu::main;
using namespace kuzu::common;
using namespace kuzu::processor;

void printRow(const std::unique_ptr<QueryResult> &result, const std::shared_ptr<FlatTuple> &row, std::ostream &os) {
    auto colNames = result->getColumnNames();
    for (uint32_t i = 0; i < result->getNumColumns(); i++) {
        std::string colName = colNames[i];
        auto val = row->getValue(i);

        os << colName << ": ";
        os << val->toString();
        os << "\n";
    }
    os << "--------------------------\n";
}


int main() {
    SystemConfig cfg(
        /*bufferPoolSize=*/-1u,
        /*maxNumThreads=*/16,
        /*enableCompression=*/true,
        /*readOnly=*/false);
    auto db = std::make_unique<Database>("test", cfg);
    auto conn = std::make_unique<Connection>(db.get());

    auto loadRes = conn->query("LOAD EXTENSION \"xxx/libgraphar.kuzu_extension\";");
    if (!loadRes->isSuccess()) {
        std::cerr << "Load extension failed: " << loadRes->getErrorMessage() << std::endl;
        std::filesystem::remove_all("test");
        return -1;
    }
    std::cout << "Extension loaded.\n";

    // === GRAPH metadata ===
    {
        auto result = conn->query("CALL GRAPHAR_METADATA('xxx/modern_graph.graph.yml') RETURN *;");
        if (!result->isSuccess()) {
            std::cerr << "GRAPHAR_METADATA error: " << result->getErrorMessage() << std::endl;
            std::filesystem::remove_all("test");
            return -1;
        }
        std::cout << "[GRAPH Metadata]\n";
        while (result->hasNext()) {
            auto row = result->getNext();
            printRow(result, row, std::cout);
        }
    }

    // === VERTEX metadata ===
    {
        auto result = conn->query("CALL GRAPHAR_METADATA('xxx/modern_graph.graph.yml', vertex_type := 'person') RETURN *;");
        if (!result->isSuccess()) {
            std::cerr << "VERTEX Metadata error: " << result->getErrorMessage() << std::endl;
            std::filesystem::remove_all("test");
            return -1;
        }
        std::cout << "[VERTEX Metadata]\n";
        while (result->hasNext()) {
            auto row = result->getNext();
            printRow(result, row, std::cout);
        }
    }

    // === EDGE metadata ===
    {
        auto result = conn->query("CALL GRAPHAR_METADATA('xxx/modern_graph.graph.yml', edge_type := 'person_knows_person') RETURN *;");
        if (!result->isSuccess()) {
            std::cerr << "EDGE Metadata error: " << result->getErrorMessage() << std::endl;
            std::filesystem::remove_all("test");
            return -1;
        }
        std::cout << "[EDGE Metadata]\n";
        while (result->hasNext()) {
            auto row = result->getNext();
            printRow(result, row, std::cout);
        }
    }

    std::filesystem::remove_all("test");
    return 0;
}
