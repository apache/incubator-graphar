#include <iostream>
#include <filesystem>
#include <memory>
#include <string>
#include "kuzu.hpp"

using namespace std;
using namespace kuzu::main;       // Database, Connection, QueryResult
using namespace kuzu::common;     // Value, LogicalTypeID
using namespace kuzu::processor;  // ResultSet, FlatTuple

#include <kuzu.hpp>
#include <iostream>
#include <memory>

using namespace std;
using namespace kuzu;
using namespace kuzu::common;

#include <kuzu.hpp>
#include <iostream>
#include <memory>

using namespace std;
using namespace kuzu;
using namespace kuzu::common;

void printRow(const unique_ptr<QueryResult> &result, const shared_ptr<FlatTuple> &row, ostream &os) {
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
    auto db = make_unique<Database>("test", cfg);
    auto conn = make_unique<Connection>(db.get());

    auto loadRes = conn->query("LOAD EXTENSION \"xxx/libgraphar.kuzu_extension\";");
    if (!loadRes->isSuccess()) {
        cerr << "Load extension failed: " << loadRes->getErrorMessage() << endl;
        filesystem::remove_all("test");
        return -1;
    }
    cout << "Extension loaded.\n";

    // === GRAPH metadata ===
    {
        auto result = conn->query("CALL GRAPHAR_METADATA('xxx/modern_graph.graph.yml') RETURN *;");
        if (!result->isSuccess()) {
            cerr << "GRAPHAR_METADATA error: " << result->getErrorMessage() << endl;
            filesystem::remove_all("test");
            return -1;
        }
        cout << "[GRAPH Metadata]\n";
        while (result->hasNext()) {
            auto row = result->getNext();
            printRow(result, row, cout);
        }
    }

    // === VERTEX metadata ===
    {
        auto result = conn->query("CALL GRAPHAR_METADATA('xxx/modern_graph.graph.yml', vertex_type := 'person') RETURN *;");
        if (!result->isSuccess()) {
            cerr << "VERTEX Metadata error: " << result->getErrorMessage() << endl;
            filesystem::remove_all("test");
            return -1;
        }
        cout << "[VERTEX Metadata]\n";
        while (result->hasNext()) {
            auto row = result->getNext();
            printRow(result, row, cout);
        }
    }

    // === EDGE metadata ===
    {
        auto result = conn->query("CALL GRAPHAR_METADATA('xxx/modern_graph.graph.yml', edge_type := 'person_knows_person') RETURN *;");
        if (!result->isSuccess()) {
            cerr << "EDGE Metadata error: " << result->getErrorMessage() << endl;
            filesystem::remove_all("test");
            return -1;
        }
        cout << "[EDGE Metadata]\n";
        while (result->hasNext()) {
            auto row = result->getNext();
            printRow(result, row, cout);
        }
    }

    filesystem::remove_all("test");
    return 0;
}
