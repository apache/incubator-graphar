#include <iostream>
#include <filesystem>
#include "kuzu.hpp"

using namespace kuzu::main;
using namespace std;

int main()
{
    // Create an empty on-disk database and connect to it
    SystemConfig systemConfig(
        /*bufferPoolSize=*/-1u,
        /*maxNumThreads=*/16,
        /*enableCompression=*/true,
        /*readOnly=*/false);
    auto database = make_unique<Database>("test", systemConfig);

    // Connect to the database.
    auto connection = make_unique<Connection>(database.get());

    // auto loadResult = connection->query("LOAD graphar;");
    auto result = connection->query("LOAD EXTENSION \"/home/gary/.kuzu/extension/0.10.0/linux_amd64/graphar/libgraphar.kuzu_extension\";");
    if (!result->isSuccess())
    {
        std::cerr << result->getErrorMessage() << std::endl;
        std::filesystem::remove_all("test");
        return -1;
    }

    // Load explain
    auto loadExplainResult = connection->query("EXPLAIN LOAD FROM \"/home/gary/incubator-graphar/cpp/build-debug/testing/ldbc_sample/parquet/ldbc_sample.graph.yml\" (file_format=\"graphar\", table_name=\"person\") RETURN *");
    // auto copyExplainResult = connection->query("EXPLAIN COPY Person FROM \"/home/gary/incubator-graphar/cpp/build-debug/testing/ldbc_sample/json/LdbcSample.graph.yml\" (file_format=\"graphar\", table_name=\"Person\")");
    if (!loadExplainResult->isSuccess())
    {
        std::cerr << loadExplainResult->getErrorMessage() << std::endl;
        std::filesystem::remove_all("test");
        return -1;
    }

    while (loadExplainResult->hasNext())
    {
        auto row = loadExplainResult->getNext();
        std::cout << row->getValue(0)->getValue<string>() << std::endl;
    }

    // Load data.
    auto loadResult = connection->query("LOAD FROM \"/home/gary/incubator-graphar/cpp/build-debug/testing/ldbc_sample/parquet/ldbc_sample.graph.yml\" (file_format=\"graphar\", table_name=\"person\") RETURN *");
    if (!loadResult->isSuccess())
    {
        std::cerr << loadResult->getErrorMessage() << std::endl;
        std::filesystem::remove_all("test");
        return -1;
    }

    while (loadResult->hasNext())
    {
        auto row = loadResult->getNext();
        std::cout << row->getValue(0)->getValue<int64_t>() << " "
                  << row->getValue(1)->getValue<string>() << " "
                  << row->getValue(2)->getValue<string>() << " "
                  << row->getValue(3)->getValue<string>() << std::endl;
    }

    std::filesystem::remove_all("test");

    return 0;
}