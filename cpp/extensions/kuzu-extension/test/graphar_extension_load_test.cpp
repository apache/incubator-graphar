#include <iostream>
#include <filesystem>
#include "kuzu.hpp"

using namespace kuzu::main;

int main()
{
    // Create an empty on-disk database and connect to it
    SystemConfig systemConfig(
        /*bufferPoolSize=*/-1u,
        /*maxNumThreads=*/16,
        /*enableCompression=*/true,
        /*readOnly=*/false);
    auto database = std::make_unique<Database>("test", systemConfig);

    // Connect to the database.
    auto connection = std::make_unique<Connection>(database.get());

    // auto loadResult = connection->query("LOAD graphar;");
    auto result = connection->query("LOAD EXTENSION \"xxx/libgraphar.kuzu_extension\";");
    if (!result->isSuccess())
    {
        std::cerr << result->getErrorMessage() << std::endl;
        std::filesystem::remove_all("test");
        return -1;
    }

    // Load explain
    auto loadExplainResult = connection->query("EXPLAIN LOAD FROM \"xxx/ldbc_sample.graph.yml\" (file_format=\"graphar\", table_name=\"person\") RETURN *");
    if (!loadExplainResult->isSuccess())
    {
        std::cerr << loadExplainResult->getErrorMessage() << std::endl;
        std::filesystem::remove_all("test");
        return -1;
    }

    while (loadExplainResult->hasNext())
    {
        auto row = loadExplainResult->getNext();
        std::cout << row->getValue(0)->getValue<std::string>() << std::endl;
    }

    // Load data.
    auto loadResult = connection->query("LOAD FROM \"xxx/ldbc_sample.graph.yml\" (file_format=\"graphar\", table_name=\"person\") RETURN *");
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
                  << row->getValue(1)->getValue<int64_t>() << " "
                  << row->getValue(2)->getValue<std::string>() << " "
                  << row->getValue(3)->getValue<std::string>() << " "
                  << row->getValue(4)->getValue<std::string>() << std::endl;
    }

    std::filesystem::remove_all("test");

    return 0;
}