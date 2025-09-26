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
    auto loadResult = connection->query("LOAD EXTENSION \"/home/gary/.kuzu/extension/0.10.0/linux_amd64/graphar/libgraphar.kuzu_extension\";");
    if (!loadResult->isSuccess())
    {
        std::cerr << loadResult->getErrorMessage() << std::endl;
        std::filesystem::remove_all("test");
        return -1;
    }

    // Create the schema.
    auto createTableResult = connection->query("CREATE NODE TABLE Person(id INT64, firstName STRING, lastName STRING, gender STRING, PRIMARY KEY (id))");
    if (!createTableResult->isSuccess())
    {
        std::cerr << createTableResult->getErrorMessage() << std::endl;
        std::filesystem::remove_all("test");
        return -1;
    }

    // Copy explain
    // auto copyExplainResult = connection->query("EXPLAIN COPY Person FROM \"/home/gary/incubator-graphar/cpp/build-debug/testing/ldbc_sample_timestamp/parquet/ldbc_sample_timestamp.graph.yml\" (file_format=\"graphar\", table_name=\"person\")");
    auto copyExplainResult = connection->query("EXPLAIN COPY Person FROM \"/home/gary/incubator-graphar/cpp/build-debug/testing/ldbc_sample/parquet/ldbc_sample.graph.yml\" (file_format=\"graphar\", table_name=\"person\")");
    // auto copyExplainResult = connection->query("EXPLAIN COPY Person FROM \"/home/gary/incubator-graphar/cpp/build-debug/testing/ldbc_sample/json/LdbcSample.graph.yml\" (file_format=\"graphar\", table_name=\"Person\")");
    if (!copyExplainResult->isSuccess())
    {
        std::cerr << copyExplainResult->getErrorMessage() << std::endl;
        std::filesystem::remove_all("test");
        return -1;
    }

    while (copyExplainResult->hasNext())
    {
        auto row = copyExplainResult->getNext();
        std::cout << row->getValue(0)->getValue<string>() << std::endl;
    }

    // Load data.
    auto copyResult = connection->query("COPY Person FROM \"/home/gary/incubator-graphar/cpp/build-debug/testing/ldbc_sample/parquet/ldbc_sample.graph.yml\" (file_format=\"graphar\", table_name=\"person\")");
    // auto copyResult = connection->query("COPY Person FROM \"/home/gary/incubator-graphar/cpp/build-debug/testing/ldbc_sample_timestamp/parquet/ldbc_sample_timestamp.graph.yml\" (file_format=\"graphar\", table_name=\"person\")");
    // auto copyResult = connection->query("COPY Person FROM \"/home/gary/incubator-graphar/cpp/build-debug/testing/ldbc_sample/json/LdbcSample.graph.yml\" (file_format=\"graphar\", table_name=\"Person\")");
    if (!copyResult->isSuccess())
    {
        std::cerr << copyResult->getErrorMessage() << std::endl;
        std::filesystem::remove_all("test");
        return -1;
    }

    // Execute a simple query.
    auto result =
        connection->query("MATCH (p:Person) RETURN p.id, p.firstName, p.lastName, p.gender;");

    // Output query result.
    while (result->hasNext())
    {
        auto row = result->getNext();
        std::cout << row->getValue(0)->getValue<int64_t>() << " "
                  << row->getValue(1)->getValue<string>() << " "
                  << row->getValue(2)->getValue<string>() << " "
                  << row->getValue(3)->getValue<string>() << std::endl;
    }

    /* EDGE */
    // connection->query("CREATE REL TABLE KNOWS(FROM Person TO Person, creationDate STRING)");
    connection->query("CREATE REL TABLE KNOWS(FROM Person TO Person, creationDate TIMESTAMP)");

    // Copy edge explain
    auto copyEdgeExplainResult = connection->query("EXPLAIN COPY KNOWS FROM \"/home/gary/incubator-graphar/cpp/build-debug/testing/ldbc_sample_timestamp/parquet/ldbc_sample_timestamp.graph.yml\" (file_format=\"graphar\", table_name=\"person_knows_person\")");
    // auto copyEdgeExplainResult = connection->query("EXPLAIN COPY KNOWS FROM \"/home/gary/incubator-graphar/cpp/build-debug/testing/ldbc_sample/parquet/ldbc_sample.graph.yml\" (file_format=\"graphar\", table_name=\"person_knows_person\")");
    // auto copyEdgeExplainResult = connection->query("EXPLAIN COPY KNOWS FROM \"/home/gary/incubator-graphar/cpp/build-debug/testing/ldbc_sample/json/LdbcSample.graph.yml\" (file_format=\"graphar\", table_name=\"Person_Knows_Person\")");
    if (!copyEdgeExplainResult->isSuccess())
    {
        std::cerr << copyEdgeExplainResult->getErrorMessage() << std::endl;
        std::filesystem::remove_all("test");
        return -1;
    }

    auto copyEdgeResult = connection->query("COPY KNOWS FROM \"/home/gary/incubator-graphar/cpp/build-debug/testing/ldbc_sample_timestamp/parquet/ldbc_sample_timestamp.graph.yml\" (file_format=\"graphar\", table_name=\"person_knows_person\")");
    // auto copyEdgeResult = connection->query("COPY KNOWS FROM \"/home/gary/incubator-graphar/cpp/build-debug/testing/ldbc_sample/parquet/ldbc_sample.graph.yml\" (file_format=\"graphar\", table_name=\"person_knows_person\")");
    // auto copyEdgeResult = connection->query("COPY KNOWS FROM \"/home/gary/incubator-graphar/cpp/build-debug/testing/ldbc_sample/json/LdbcSample.graph.yml\" (file_format=\"graphar\", table_name=\"Person_Knows_Person\")");
    if (!copyEdgeResult->isSuccess())
    {
        std::cerr << copyEdgeResult->getErrorMessage() << std::endl;
        std::filesystem::remove_all("test");
        return -1;
    }

    auto edgeResult =
        connection->query("MATCH (a:Person)-[k:KNOWS]->(b:Person) RETURN a.id, k.creationDate, b.id;");
    while (edgeResult->hasNext())
    {
        auto row = edgeResult->getNext();
        std::cout << row->getValue(0)->getValue<int64_t>() << " "
                //   << row->getValue(1)->getValue<string>() << " "
                  << row->getValue(1)->getValue<int64_t>() << " "
                  << row->getValue(2)->getValue<int64_t>() << std::endl;
    }

    std::filesystem::remove_all("test");

    return 0;
}