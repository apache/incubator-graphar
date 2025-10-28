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
    auto loadResult = connection->query("LOAD EXTENSION \"xxx/libgraphar.kuzu_extension\";");
    if (!loadResult->isSuccess())
    {
        std::cerr << loadResult->getErrorMessage() << std::endl;
        std::filesystem::remove_all("test");
        return -1;
    }

    // Create the schema.
    connection->query("CREATE NODE TABLE Person(internal_id INT64, id INT64, firstName STRING, lastName STRING, gender STRING, PRIMARY KEY (id))");

    // Copy explain
    auto copyExplainResult = connection->query("EXPLAIN COPY Person FROM \"xxx/ldbc_sample.graph.yml\" (file_format=\"graphar\", table_name=\"person\")");
    while (copyExplainResult->hasNext())
    {
        auto row = copyExplainResult->getNext();
        std::cout << row->getValue(0)->getValue<std::string>() << std::endl;
    }

    // Load data.
    auto copyResult = connection->query("COPY Person FROM \"xxx/ldbc_sample.graph.yml\" (file_format=\"graphar\", table_name=\"person\")");
    if (!copyResult->isSuccess())
    {
        std::cerr << copyResult->getErrorMessage() << std::endl;
        std::filesystem::remove_all("test");
        return -1;
    }

    // Execute a simple query.
    auto result =
        connection->query("MATCH (p:Person) RETURN p.internal_id, p.id, p.firstName, p.lastName, p.gender ORDER BY p.internal_id ASC LIMIT 10;");
        // connection->query("MATCH (p:Person) RETURN p.internal_id, p.id, p.firstName, p.lastName, p.gender LIMIT 10;");
    while (result->hasNext())
    {
        auto row = result->getNext();
        std::cout << row->getValue(0)->getValue<int64_t>() << " "
                  << row->getValue(1)->getValue<int64_t>() << " "
                  << row->getValue(2)->getValue<std::string>() << " "
                  << row->getValue(3)->getValue<std::string>() << " "
                  << row->getValue(4)->getValue<std::string>() << std::endl;
    }

    // export to gar data.
    auto exportPlan = connection->query("EXPLAIN COPY (MATCH (p:Person) RETURN p.id as id, p.firstName as firstName, p.lastName as lastName, p.gender as gender ORDER BY p.internal_id ASC) TO \"xxx/ldbc_sample.graph.yml.graphar\" (table_name=\"person\")");
    // auto exportPlan = connection->query("EXPLAIN COPY (MATCH (p:Person) RETURN p.id as id, p.firstName as firstName, p.lastName as lastName, p.gender as gender) TO \"xxx/ldbc_sample.graph.yml.graphar\" (table_name=\"person\")");
    if (!exportPlan->isSuccess())
    {
        std::cerr << exportPlan->getErrorMessage() << std::endl;
        std::filesystem::remove_all("test");
        return -1;
    }

    while (exportPlan->hasNext())
    {
        auto row = exportPlan->getNext();
        std::cout << row->getValue(0)->getValue<std::string>() << std::endl;
    }

    auto exportResult = connection->query("COPY (MATCH (p:Person) RETURN p.id as id, p.firstName as firstName, p.lastName as lastName, p.gender as gender ORDER BY p.internal_id ASC) TO \"xxx/ldbc_sample.graph.yml.graphar\" (table_name=\"person\", target_dir=\"xxx/copyToTestSet/\")");
    // auto exportResult = connection->query("COPY (MATCH (p:Person) RETURN p.id as id, p.firstName as firstName, p.lastName as lastName, p.gender as gender) TO \"xxx/ldbc_sample.graph.yml.graphar\" (table_name=\"person\", target_dir=\"xxx/copyToTestSet/\")");
    if (!exportResult->isSuccess())
    {
        std::cerr << exportResult->getErrorMessage() << std::endl;
        std::filesystem::remove_all("test");
        return -1;
    }
    std::cout << "Vertex exported successfully." << std::endl;

    /* EDGE */
    connection->query("CREATE REL TABLE KNOWS(FROM Person TO Person, creationDate STRING)");

    auto copyEdgeResult = connection->query("COPY KNOWS FROM \"xxx/ldbc_sample.graph.yml\" (file_format=\"graphar\", table_name=\"person_knows_person\")");
    if (!copyEdgeResult->isSuccess())
    {
        std::cerr << copyEdgeResult->getErrorMessage() << std::endl;
        std::filesystem::remove_all("test");
        return -1;
    }

    auto edgeResult =
        connection->query("MATCH (a:Person)-[k:KNOWS]->(b:Person) RETURN a.id AS from, a.internal_id AS internal_from, k.creationDate AS creationDate, b.id AS to, b.internal_id AS internal_to ORDER BY internal_from ASC, internal_to ASC LIMIT 10;");
    while (edgeResult->hasNext())
    {
        auto row = edgeResult->getNext();
        std::cout << row->getValue(0)->getValue<int64_t>() << " "
                  << row->getValue(1)->getValue<int64_t>() << " "
                  << row->getValue(2)->getValue<std::string>() << " "
                  << row->getValue(3)->getValue<int64_t>() << " "
                  << row->getValue(4)->getValue<int64_t>() << std::endl;
    }

    // export edge to gar data.
    auto exportEdgePlan = connection->query("EXPLAIN COPY (MATCH (a:Person)-[k:KNOWS]->(b:Person) RETURN a.internal_id as from, k.creationDate as creationDate, b.internal_id as to) TO \"xxx/ldbc_sample.graph.yml.graphar\" (table_name=\"person_knows_person\", target_dir=\"xxx/copyToTestSet/\")");
    if (!exportEdgePlan->isSuccess())
    {
        std::cerr << exportEdgePlan->getErrorMessage() << std::endl;
        std::filesystem::remove_all("test");
        return -1;
    }

    while (exportEdgePlan->hasNext())
    {
        auto row = exportEdgePlan->getNext();
        std::cout << row->getValue(0)->getValue<std::string>() << std::endl;
    }

    auto exportEdgeResult = connection->query("COPY (MATCH (a:Person)-[k:KNOWS]->(b:Person) RETURN a.internal_id as from, k.creationDate as creationDate, b.internal_id as to) TO \"xxx/ldbc_sample.graph.yml.graphar\" (table_name=\"person_knows_person\", target_dir=\"xxx/copyToTestSet/\")");
    if (!exportEdgeResult->isSuccess())
    {
        std::cerr << exportEdgeResult->getErrorMessage() << std::endl;
        std::filesystem::remove_all("test");
        return -1;
    }

    std::cout << "Edge exported successfully." << std::endl;

    std::filesystem::remove_all("test");
    return 0;
}