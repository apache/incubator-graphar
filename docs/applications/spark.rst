Co-Work with Apache Spark
============================

`Apache Spark <https://spark.apache.org/>`_ is a multi-language engine for executing data engineering, data science, and machine learning on single-node machines or clusters. The GraphAr Spark library is developed to make the integration of GraphAr with Spark easy. This library allows users to efficiently generate, load, and transform GAR files, and to integrate GraphAr with other Spark-compatible systems.

Examples of this co-working integration have been provided as showcases.


Examples
------------------------

Transform GAR files
`````````````````````
We provide an example in `TestGraphTransformer.scala`_, which demonstrates
how to conduct data transformation at the graph level. `TransformExample.scala`_ is
another example for graph data conversion between different file types or different
adjList types, which is implemented at the vertex/edge table level. To do this,
the original data is first loaded into a Spark DataFrame using the GraphAr Spark Reader.
Then, the DataFrame is written into generated GAR files through a GraphAr Spark Writer,
following the meta data defined in a new information file.


Compute with GraphX
`````````````````````
Another important use case of GraphAr is to use it as a data source for graph
computing or analytics; `ComputeExample.scala`_ provides an example of constructing
a GraphX graph from reading GAR files and executing a connected-components computation.
Also, executing queries with Spark SQL and running other graph analytic algorithms
can be implemented in a similar fashion.


Import/Export graphs of Neo4j
```````````````````````````````
`Neo4j <https://neo4j.com/product/neo4j-graph-database/>`_ graph database provides
a `spark connector <https://neo4j.com/docs/spark/current/overview/>`_ for integration
with Spark. The Neo4j Spark Connector, combined with the GraphAr Spark library,
enables us to migrate graph data between Neo4j and GraphAr. This is also a key application
of GraphAr in which it acts as a persistent storage of graph data in the database.

We provide two example programs that demonstrate how GraphAr can be used in conjunction
with Neo4j. It utilizes one of the built-in Neo4j datasets, the `Movie Graph <https://neo4j.com/developer/example-data/#built-in-examples>`_,
which is a mini graph application containing actors and directors that are related through the movies they have collaborated on.
Given some necessary information like the chunk size, the prefix of the file path, and the file type,
the program can read the graph data from Neo4j and write it into GraphAr files.
When exporting graph data from Neo4j and writing to GraphAr, please refer to the following code,
with `Neo4j2GraphAr.scala`_ providing a complete example.

.. code:: scala

def main(args: Array[String]): Unit = {
    // connect to the Neo4j instance
    val spark = SparkSession.builder()
      .appName("Neo4j to GraphAr for Movie Graph")
      .config("neo4j.url", "bolt://localhost:7687")
      .config("neo4j.authentication.type", "basic")
      .config("neo4j.authentication.basic.username", sys.env.get("Neo4j_USR").get)
      .config("neo4j.authentication.basic.password", sys.env.get("Neo4j_PWD").get)
      .config("spark.master", "local")
      .getOrCreate()

    // initialize a graph writer
    val writer: GraphWriter = new GraphWriter()

    // put movie graph data into writer
    readAndPutDataIntoWriter(writer, spark)

    // write in graphar format
    val outputPath: String = args(0)
    val vertexChunkSize: Long = args(1).toLong
    val edgeChunkSize: Long = args(2).toLong
    val fileType: String = args(3)

    writer.write(outputPath, spark, "MovieGraph", vertexChunkSize, edgeChunkSize, fileType)
}

Additionally, when importing data from GraphAr to create/update instances in Neo4j, please refer to the following code:

.. code:: scala

def main(args: Array[String]): Unit = {
    // connect to the Neo4j instance
    val spark = SparkSession.builder()
      .appName("GraphAr to Neo4j for Movie Graph")
      .config("neo4j.url", "bolt://localhost:7687")
      .config("neo4j.authentication.type", "basic")
      .config("neo4j.authentication.basic.username", sys.env.get("Neo4j_USR").get)
      .config("neo4j.authentication.basic.password", sys.env.get("Neo4j_PWD").get)
      .config("spark.master", "local")
      .getOrCreate()

    val graphInfoPath: String = args(0)
    val graphInfo = GraphInfo.loadGraphInfo(graphInfoPath, spark)

    val graphData = GraphReader.read(graphInfoPath, spark)
    val vertexData = graphData._1
    val edgeData = graphData._2

    putVertexDataIntoNeo4j(graphInfo, vertexData, spark)
    putEdgeDataIntoNeo4j(graphInfo, vertexData, edgeData, spark)
}

See `GraphAr2Neo4j.scala`_ for the complete example.

.. tip::

  - The Neo4j Spark Connector offers different save modes and writing options, such as Append(CREATE) or Overwrite(MERGE). Please refer to its `documentation <https://neo4j.com/docs/spark/current/writing/>`_ for more information and take the most appropriate method while using.
  - The Neo4j Spark Connector supports to use `Spark structured streaming API <https://neo4j.com/docs/spark/current/streaming/>`_, which works differently from Spark batching. One can utilize this API to read/write a stream from/to Neo4j, avoiding to maintain all data in the memory.


.. _TestGraphTransformer.scala: https://github.com/alibaba/GraphAr/blob/main/spark/src/test/scala/com/alibaba/graphar/TestGraphTransformer.scala

.. _TransformExample.scala: https://github.com/alibaba/GraphAr/blob/main/spark/src/test/scala/com/alibaba/graphar/TransformExample.scala

.. _ComputeExample.scala: https://github.com/alibaba/GraphAr/blob/main/spark/src/test/scala/com/alibaba/graphar/ComputeExample.scala

.. _Neo4j2GraphAr.scala: https://github.com/alibaba/GraphAr/blob/main/spark/src/main/scala/com/alibaba/graphar/example/Neo4j2GraphAr.scala

.. _GraphAr2Neo4j.scala: https://github.com/alibaba/GraphAr/blob/main/spark/src/main/scala/com/alibaba/graphar/example/GraphAr2Neo4j.scala
