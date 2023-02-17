Co-Work with Apache Spark
============================

`Apache Spark <https://spark.apache.org/>`_ is a multi-language engine for executing data engineering, data science, and machine learning on single-node machines or clusters. The GraphAr Spark library is developed to make integrating graphs in GraphAr with Spark easy. Utilizing this library, one can generate, load and transform GAR files efficiently. One can also use it to integrate GraphAr to other systems that work with Spark.

Some examples are provided as the showcases for co-working with Apache Spark.


Examples
------------------------

Transform GAR files
`````````````````````
`TransformExample.scala`_ is an example for graph data conversion between different file types or different adjList types. For such cases, the original data will be first loaded as the Spark DataFrame using the GraphAr Spark Reader; and then, this DataFrame is written into generated GAR files through a GraphAr Spark Writer, following the meta data defined in a new information file.


Compute with GraphX
`````````````````````
Another important scenario of using GraphAr is to take it as the data source for conducting graph computing or analytics. `ComputeExample.scala`_  includes an example for constructing the GraphX graph from GAR files and executing a connected-components computation. Also, executing queries with Spark SQL or running other graph analytic algorithms could be implemented in a similar way.


Importing/Exporting Graphs of Neo4j
``````````````````````````````````````````
`Neo4j <https://neo4j.com/product/neo4j-graph-database/>`_ graph database provides a `spark connector <https://neo4j.com/docs/spark/current/overview/>`_ to integrate with Spark. Neo4j Spark Connector together with GraphAr Spark library enable us to migrate the graph data between Neo4j and GraphAr. This also a critical application case of GraphAr in which it acts as a persistent storage of the graph data in database.

When exporting the graph data from Neo4j to GraphAr, please refer to the following code:

.. code:: scala

  // connect to the Neo4j instance
  val spark = SparkSession.builder()
    .config("neo4j.url", "bolt://localhost:7687")
    .config("neo4j.authentication.type", "basic")
    .config("neo4j.authentication.basic.username", sys.env.get("Neo4j_USR").get)
    .config("neo4j.authentication.basic.password", sys.env.get("Neo4j_PWD").get)
    .config("spark.master", "local")
    .getOrCreate()
  
  // read vertices with label "Person" from Neo4j as a DataFrame
  val person_df = spark.read.format("org.neo4j.spark.DataSource")
    .option("labels", "Person")
    .load()

  // construct the GraphAr Spark writer
  val person_df_with_index = IndexGenerator.generateVertexIndexColumn(person_df)
  val vertex_info = ...     // the user-provided meta information
  val prefix : String = ... // the prefix of the file path
  val writer = new VertexWriter(prefix, vertex_info, person_df_with_index)

  // use the DataFrame to generate GAR files
  writer.writeVertexProperties() 

See `Neo4j2GraphAr.scala`_ for the complete example.

The information file for the this group of vertices looks like: 

.. code:: Yaml

  label: Person
  chunk_size: 100
  prefix: vertex/person/
  property_groups:
    - properties:
        - name: <id>
          data_type: int64
          is_primary: false
        - name: <labels>
          data_type: array
          is_primary: false
      file_type: parquet
    - properties:
        - name: name
          data_type: string
          is_primary: true
        - name: born
          data_type: int64
          is_primary: false
      file_type: orc
  version: gar/v1

.. note::

  Please note that when reading data from Neo4j with this method, the DataFrame contains all the fields contained in the nodes ((vertex properties)), plus two additional columns:

  - <id> the internal Neo4j ID
  - <labels> a list of labels for that node

And when importing data from GraphAr to create/update instances in Neo4j, please refer to the following code:

.. code:: scala
  
  // construct the GraphAr Spark reader
  val spark_session = ...   // the spark session
  val prefix : String = ... // the prefix of the file path
  val reader = new VertexReader(prefix, vertex_info, spark_session)

  // reading chunks for all property groups
  val vertex_df = reader.readAllVertexPropertyGroups(false)

  // group vertices with the same Neo4j labels together
  val labels_array = vertex_df.select("<labels>").distinct.collect.flatMap(_.toSeq)
  val vertex_df_array = labels_array.map(labels => vertex_df.where(vertex_df("<labels>") === labels))

  // each time, write a group of vertices to Neo4j
  vertex_df_array.foreach(df => {
    val labels = df.first().getAs[Seq[String]]("<labels>")
    var str = ""
    labels.foreach(label => {str += ":" + label})
    df.drop("<id>").drop("<labels>")
      .write.format("org.neo4j.spark.DataSource")
      .mode(SaveMode.Append)
      .option("labels", str)
      .save()
  })

See `GraphAr2Neo4j.scala`_ for the complete example.

.. note::

  The Neo4j Spark Connector provides different save modes and writing options, such as CREATE or MERGE. Please refer to its `documentation <https://neo4j.com/docs/spark/current/writing/>`_ for more information.


.. _TransformExample.scala: https://github.com/alibaba/GraphAr/blob/main/spark/src/test/scala/com/alibaba/graphar/TransformExample.scala

.. _ComputeExample.scala: https://github.com/alibaba/GraphAr/blob/main/spark/src/test/scala/com/alibaba/graphar/ComputeExample.scala

.. _Neo4j2GraphAr.scala: https://github.com/alibaba/GraphAr/blob/main/spark/src/test/scala/com/alibaba/graphar/Neo4j2GraphAr.scala

.. _GraphAr2Neo4j.scala: https://github.com/alibaba/GraphAr/blob/main/spark/src/test/scala/com/alibaba/graphar/GraphAr2Neo4j.scala
