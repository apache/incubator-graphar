GraphAr Spark Tools
============================

Overview
-----------

GraphAr Spark tools are provided as a library for generating, loading and transforming GAR files with Apache Spark easy. It consists of the following parts:

- **Information Classes**: As same with in C++ SDK, the information classes are implemented as a part of Spark tools for constructing and accessing the meta information about the graphs, vertices and edges in GraphAr.
- **IndexGenerator**: The IndexGenerator helps to generate the indices for vertex/edge DataFrames. In most cases, IndexGenerator is first utilized to generate the indices for a DataFrame (e.g., from primary keys), and then this DataFrame can be written into GAR files through the Writer.
- **Writer**: The GraphAr Spark Writer provides a set of interfaces that can be used to write Spark DataFrames into GAR files. Every time it takes a DataFrame as the logical table for a type of vertices or edges, assembles the data in specified format (e.g., reorganize the edges in the CSR way) and then dumps it to standard GAR files (orc, parquet or CSV files) under the specific directory path.
- **Reader**: The GraphAr Spark Reader provides a set of interfaces that can be used to read GAR files. It reads a set of vertices or edges at a time and assembles the result into Spark DataFrames. Similar with the Reader SDK in C++, it supports the users to specify the data they need, e.g., to read a single property group instead of all properties.
 
Use Cases
----------

The GraphAr Spark Tools can be applied to these scenarios:

- Take GAR as data sources to execute SQL queries or do graph processing (e.g., using GraphX).
- Transform data between GAR and other data sources (e.g., Hive, Neo4j, NebulaGraph, ...).
- Transform GAR data between different file types (e.g., from ORC to parquet).
- Transform GAR data between different adjList types (e.g., from COO to CSR).
- Modify existing GAR data (e.g., add new vertices/edges).


Get GraphAr Spark Tools
------------------------------

Make the graphar-spark-tools directory as the current working directory:

.. code-block:: shell

   cd spark/

Compile package:

.. code-block:: shell

   mvn package

After compilation, a similar file *graphar-0.1.0-SNAPSHOT-shaded.jar* is generated in the directory *spark/target/*.


How to Use
-----------------

Information Classes
`````````````````````
The information classes are included in Spark tools for constructing and accessing the meta information about the graphs, vertices and edges in GraphAr. They are also used as the essential parameters for constructing readers/writers. In common cases, the information can be built from reading and parsing existing meta files (Yaml files). Also, we support to construct them in memory from nothing.

To build information from Yaml files, please refer to the following code.

.. code:: scala

   // read graph yaml and build information
   val file_path = ... // the path to the yaml file
   val yaml_path = new Path(file_path)
   val fs = FileSystem.get(yaml_path.toUri(), spark.sparkContext.hadoopConfiguration)
   val input = fs.open(yaml_path)
   val graph_yaml = new Yaml(new Constructor(classOf[GraphInfo]))
   val graph_info = graph_yaml.load(input).asInstanceOf[GraphInfo]

   // use information classes
   val vertices = graph_info.getVertices
   val edges = graph_info.getEdges
   val version = graph_info.getVersion

See `TestGraphInfo.scala`_ for the complete example.


IndexGenerator
``````````````````
As introduced in GraphAr file format, each vertex is assigned with a unique index (vertex id) in GraphAr, which starts from 0 and increases continuously for each type of vertices (i.e., with the same label). While in Spark, the vertex/edge tables are often lack of this information. For example, an edge table uses the primary key "id" (which is a string) to identify the source and destination vertex for the edges.

The GraphAr Spark IndexGenerator helps to generate the indices for vertex/edge DataFrames. For a vertex DataFrame, one can construct a mapping from the primary keys to the GAR indices, or if without primary keys, an index column can be generated directly. And for an edge DataFrame, source and destination columns can be generated from the vertex index mapping (when the end vertices are represented by the primary keys), or they can also be generated directly without the mapping.

.. tip::
   In most cases, IndexGenerator is first utilized to generate the indices for a DataFrame, and then this DataFrame can be written into GAR files through the Writer.

When using the IndexGenerator, please refer to the following code.

.. code:: scala

   // generate indices for vertex DataFrame
   val vertex_df = ...
   val vertex_df_with_index = IndexGenerator.generateVertexIndexColumn(vertex_df)

   // generate indices for src & dst columns of edge DataFrame
   val edge_df = ...
   val edge_df_with_index = IndexGenerator.generateSrcAndDstIndexUnitedlyForEdges(edge_df, "src", "dst")

   // generate indices for src & dst columns of edge DataFrame from vertex primary keys
   val vertex_df = ...
   val edge_df = ...
   val vertex_mapping = IndexGenerator.constructVertexIndexMapping(vertex_df, "id")
   val edge_df_src_index = IndexGenerator.generateSrcIndexForEdgesFromMapping(edge_df, "src", vertex_mapping)
   val edge_df_src_dst_index = IndexGenerator.generateDstIndexForEdgesFromMapping(edge_df_src_index, "dst", vertex_mapping)

See `TestIndexGenerator.scala`_ for the complete example.


Writer
``````````````````
GraphAr Spark Writer provides the Spark interfaces which can be used to write DataFrames into GraphAr formatted files in a batch-import way. For writing vertex property chunks through the VertexWriter, the users can specify a property group, or, all property groups will be written into corresponding chunks.

As for the edge chunks, besides the meta data (edge info), the adjList type should also be specified. One may choose to write the adjList/properties only, or write all of the adjList, properties and the offset (for CSR and CSC format only) chunks at the same time.

When using the GAR Spark Writer, please refer to the following code.

.. code:: scala

   // generate the vertex index column for vertex dataframe
   val vertex_df = ...
   val vertex_df_with_index = IndexGenerator.generateVertexIndexColumn(vertex_df)
   // construct the vertex writer
   val vertex_info = ...
   val prefix = ...
   val writer = new VertexWriter(prefix, vertex_info, vertex_df_with_index)
   // write certain property group
   val property_group = vertex_info.getPropertyGroup("id")
   writer.writeVertexProperties(property_group)
   // write all properties
   writer.writeVertexProperties()

   // generate vertex index for edge dataframe
   val edge_df = ...
   val edge_df_with_index = IndexGenerator.generateSrcAndDstIndexUnitedlyForEdges(edge_df, "src", "dst")
   // construct the edge writer
   val edge_info = ...
   val adj_list_type = AdjListType.ordered_by_source
   val writer = new EdgeWriter(prefix, edge_info, adj_list_type, edge_df_with_index)
   // write adjList
   writer.writeAdjList()
   // write certain property group
   val property_group = edge_info.getPropertyGroup("creationDate", adj_list_type)
   writer.writeEdgeProperties(property_group)
   // write all of adjList and properties
   writer.writeEdges()

See `TestWriter.scala`_ for the complete example.


Reader
``````````````````
The GraphAr Spark Reader provides a set of interfaces that can be used to read GAR files. It reads a type of vertices or edges at a time and assembles the result into a Spark DataFrame. Similar with the Reader SDK in C++, it supports the users to specify the data they need, e.g., a single property group.

After reading the GAR files into the Spark DataFrame, the users can utilize it to do graph processing, execute SQL queries, or conduct some transformations (e.g., add new vertices/edges, change the file type, or reorganize the order of edges) on it and then write it again into GAR files if required.

When using the GAR Spark Reader, please refer to the following code.

.. code:: scala

   // construct the vertex reader
   val prefix = ...
   val vertex_info = ...
   val reader = new VertexReader(prefix, vertex_info, spark)
   val property_group = vertex_info.getPropertyGroup("gender")
   // read a single chunk
   val single_chunk_df = reader.readVertexPropertyChunk(property_group, 0)
   // ...
   // read all property chunks
   val vertex_df = reader.readAllVertexProperties()

   //construct the edge reader
   val edge_info = ...
   val adj_list_type = AdjListType.ordered_by_source
   val reader = new EdgeReader(prefix, edge_info, adj_list_type, spark)
   // read a single adjList chunk
   val single_adj_list_df = reader.readAdjListChunk(2, 0)
   // read all adjList chunks for a vertex chunk
   val adj_list_df_chunk_2 = reader.readAdjListForVertexChunk(2)
   // ...
   // read all edge chunks (including adjList and all properties)
   val edge_df = reader.readEdges()

See `TestReader.scala`_ for the complete example.

More examples
``````````````````
For more information on usage, please refer to the examples:

- `ComputeExample.scala`_  includes an example to construct the graph for GraphX from reading GAR files and then run a algorithm to compute connected components;
- `TransformExample.scala`_ is an example to show the usage of transforming the graph data between different file types or adjList types.


.. _TestGraphInfo.scala: https://github.com/alibaba/GraphAr/blob/main/spark/src/test/scala/com/alibaba/graphar/TestGraphInfo.scala

.. _TestIndexGenerator.scala: https://github.com/alibaba/GraphAr/blob/main/spark/src/test/scala/com/alibaba/graphar/TestIndexGenerator.scala

.. _TestWriter.scala: https://github.com/alibaba/GraphAr/blob/main/spark/src/test/scala/com/alibaba/graphar/TestWriter.scala

.. _TestReader.scala: https://github.com/alibaba/GraphAr/blob/main/spark/src/test/scala/com/alibaba/graphar/TestReader.scala

.. _ComputeExample.scala: https://github.com/alibaba/GraphAr/blob/main/spark/src/test/scala/com/alibaba/graphar/ComputeExample.scala

.. _TransformExample.scala: https://github.com/alibaba/GraphAr/blob/main/spark/src/test/scala/com/alibaba/graphar/TransformExample.scala