GraphAr
========

|GraphAr CI| |Docs CI| |GraphAr Docs|

Welcome to GraphAr (short for "Graph Archive"), an open source, standardized file format for graph data storage and retrieval.

What is GraphAr?
-----------------

|Overview Pic|

Graph processing serves as the essential building block for a diverse variety of
real-world applications such as social network analytics, data mining, network routing,
and scientific computing.

GraphAr is a project that aims to make it easier for diverse applications and
systems (in-memory and out-of-core storages, databases, graph computing systems, and interactive graph query frameworks)
to build and access graph data conveniently and efficiently.

It can be used for importing/exporting and persistent storage of graph data,
thereby reducing the burden on systems when working together. Additionally, it can
serve as a direct data source for graph processing applications.

To achieve this, GraphAr provides:

- The Graph Archive(GAR) file format: a standardized system-independent file format for storing graph data
- Libraries: a set of libraries for reading, writing and transforming GAR files

By using GraphAr, you can:

- Store and persist your graph data in a system-independent way with the GAR file format
- Easily access and generate GAR files using the libraries
- Utilize Apache Spark to quickly manipulate and transform your GAR files

The GAR File Format
-------------------
The GAR file format is designed for storing property graphs. It uses metadata to
record all the necessary information of a graph, and maintains the actual data in
a chunked way.

A property graph consists of vertices and edges, with each vertex contains:

- A unique identifier (called vertex id or vertex index).
- A text label that describes the vertex type.
- A collection of properties, with each property can be represented by a key-value pair.

And each edge contains:

- A unique identifier (called edge id or edge index).
- The outgoing vertex (source).
- The incoming vertex (destination).
- A text label that describes the relationship between the two vertices.
- A collection of properties.

The following is an example property graph containing two types of vertices ("person" and "comment") and three types of edges.

|Property Graph|

Vertices in GraphAr
^^^^^^^^^^^^^^^^^^^

Logical table of vertices
""""""""""""""""""""""""""

Each type of vertices (with the same label) constructs a logical vertex table, with each vertex assigned with a global index (vertex id) starting from 0, corresponding to the row number of the vertex in the logical vertex table. An example layout for a logical table of vertices under the label "person" is provided for reference.

Given a vertex id and the vertex label, a vertex is uniquely identifiable and its respective properties can be accessed from this table. The vertex id is further used to identify the source and destination vertices when maintaining the topology of the graph.

|Vertex Logical Table|

Physical table of vertices
""""""""""""""""""""""""""

The logical vertex table will be partitioned into multiple continuous vertex chunks for enhancing the reading/writing efficiency. To maintain the ability of random access, the size of vertex chunks for the same label is fixed. To support to access required properties avoiding reading all properties from the files, and to add properties for vertices without modifying the existing files, the columns of the logical table will be divided into several column groups.

Take the "person" vertex table as an example, if the chunk size is set to be 500, the logical table will be separated into sub-logical-tables of 500 rows with the exception of the last one, which may have less than 500 rows. The columns for maintaining properties will also be divided into distinct groups (e.g., 2 for our example). As a result, a total of 4 physical vertex tables are created for storing the example logical table, which can be seen from the following figure.

|Vertex Physical Table|


Edges in GraphAr
^^^^^^^^^^^^^^^^

Logical table of edges
""""""""""""""""""""""""""

For maintaining a type of edges (that with the same triplet of the source label, edge label, and destination label), a logical edge table is established.  And in order to support quickly creating a graph from the graph storage file, the logical edge table could maintain the topology information in a way similar to CSR/CSC (learn more about `CSR/CSC <https://en.wikipedia.org/wiki/Sparse_matrix>`_), that is, the edges are ordered by the vertex id of either source or destination. In this way, an offset table is required to store the start offset for each vertex's edges, and the edges with the same source/destination will be stored continuously in the logical table.

Take the logical table for "person likes person" edges as an example, the logical edge table looks like:

|Edge Logical Table|


Physical table of edges
""""""""""""""""""""""""""

According to the partition strategy and the order of the edges, edges can be one of the four types: **ordered_by_source**, **ordered_by_dest**, **unordered_by_source** or **unordered_by_dest**. A logical edge table could contain physical tables of three categories:

- The adjList table (which contains only two columns: the vertex id of the source and the destination).
- The edge property tables (if there are properties on edges).
- The offset table (optional, only required for ordered edges).

Since the vertex table are partitioned into multiple chunks, the logical edge table is also partitioned into some sub-logical-tables, with each sub-logical-table contains edges that the source (if the type is **ordered_by_source** or **unordered_by_source**) or destination (if the type is **ordered_by_dest** or **unordered_by_dest**) vertices are in the same vertex chunk. After that, a sub-logical-table is further divided into edge chunks of a predefined, fixed number of rows (referred to as edge chunk size). Finally, an edge chunk is separated into an adjList table and 0 or more property tables.

Additionally, the partition of the offset table should be in alignment with the partition of the corresponding vertex table. The first row of each offset chunk is always 0, indicating the starting point for the corresponding sub-logical-table for edges.

Take the "person knows person" edges to illustrate. Suppose the vertex chunk size is set to 500 and the edge chunk size is 1024, the edges will be saved in the following physical tables:

|Edge Physical Table1|
|Edge Physical Table2|


Libraries
----------

Libraries are provided for reading, writing and transforming files in GraphAr,
now the C++ library and the Spark library are available. And we are going to
provide libraries for more programming languages.

The C++ Library
^^^^^^^^^^^^^^^
See `GraphAr C++ Library`_ for details about the building of the C++ library.

The Spark Library
^^^^^^^^^^^^^^^^^

See `GraphAr Spark Library`_ for details about the Spark library.


Contributing to GraphAr
----------------------------

See `Contribution Guide`_ for details on submitting patches and the contribution workflow.

License
-------

**GraphAr** is distributed under `Apache License 2.0`_. Please note that
third-party libraries may not have the same license as GraphAr.


.. _Apache License 2.0: https://github.com/alibaba/GraphAr/blob/main/LICENSE

.. |GraphAr CI| image:: https://github.com/alibaba/GraphAr/actions/workflows/ci.yml/badge.svg
   :target: https://github.com/alibaba/GraphAr/actions

.. |Docs CI| image:: https://github.com/alibaba/GraphAr/actions/workflows/docs.yml/badge.svg
   :target: https://github.com/alibaba/GraphAr/actions

.. |GraphAr Docs| image:: https://img.shields.io/badge/docs-latest-brightgreen.svg
   :target: https://alibaba.github.io/GraphAr/

.. |Overview Pic| image:: https://alibaba.github.io/GraphAr/_images/overview.png
  :width: 650
  :alt: Overview

.. |Property Graph| image:: https://alibaba.github.io/GraphAr/_images/property_graph.png
  :width: 650
  :alt: property graph

.. |Vertex Logical Table| image:: https://alibaba.github.io/GraphAr/_images/vertex_logical_table.png
  :width: 650
  :alt: vertex logical table

.. |Vertex Physical Table| image:: https://alibaba.github.io/GraphAr/_images/vertex_physical_table.png
  :width: 650
  :alt: vertex physical table

.. |Edge Logical Table| image:: https://alibaba.github.io/GraphAr/_images/edge_logical_table.png
  :width: 650
  :alt: edge logical table

.. |Edge Physical Table1| image:: https://alibaba.github.io/GraphAr/_images/edge_physical_table1.png
  :width: 650
  :alt: edge logical table1

.. |Edge Physical Table2| image:: https://alibaba.github.io/GraphAr/_images/edge_physical_table2.png
  :width: 650
  :alt: edge logical table2

.. _GraphAr File Format: https://alibaba.github.io/GraphAr/user-guide/file-format.html

.. _GraphAr Spark Library: https://github.com/alibaba/GraphAr/tree/main/spark

.. _GraphAr C++ Library: https://github.com/alibaba/GraphAr/tree/main/cpp

.. _example files: https://github.com/GraphScope/gar-test/blob/main/ldbc_sample/

.. _Contribution Guide: https://alibaba.github.io/GraphAr/user-guide/contributing.html

.. _GitHub Issues: https://github.com/alibaba/GraphAr/issues/new

.. _Github Discussions: https://github.com/alibaba/GraphAr/discussions
