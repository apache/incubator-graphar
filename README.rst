.. raw:: html

    <h1 align="center" style="clear: both;">
        <img src="docs/images/graphar-logo.svg" width="350" alt="GraphAr">
    </h1>
    <p align="center">
        An open source, standard data file format for graph data storage and retrieval
    </p>

|GraphAr CI| |Docs CI| |GraphAr Docs| |Good First Issue|

📢 Join our `Weekly Community Meeting`_ to learn more about GraphAr and get involved!

What is GraphAr?
-----------------

|

.. image:: docs/images/overview.png
  :width: 770
  :align: center
  :alt: Overview

|

Graph processing serves as the essential building block for a diverse variety of
real-world applications such as social network analytics, data mining, network routing,
and scientific computing.

GraphAr (short for "Graph Archive") is a project that aims to make it easier for diverse applications and
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

A property graph consists of vertices and edges, with each vertex contains a unique identifier and:

- A text label that describes the vertex type.
- A collection of properties, with each property can be represented by a key-value pair.

Each edge contains a unique identifier and:

- The outgoing vertex (source).
- The incoming vertex (destination).
- A text label that describes the relationship between the two vertices.
- A collection of properties.

The following is an example property graph containing two types of vertices ("person" and "comment") and three types of edges.

.. image:: docs/images/property_graph.png
  :width: 700
  :align: center
  :alt: property graph

Vertices in GraphAr
^^^^^^^^^^^^^^^^^^^

Logical table of vertices
""""""""""""""""""""""""""

Each type of vertices (with the same label) constructs a logical vertex table, with each vertex assigned with a global index inside this type (called internal vertex id) starting from 0, corresponding to the row number of the vertex in the logical vertex table. An example layout for a logical table of vertices under the label "person" is provided for reference.

Given an internal vertex id and the vertex label, a vertex is uniquely identifiable and its respective properties can be accessed from this table. The internal vertex id is further used to identify the source and destination vertices when maintaining the topology of the graph.

.. image:: docs/images/vertex_logical_table.png
  :width: 650
  :align: center
  :alt: vertex logical table

Physical table of vertices
""""""""""""""""""""""""""

The logical vertex table will be partitioned into multiple continuous vertex chunks for enhancing the reading/writing efficiency. To maintain the ability of random access, the size of vertex chunks for the same label is fixed. To support to access required properties avoiding reading all properties from the files, and to add properties for vertices without modifying the existing files, the columns of the logical table will be divided into several column groups.

Take the "person" vertex table as an example, if the chunk size is set to be 500, the logical table will be separated into sub-logical-tables of 500 rows with the exception of the last one, which may have less than 500 rows. The columns for maintaining properties will also be divided into distinct groups (e.g., 2 for our example). As a result, a total of 4 physical vertex tables are created for storing the example logical table, which can be seen from the following figure.

.. image:: docs/images/vertex_physical_table.png
  :width: 650
  :align: center
  :alt: vertex physical table

**Note**: For efficiently utilize the filter push-down of the payload file format like Parquet, the internal vertex id is stored in the payload file as a column. And since the internal vertex id is continuous, the payload file format can use the delta encoding for the internal vertex id column, which would not bring too much overhead for the storage.

Edges in GraphAr
^^^^^^^^^^^^^^^^

Logical table of edges
""""""""""""""""""""""""""

For maintaining a type of edges (that with the same triplet of the source label, edge label, and destination label), a logical edge table is established.  And in order to support quickly creating a graph from the graph storage file, the logical edge table could maintain the topology information in a way similar to CSR/CSC (learn more about `CSR/CSC <https://en.wikipedia.org/wiki/Sparse_matrix>`_), that is, the edges are ordered by the internal vertex id of either source or destination. In this way, an offset table is required to store the start offset for each vertex's edges, and the edges with the same source/destination will be stored continuously in the logical table.

Take the logical table for "person knows person" edges as an example, the logical edge table looks like:

.. image:: docs/images/edge_logical_table.png
  :width: 650
  :align: center
  :alt: edge logical table

Physical table of edges
""""""""""""""""""""""""""

As same with the vertex table, the logical edge table is also partitioned into some sub-logical-tables, with each sub-logical-table contains edges that the source (or destination) vertices are in the same vertex chunk. According to the partition strategy and the order of the edges, edges can be stored in GraphAr following one of the four types:

- **ordered_by_source**: all the edges in the logical table are ordered and further partitioned by the internal vertex id of the source, which can be seen as the CSR format.
- **ordered_by_dest**: all the edges in the logical table are ordered and further partitioned by the internal vertex id of the destination, which can be seen as the CSC format.
- **unordered_by_source**: the internal id of the source vertex is used as the partition key to divide the edges into different sub-logical-tables, and the edges in each sub-logical-table are unordered, which can be seen as the COO format.
- **unordered_by_dest**: the internal id of the destination vertex is used as the partition key to divide the edges into different sub-logical-tables, and the edges in each sub-logical-table are unordered, which can also be seen as the COO format.

After that, a sub-logical-table is further divided into edge chunks of a predefined, fixed number of rows (referred to as edge chunk size). Finally, an edge chunk is separated into physical tables in the following way:

- an adjList table (which contains only two columns: the internal vertex id of the source and the destination).
- 0 or more edge property tables, with each table contains a group of properties.

Additionally, there would be an offset table for **ordered_by_source** or **ordered_by_dest** edges. The offset table is used to record the starting point of the edges for each vertex. The partition of the offset table should be in alignment with the partition of the corresponding vertex table. The first row of each offset chunk is always 0, indicating the starting point for the corresponding sub-logical-table for edges.

Take the "person knows person" edges to illustrate. Suppose the vertex chunk size is set to 500 and the edge chunk size is 1024, and the edges are **ordered_by_source**, then the edges could be saved in the following physical tables:

.. image:: docs/images/edge_physical_table1.png
  :width: 650
  :align: center
  :alt: edge logical table1

.. image:: docs/images/edge_physical_table2.png
  :width: 650
  :align: center
  :alt: edge logical table2

Building Libraries
------------------

GraphAr offers a collection of libraries for the purpose of reading, writing and transforming files.
Currently, the following libraries are available, and plans are in place to expand support to additional programming language.

The C++ Library
^^^^^^^^^^^^^^^
See `GraphAr C++ Library`_ for details about the building of the C++ library.

The Java Library
^^^^^^^^^^^^^^^^
The GraphAr Java library is created with bindings to the C++ library (currently at version v0.10.0), utilizing  `Alibaba-FastFFI`_ for implementation.
See `GraphAr Java Library`_ for details about the building of the Java library.

The Spark Library
^^^^^^^^^^^^^^^^^
See `GraphAr Spark Library`_ for details about the Spark library.

The PySpark Library
^^^^^^^^^^^^^^^^^^^
The GraphAr PySpark library is developed as bindings to the GraphAr Spark library.
See `GraphAr PySpark Library`_ for details about the PySpark library.


Contributing
-------------

Contributing Guidelines
^^^^^^^^^^^^^^^^^^^^^^^^

Read through our `contribution guidelines`_ to learn about our submission process, coding rules, and more.

Code of Conduct
^^^^^^^^^^^^^^^^

Help us keep GraphAr open and inclusive. Please read and follow our `Code of Conduct`_.

Getting Involved
----------------

Join the conversation and help the community. Even if you do not plan to contribute
to GraphAr itself or GraphAr integrations in other projects, we'd be happy to have you involved.

- Ask questions on `GitHub Discussions`_. We welcome all kinds of questions, from beginner to advanced!
- Follow our activity and ask for feature requests on `GitHub Issues`_.
- Join our `Weekly Community Meeting`_.

Read through our `community introduction`_ to learn about our communication channels, governance, and more.


License
-------

**GraphAr** is distributed under `Apache License 2.0`_. Please note that
third-party libraries may not have the same license as GraphAr.

Publication
-----------

- Xue Li, Weibin Zeng, Zhibin Wang, Diwen Zhu, Jingbo Xu, Wenyuan Yu, Jingren Zhou.
  `Enhancing Data Lakes with GraphAr: Efficient Graph Data Management with a Specialized Storage Scheme[J] <https://arxiv.org/abs/2312.09577>`_.
  arXiv preprint arXiv:2312.09577, 2023.

.. code:: bibtex

  @article{li2023enhancing,
    author = {Xue Li and Weibin Zeng and Zhibin Wang and Diwen Zhu and Jingbo Xu and Wenyuan Yu and Jingren Zhou},
    title = {Enhancing Data Lakes with GraphAr: Efficient Graph Data Management with a Specialized Storage Scheme},
    year = {2023},
    url = {https://doi.org/10.48550/arXiv.2312.09577},
    doi = {10.48550/ARXIV.2312.09577},
    eprinttype = {arXiv},
    eprint = {2312.09577},
    biburl = {https://dblp.org/rec/journals/corr/abs-2312-09577.bib},
    bibsource = {dblp computer science bibliography, https://dblp.org}
  }


.. _Apache License 2.0: https://github.com/apache/incubator-graphar/blob/main/LICENSE

.. |GraphAr CI| image:: https://github.com/apache/incubator-graphar/actions/workflows/ci.yml/badge.svg
   :target: https://github.com/apache/incubator-graphar/actions

.. |Docs CI| image:: https://github.com/apache/incubator-graphar/actions/workflows/docs.yml/badge.svg
   :target: https://github.com/apache/incubator-graphar/actions

.. |GraphAr Docs| image:: https://img.shields.io/badge/docs-latest-brightgreen.svg
   :target: https://alibaba.github.io/GraphAr/

.. |Good First Issue| image:: https://img.shields.io/github/labels/apache/incubator-graphar/Good%20First%20Issue?color=green&label=Contribute%20&style=plastic
   :target: https://github.com/apache/incubator-graphar/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22

.. _GraphAr File Format: https://alibaba.github.io/GraphAr/user-guide/file-format.html

.. _GraphAr Spark Library: https://github.com/apache/incubator-graphar/tree/main/spark

.. _GraphAr PySpark Library: https://github.com/apache/incubator-graphar/tree/main/pyspark

.. _GraphAr C++ Library: https://github.com/apache/incubator-graphar/tree/main/cpp

.. _GraphAr Java Library: https://github.com/apache/incubator-graphar/tree/main/java

.. _example files: https://github.com/GraphScope/gar-test/blob/main/ldbc_sample/

.. _contribution guidelines: https://github.com/apache/incubator-graphar/tree/main/CONTRIBUTING.rst

.. _Code of Conduct: https://github.com/apache/incubator-graphar/blob/main/CODE_OF_CONDUCT.md

.. _GraphAr Slack: https://join.slack.com/t/grapharworkspace/shared_invite/zt-1wh5vo828-yxs0MlXYBPBBNvjOGhL4kQ

.. _Weekly Community Meeting: https://github.com/apache/incubator-graphar/wiki/GraphAr-Weekly-Community-Meeting

.. _community introduction: https://github.com/apache/incubator-graphar/tree/main/docs/developers/community.rst

.. _GitHub Issues: https://github.com/apache/incubator-graphar/issues/new

.. _Github Discussions: https://github.com/apache/incubator-graphar/discussions

.. _Alibaba-FastFFI: https://github.com/alibaba/fastFFI
