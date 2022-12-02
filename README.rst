GraphAr
========

|GraphAr CI| |Docs CI| |GraphAr Docs|

GraphAr (short for "Graph Archive") is an open source, standard data file format with C++ SDK and Spark tools for graph data storage and retrieval.

The GraphAr project includes such modules as:

- The design of the standardized file format (GAR) for graph data.
- A C++ Library for reading and writing GAR files.
- Apache Spark tools for generating, loading and transforming GAR files (coming soon).
- Examples of applying GraphAr to graph processing applications or existing systems such as GraphScope.




|Overview Pic|


Motivation
----------

Graph processing serves as the essential building block for a diverse variety of real-world applications such as social network analytics, data mining, network routing, and scientific computing.

GraphAr (GAR) is established to enable diverse graph applications and systems (in-memory and out-of-core storages, databases, graph computing systems and interactive graph query frameworks) to build and access the graph data conveniently and efficiently. It specifies a standardized system-independent file format for graph and provides a set of interfaces to generate and access such formatted files.

GraphAr (GAR) targets two main scenarios:

- To serve as the standard file format for importing/exporting and persistent storage of the graph data for diverse existing systems, reducing the overhead when various systems co-work.
- To serve as the direct data source for graph processing applications.


What's in GraphAr
---------------------

The **GAR** file format that defines a standard store file format for graph data.

The **GAR SDK** library that contains a C++ library to provide APIs for accessing and generating the GAR format files.


GraphAr File Format
---------------------

GraphAr specifies a standardized system-independent file format (GAR) for storing property graphs.
It uses metadata to record all the necessary information of a graph, and maintains the actual data
in a chunked way.

What is Property Graph
^^^^^^^^^^^^^^^^^^^^^^^

GraphAr is designed for representing and storing the property graphs. Graph (in discrete mathematics) is a structure made of vertices and edges. Property graph is then a type of graph model where the vertices/edges could carry a name (also called as type or label) and some properties. Since carrying additional information than non-property graphs, the property graph is able to represent connections among data scattered across diverse data databases and with different schemas. Compared with the relational database schema, the property graph excels at showing data dependencies. Therefore, it is widely-used in modeling modern applications including social network analytics, data mining, network routing, scientific computing and so on.

A property graph includes vertices and edges. Each vertex contains:

- A unique identifier (called vertex id or vertex index).
- A text label that describes the vertex type.
- A collection of properties, with each property can be represented by a key-value pair.

And each edge contains:

- A unique identifier (called edge id or edge index).
- The outgoing vertex (source).
- The incoming vertex (destination).
- A text label that describes the relationship between the two vertices.
- A collection of properties.

The following is an example property graph containing two types of vertices "person" and "comment" and three types of edges.

|Property Graph|

Vertices in GraphAr
^^^^^^^^^^^^^^^^^^^^^^^

Logical table of vertices
""""""""""""""""""""""""""

Each type of vertices (with the same label) constructs a logical vertex table, with each vertex assigned with a global index (vertex id) starting from 0, that is, the row number of that vertex in the logical vertex table. The following example shows the layout of the logical table for vertices that with label "person".

Given a vertex id as well as the vertex label, a vertex can be identified uniquely, and the properties of it can be accessed from this table. When maintaining the topology of a graph, the vertex id is used to identify the source and destination for each of the edges.

|Vertex Logical Table|

Physical table of vertices
""""""""""""""""""""""""""

For enhancing the reading/writing efficiency, the logical vertex table will be partitioned into multiple continuous vertex chunks. And to maintain the ability of random access, the size of vertex chunks for the same label is fixed. To support to access required properties avoiding reading all properties from the files, and to add properties for vertices without modifying the existing files, the columns of the logical table will be divided into several column groups.

Take the "person" vertex table as an example, if the chunk size is set to be 500, the logical table will be separated into sub-logical-tables of 500 rows except the last one, which can be less than 500 rows.  And the columns for maintaining properties are also separated, being divided into several groups (e.g., 2 groups for our example). Therefore, there are 4 physical vertex tables in total for actually storing the example logical table, as the following figure shows.

|Vertex Physical Table|


Edges in GraphAr
^^^^^^^^^^^^^^^^

Logical table of edges
""""""""""""""""""""""""""

For maintaining a type of edges (that with the same triplet of the source label, edge label, and destination label), a logical edge table is established.  And in order to support quickly creating a graph from the graph storage file, the logical edge table could maintain the topology information in a way similar to CSR/CSC (learn more about `CSR/CSC <https://en.wikipedia.org/wiki/Sparse_matrix>`_), that is, the edges are ordered by the vertex id of source/destination. In this way, one offset table is required to store the start offset for each vertex's edges. And the edges with the same source/destination will be stored continuously in the logical table.

Take the logical table for "person likes person" edges as an example, the logical edge table looks like:

|Edge Logical Table|


Physical table of edges
""""""""""""""""""""""""""

According to the partition strategy and the order of the edges, edges can be one of the four types: **ordered_by_source**, **ordered_by_dest**, **unordered_by_source** or **unordered_by_dest**. A logical edge table could contain physical tables of three categories:

- The adjList table (which contains only two columns: the vertex id of the source and the destination).
- The edge property tables (if there are properties on edges).
- The offset table (optional, only required for ordered edges).

Since the vertex table are partitioned into multiple chunks, the logical edge table is also partitioned into some sub-logical-tables, with each sub-logical-table contains edges that the source (if the type is **ordered_by_source** or **unordered_by_source**) or destination (if the type is **ordered_by_dest** or **unordered_by_dest**) vertices are in the same vertex chunk. After that, a sub-logical-table is further divided into edge chunks in which the number of rows is fixed (called edge chunk size). Finally, an edge chunk is separated into an adjList table and 0 or more property tables.

Also, the partition of the offset table is aligned with the partition of the corresponding vertex table. The first row of each offset chunk is always 0, means that to start with the first row of the corresponding sub-logical-table for edges.

Take the "person knows person" edges to illustrate, when the vertex chunk size is set to be 500 and the edge chunk size is 1024, the edges will be saved in the following physical tables:

|Edge Physical Table1|
|Edge Physical Table2|


Building SDK Steps
---------------------

Dependencies
^^^^^^^^^^^^^

**GraphAr** is developed and tested on ubuntu 20.04. It should also work on other unix-like distributions. Building GraphAr requires the following softwares installed as dependencies.

- A modern C++ compiler compliant with C++17 standard (g++ >= 7.1 or clang++ >= 5).
- `CMake <https://cmake.org/>`_ (>=2.8)

Here are the dependencies for optional features:

- `Doxygen <https://www.doxygen.nl/index.html>`_ (>= 1.8) for generating documentation;
- `sphinx <https://www.sphinx-doc.org/en/master/index.html>`_ for generating documentation.

Extra dependencies are required by examples and unit tests:

- `BGL <https://www.boost.org/doc/libs/1_80_0/libs/graph/doc/index.html>`_ (>= 1.58).


Building and install GraphAr C++ library
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once the required dependencies have been installed, go to the root directory of GraphAr and do an out-of-source build using CMake.

.. code-block:: shell

    git submodule update --init
    mkdir build && cd build
    cmake ..
    make -j$(nproc)

**Optional**: Using a Custom Namespace

The `namespace` that `gar` is defined in is configurable. By default,
it is defined in `namespace GraphArchive`; however this can be toggled by
setting `NAMESPACE` option with cmake:

.. code:: shell

    mkdir build
    cd build
    cmake .. -DNAMESPACE=MyNamespace
    make -j$(nproc)

Run the test with command:

.. code-block:: shell

    make test

Install the GraphAr library:

.. code-block:: shell

    sudo make install

Build the documentation of GraphAr library:

.. code-block:: shell

    # assume doxygen and sphinx has been installed.
    pip3 install -r ../requirements-dev.txt --user
    make doc

Using GraphAr C++ library in your own project
-----------------------------------------------

The way we recommend to integrate the GraphAr C++ library in your own C++ project is to use
CMake's `find_package` function for locating and integrating dependencies.

Here is a minimal `CMakeLists.txt` that compiles a source file `my_example.cc` into an executable
target linked with GraphAr C++ shared library.

.. code-block:: cmake

    project(MyExample)

    find_package(gar REQUIRED)
    include_directories(${GAR_INCLUDE_DIRS})

    add_executable(my_example my_example.cc)
    target_compile_features(my_example PRIVATE cxx_std_17)
    target_link_libraries(my_example PRIVATE ${GAR_LIBRARIES})


Contributing to GraphAr
----------------------

- Read the `Contribution Guide`_.
- Please report bugs by submitting `GitHub Issues`_ or ask me anything in `Github Discussions`_.
- Submit contributions using pull requests.

Thank you in advance for your contributions to GraphAr!


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

.. |Overview Pic| image:: https://github.com/alibaba/GraphAr/blob/main/docs/images/overview.png?raw=true
  :width: 650
  :alt: Overview

.. |Property Graph| image:: https://github.com/alibaba/GraphAr/blob/main/docs/images/property_graph.png?raw=true
  :width: 650
  :alt: property graph

.. |Vertex Logical Table| image:: https://github.com/alibaba/GraphAr/blob/main/docs/images/vertex_logical_table.png?raw=true
  :width: 650
  :alt: vertex logical table

.. |Vertex Physical Table| image:: https://github.com/alibaba/GraphAr/blob/main/docs/images/vertex_physical_table.png?raw=true
  :width: 650
  :alt: vertex physical table

.. |Edge Logical Table| image:: https://github.com/alibaba/GraphAr/blob/main/docs/images/edge_logical_table.png?raw=true
  :width: 650
  :alt: edge logical table

.. |Edge Physical Table1| image:: https://github.com/alibaba/GraphAr/blob/main/docs/images/edge_physical_table1.png?raw=true
  :width: 650
  :alt: edge logical table1

.. |Edge Physical Table2| image:: https://github.com/alibaba/GraphAr/blob/main/docs/images/edge_physical_table2.png?raw=true
  :width: 650
  :alt: edge logical table2

.. _GraphAr File Format: https://github.com/alibaba/GraphAr/blob/main/docs/user-guide/file-format.rst

.. _example files: https://github.com/GraphScope/gar-test/blob/main/ldbc_sample/

.. _Contribution Guide: https://github.com/alibaba/GraphAr/blob/main/CONTRIBUTING.rst

.. _GitHub Issues: https://github.com/alibaba/GraphAr/issues/new

.. _Github Discussions: https://github.com/alibaba/GraphAr/discussions
