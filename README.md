<h1 align="center" style="clear: both;">
    <img src="docs/images/graphar-logo.svg" width="350" alt="GraphAr">
</h1>
<p align="center">
    An open source, standard data file format for graph data storage and retrieval
</p>

[![GraphAr
CI](https://github.com/apache/graphar/actions/workflows/ci.yml/badge.svg)](https://github.com/apache/graphar/actions)
[![Docs
CI](https://github.com/apache/graphar/actions/workflows/docs.yml/badge.svg)](https://github.com/apache/graphar/actions)
[![codecov](https://codecov.io/gh/apache/graphar/graph/badge.svg)](https://codecov.io/gh/apache/graphar)
[![GraphAr
Docs](https://img.shields.io/badge/docs-latest-brightgreen.svg)](https://graphar.apache.org/docs/)
[![Good First
Issue](https://img.shields.io/github/labels/apache/graphar/Good%20First%20Issue?color=green&label=Contribute)](https://github.com/apache/graphar/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22)
[![README-zh](https://shields.io/badge/README-%E4%B8%AD%E6%96%87-blue)](README-zh-cn.md)

## What is GraphAr?

<img src="docs/images/overview.png" class="align-center" width="770"
alt="Overview" />

Graph processing serves as the essential building block for a diverse
variety of real-world applications such as social network analytics,
data mining, network routing, and scientific computing.

GraphAr (short for "Graph Archive") is a project that aims to make it
easier for diverse applications and systems (in-memory and out-of-core
storages, databases, graph computing systems, and interactive graph
query frameworks) to build and access graph data conveniently and
efficiently.

It can be used for importing/exporting and persistent storage of graph
data, thereby reducing the burden on systems when working together.
Additionally, it can serve as a direct data source for graph processing
applications.

To achieve this, GraphAr project provides:

- The GraphAr format: a standardized system-independent
  format for storing graph data
- Libraries: a set of libraries for reading, writing and transforming
  GraphAr format data

By using GraphAr, you can:

- Store and persist your graph data in a system-independent way with the
  GraphAr format
- Easily access and generate GraphAr format data using the libraries
- Utilize Apache Spark to quickly manipulate and transform your graphar 
  format data

## The GraphAr Format

The GraphAr format is designed for storing property graphs. It uses
metadata to record all the necessary information of a graph, and
maintains the actual data in a chunked way.

A property graph consists of vertices and edges, with each vertex
contains a unique identifier and:

- A text label that describes the vertex type.
- A collection of properties, with each property can be represented by a
  key-value pair.

Each edge contains a unique identifier and:

- The outgoing vertex (source).
- The incoming vertex (destination).
- A text label that describes the relationship between the two vertices.
- A collection of properties.

The following is an example property graph containing two types of
vertices ("person" and "comment") and three types of edges.

<img src="docs/images/property_graph.png" class="align-center"
width="700" alt="property graph" />

### Vertices in GraphAr

#### Logical table of vertices

Each type of vertices (with the same label) constructs a logical vertex
table, with each vertex assigned with a global index inside this type
(called internal vertex id) starting from 0, corresponding to the row
number of the vertex in the logical vertex table. An example layout for
a logical table of vertices under the label "person" is provided for
reference.

Given an internal vertex id and the vertex label, a vertex is uniquely
identifiable and its respective properties can be accessed from this
table. The internal vertex id is further used to identify the source and
destination vertices when maintaining the topology of the graph.

<img src="docs/images/vertex_logical_table.png" class="align-center"
width="650" alt="vertex logical table" />

###  Physical table of vertices

The logical vertex table will be partitioned into multiple continuous
vertex chunks for enhancing the reading/writing efficiency. To maintain
the ability of random access, the size of vertex chunks for the same
label is fixed. To support to access required properties avoiding
reading all properties from the files, and to add properties for
vertices without modifying the existing files, the columns of the
logical table will be divided into several column groups.

Take the `person` vertex table as an example, if the chunk size is set
to be 500, the logical table will be separated into sub-logical-tables
of 500 rows with the exception of the last one, which may have less than
500 rows. The columns for maintaining properties will also be divided
into distinct groups (e.g., 2 for our example). As a result, a total of
4 physical vertex tables are created for storing the example logical
table, which can be seen from the following figure.

<img src="docs/images/vertex_physical_table.png" class="align-center"
width="650" alt="vertex physical table" />

> [!NOTE]
> For efficiently utilize the filter push-down of the payload
file format like Parquet, the internal vertex id is stored in the
payload file as a column. And since the internal vertex id is
continuous, the payload file format can use the delta encoding for the
internal vertex id column, which would not bring too much overhead for
the storage.

### Edges in GraphAr

#### Logical table of edges

For maintaining a type of edges (that with the same triplet of the
source label, edge label, and destination label), a logical edge table
is established. And in order to support quickly creating a graph from
the graph storage file, the logical edge table could maintain the
topology information in a way similar to [CSR/CSC](https://en.wikipedia.org/wiki/Sparse_matrix), that is, the
edges are ordered by the internal vertex id of either source or
destination. In this way, an offset table is required to store the start
offset for each vertex's edges, and the edges with the same
source/destination will be stored continuously in the logical table.

Take the logical table for `person knows person` edges as an example,
the logical edge table looks like:

<img src="docs/images/edge_logical_table.png" class="align-center"
width="650" alt="edge logical table" />

#### Physical table of edges

As same with the vertex table, the logical edge table is also
partitioned into some sub-logical-tables, with each sub-logical-table
contains edges that the source (or destination) vertices are in the same
vertex chunk. According to the partition strategy and the order of the
edges, edges can be stored in GraphAr following one of the four types:

- **ordered_by_source**: all the edges in the logical table are ordered
  and further partitioned by the internal vertex id of the source, which
  can be seen as the CSR format.
- **ordered_by_dest**: all the edges in the logical table are ordered
  and further partitioned by the internal vertex id of the destination,
  which can be seen as the CSC format.
- **unordered_by_source**: the internal id of the source vertex is used
  as the partition key to divide the edges into different
  sub-logical-tables, and the edges in each sub-logical-table are
  unordered, which can be seen as the COO format.
- **unordered_by_dest**: the internal id of the destination vertex is
  used as the partition key to divide the edges into different
  sub-logical-tables, and the edges in each sub-logical-table are
  unordered, which can also be seen as the COO format.

After that, a sub-logical-table is further divided into edge chunks of a
predefined, fixed number of rows (referred to as edge chunk size).
Finally, an edge chunk is separated into physical tables in the
following way:

- an adjList table (which contains only two columns: the internal vertex
  id of the source and the destination).
- 0 or more edge property tables, with each table contains a group of
  properties.

Additionally, there would be an offset table for **ordered_by_source**
or **ordered_by_dest** edges. The offset table is used to record the
starting point of the edges for each vertex. The partition of the offset
table should be in alignment with the partition of the corresponding
vertex table. The first row of each offset chunk is always 0, indicating
the starting point for the corresponding sub-logical-table for edges.

Take the `person knows person` edges to illustrate. Suppose the vertex
chunk size is set to 500 and the edge chunk size is 1024, and the edges
are **ordered_by_source**, then the edges could be saved in the
following physical tables:

<img src="docs/images/edge_physical_table1.png" class="align-center"
width="650" alt="edge logical table1" />

<img src="docs/images/edge_physical_table2.png" class="align-center"
width="650" alt="edge logical table2" />

## Libraries

GraphAr offers a collection of libraries for the purpose of reading,
writing and transforming files. Currently, the following libraries are
available, and plans are in place to expand support to additional
programming language.

### The C++ Library

See [GraphAr C++
Library](./cpp) for
details about the building of the C++ library.


### The Scala with Spark Library

See [GraphAr Spark
Library](./maven-projects/spark)
for details about the Scala with Spark library.

### The Java Library

> [!NOTE] 
> The Java library is under development. 

The GraphAr Java library is created with bindings to the C++ library
(currently at version v0.10.0), utilizing
[Alibaba-FastFFI](https://github.com/alibaba/fastFFI) for
implementation. See [GraphAr Java
Library](./maven-projects/java) for
details about the building of the Java library.

### The Python with PySpark Library

> [!NOTE] 
> The Python with PySpark library is under development.

The PySpark library is developed as bindings to the GraphAr
Spark library. See [GraphAr PySpark
Library](./pyspark)
for details about the PySpark library.

## Contributing

- Start with [Contributing Guide](https://github.com/apache/graphar/blob/main/CONTRIBUTING.md).
- Submit [Issues](https://github.com/apache/graphar/issues) for bug reports, feature requests.
- Discuss at [dev mailing list](mailto:dev@graphar.apache.org) ([subscribe](mailto:dev-subscribe@graphar.apache.org?subject=(send%20this%20email%20to%20subscribe)) / [unsubscribe](mailto:dev-unsubscribe@graphar.apache.org?subject=(send%20this%20email%20to%20unsubscribe)) / [archives](https://lists.apache.org/list.html?dev@graphar.apache.org)).
- Asking questions on [GitHub Discussions](https://github.com/apache/graphar/discussions/new?category=q-a).
- Join our [Biweekly Community Meeting](https://github.com/apache/graphar/wiki/GraphAr-Community-Meeting).

## License

**GraphAr** is distributed under [Apache License
2.0](https://github.com/apache/graphar/blob/main/LICENSE).
Please note that third-party libraries may not have the same license as
GraphAr.

## Publication

- Xue Li, Weibin Zeng, Zhibin Wang, Diwen Zhu, Jingbo Xu, Wenyuan Yu,
  Jingren Zhou. [Enhancing Data Lakes with GraphAr: Efficient Graph Data
  Management with a Specialized Storage
  Scheme\[J\]](https://arxiv.org/abs/2312.09577). arXiv preprint
  arXiv:2312.09577, 2023.

```bibtex
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
```
