Integrate into GraphScope
============================

`GraphScope <https://graphscope.io/>`_ is a unified distributed graph computing platform that provides a one-stop environment for performing diverse graph operations on a cluster through a user-friendly Python interface. As an important application case of GraphAr, we integrate it into GraphScope.

GraphScope works on a graph G fragmented via a partition strategy picked by the user and each worker maintains a fragment of G. Given a query, it posts the same query to all the workers and computes following the BSP (Bulk Synchronous Parallel) model. More specifically, each worker first executes processing against its local fragment, to compute partial answers in parallel. And then each worker may exchange partial results with other processors via synchronous message passing.

To integrate GraphAr into GraphScope, we implement *ArrowFragmentBuilder* and *ArrowFragmentWriter*. *ArrowFragmentBuilder* establishes the fragments for workers of GraphScope through reading GAR files in parallel. And *ArrowFragmentWriter* could dump the GraphScope fragments into GAR files. Please refer to the `source code <https://github.com/acezen/v6d/tree/acezen/gsf-fragment-ev-builder/modules/graph/loader>`_ if you want to learn more about the implementations.


Performance Report
------------------------

Parameter settings
``````````````````
The time performance of *ArrowFragmentBuilder* and *ArrowFragmentWriter* in GraphScope highly depends on the how to partition the graph into GAR files, that is, the *vertex chunk size* and *edge chunk size*, which are specified in the vertex information file and in the edge information file, respectively. See `GraphAr File Format <../user-guide/file-format.html>`_ to learn about the definitions of chunk size in GAR.

In general, if the file size is large, there will be fewer chunks generated. Especially on small graphs, few files will result in low degree of parallelism, leading to longer disk I/O time. On the contrary, if the file size is too small, there will be a large number of small files, thus the overhead incurred by file system and the file parser will increase.

We conduct micro benchmarks to compare the time performance for reading/writing GAR files by *ArrowFragmentBuilder*/*ArrowFragmentWriter*, under different settings of *vertex chunk size* and *edge chunk size*. The settings we recommend for *vertex chunk size* and *edge chunk size* are **2^18** and **2^22**, respectively, which lead to efficient performance in most cases. These settings can also be used as the reference values when integrating GraphAr into other systems besides GraphScope.

Time performance results
````````````````````````
Here we report the performance results of *ArrowFragmentBuilder*, and compared it with loading the same graph through other ways, including the default loading strategy of GraphScope (through reading the csv files in parallel) and loading based by deserialization. The execution time includes loading the graph data from the disk into memory and building GraphScope fragments from such data. The experimental results in the following table are tested on a cluster of 4 AliCloud ecs.r6.6xlarge instances (24vCPU, 192GB memory), and using `com-friendster <https://snap.stanford.edu/data/com-Friendster.html>`_ (a simple graph) and `ldbc-snb-30 <https://ldbcouncil.org/benchmarks/snb/>`_ (a multi-labeled property graph) as the datasets.

+----------------+---------+-----------------+-----------------+-----------------+
| Dataset        | Workers | Default Loading | Deserialization | GraphAr Loading |
+================+=========+=================+=================+=================+
| com-friendster | 4       | 11min31s        | 1min41s         | 2min21s         |
+----------------+---------+-----------------+-----------------+-----------------+
| ldbc-snb-30    | 4       | 6min28s         | 54s             | 1min19s         |
+----------------+---------+-----------------+-----------------+-----------------+
