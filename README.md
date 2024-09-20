<h1 align="center" style="clear: both;">
    <img src="docs/images/graphar-logo.svg" width="350" alt="GraphAr">
</h1>
<p align="center">
    An open source, standard data file format for graph data storage and retrieval
</p>

This project is a research initiative by GraphAr (short for "Graph Archive") aimed at providing an efficient storage scheme for graph data in data lakes. It is designed to enhance the efficiency of data lakes utilizing the capabilities of existing formats, with a specific focus on [Apache Parquet](https://github.com/apache/parquet-format). GraphAr ensures seamless integration with existing tools and introduces innovative additions specifically tailored to handle LPGs (Labeled Property Graphs). 

Leveraging the strengths of Parquet, GraphAr captures LPG semantics precisely and facilitates graph-specific operations such as neighbor retrieval and label filtering.
See [GraphAr Format](https://github.com/apache/incubator-graphar/blob/research/GRAPHAR.md) for more details about the GraphAr format. And refer to the [Research Paper](https://arxiv.org/abs/2312.09577) for the detailed design and implementation of our encoding/decoding techniques.


## Dependencies

**GraphAr** is developed and tested on Ubuntu 20.04.5 LTS. It should also work on other unix-like distributions. Building GraphAr requires the following software installed as dependencies:

- A C++17-enabled compiler. On Linux, GCC 7.1 or higher should be sufficient. For macOS, at least Clang 5 is required
- CMake 3.16 or higher
- On Linux and macOS, ``make`` build utilities
- curl-devel with SSL (Linux) or curl (macOS) for s3 filesystem support


## Building Steps

### Step 1: Clone the Repository

```bash
    $ git clone https://github.com/apache/incubator-graphar.git
    $ cd incubator-graphar
    $ git checkout research
    $ git submodule update --init
```

### Step 2: Build the Project
```bash
    $ mkdir build
    $ cd build
    $ chmod +x ../script/build.sh
    $ ../script/build.sh
```

## Preparing Graph Data

Before running the benchmarking components, you need to prepare the graph datasets. You can download from public graph datasets or generate synthetic graph datasets using our data generator.

### Preparing Topology Graphs

#### Transforming Public Graphs

Suppose we want to use the Facebook dataset. First, download the dataset from the [SNAP](https://snap.stanford.edu/data/egonets-Facebook.html) website and extract it.
As an example, we have already included this Facebook dataset in the `dataset` directory.

Then, convert the dataset into Parquet format:

```bash
    $ cd incubator-graphar/build
    $ ./release/Csv2Parquet {input_path} {output_path} {header_line_num}
```
Or, you could use the following command to convert the dataset into the GraphAr format:

```bash
    $ ./release/data-generator {input_path} {output_path} {vertex_num} {is_directed} {is_weighted} {is_sorted} {is_reversed} {delimiter} {header_line_num}
```

For example, running the command for the Facebook dataset:

```bash
    $ ./release/Csv2Parquet {path_to_graphar}/dataset/facebook/facebook.txt {path_to_graphar}/dataset/facebook/facebook 0
    $ ./release/data-generator {path_to_graphar}/dataset/facebook/facebook.txt {path_to_graphar}/dataset/facebook/facebook 4039 false false true false space 0
```

The above commands will convert the facebook dataset into the Parquet and GraphAr format and store the output in the `dataset/facebook` directory.

#### Generating Synthetic Graphs

We also provide a data generator to generate synthetic graph datasets. The data generator is located in the `synthetic` directory. You can use the following command to generate a synthetic graph dataset:

```bash
    $ cd synthetic
    $ mkdir build
    $ cd build
    $ cmake ..
    $ make
    $ ./DataGenerator {vertex_num} {output_path} # e.g., ./DataGenerator 100 example-synthetic-graph
```

It will generate a synthetic graph with the specified number of vertices in CSV format. Afterward, you can convert this CSV file into Parquet or GraphAr format using the `Csv2Parquet` or `data-generator` tool, as described above.

### Preparing Labeled Graphs

### Preparing Label Data

To enable the label filtering benchmarking component, original label data must be extracted from graphs obtained from various sources. We use a CSV file to store the original label data, where each row represents a vertex and each column represents a label, formatted as a binary matrix:

| Label 1 | Label 2 | ... | Label N |
|---------|---------|-----|---------|
| 1       | 0       | ... | 0       |
| 1       | 1       | ... | 0       |
| ...     | ...     | ... | ...     |

For example, the `dataset/bloom` directory contains the label data for the [Bloom](https://github.com/neo4j-graph-examples/bloom/tree/main) dataset. This dataset includes 32,960 vertices and 18 labels.


### Graphs from the LDBC Benchmark

Graphs from the LDBC benchmark are generated using the [LDBC SNB Data Generator](https://ldbcouncil.org/post/snb-data-generator-getting-started/) tool in CSV format. Each dataset consists of multiple CSV files, where each file represents a specific edge or vertex type, e.g., `person_knows_person_0_0.csv` and `person_0_0.csv`.
Once the original dataset is generated, you can convert it into Parquet/GraphAr format as described above.

The following command will generate the Parquet and GraphAr files for `person_knows_person` data of the SF30 dataset:

```bash
    $ ../script/generate_ldbc.sh {path_to_dataset}/sf30/social_network/dynamic/person_knows_person_0_0.csv  {path_to_dataset}/sf30/social_network/dynamic/person_0_0.csv {path_to_dataset}/sf30/person_knows_person
```

Please refer to `script/generate_ldbc_all.sh` for more details on this preparation process.

## Running Benchmarking Components

### Neighbor Retrieval

To run the neighbor retrieval benchmarking component, you can use the following command:

```bash
    $ ../script/run_neighbor_retrieval.sh {graph_path} {vertex_num} {source_vertex}
```

For example: 

```bash
    $ ../script/run_neighbor_retrieval.sh {path_to_graphar}/dataset/facebook/facebook 4039 1642
```

Other datasets can be used in the same way, with the corresponding parameters specified as needed. We also provide a script in `script/run_neighbor_retrieval_all.sh` for reference.

### Label Filtering

To run the label filtering benchmarking component, please adjust the parameters according to the dataset (refer to `script/label_filtering.md`) for both [simple condition test](https://github.com/lixueclaire/arrow/blob/encoding-graphar/cpp/examples/parquet/graphar/test-all.cc) and [complex condition test](https://github.com/lixueclaire/arrow/blob/encoding-graphar/cpp/examples/parquet/graphar/test.cc).

Then, run the tests using the following commands:

```bash
    $ ./release/parquet-graphar-label-all-example < {graph_path} # simple-condition filtering
    $ ./release/parquet-graphar-label-example < {graph_path}     # complex-condition filtering
```

For example:

```bash
    $ ./release/parquet-graphar-label-all-example < {path_to_graphar}/dataset/bloom/bloom-43-nodes.csv
    $ ./release/parquet-graphar-label-example < {path_to_graphar}/dataset/bloom/bloom-43-nodes.csv
```

### End-to-End Workload

Once the LDBC dataset is converted into Parquet and GraphAr format, you can run the LDBC workload using a command like the following:

```bash
    $ ./release/run-work-load {path_to_dataset}/sf-30/person_knows_person {path_to_dataset}/sf-30/person_knows_person-vertex-base 165430 70220 delta
```
This command will run the LDBC workload IS-3 on the SF-30 dataset, formatted in GraphAr. The total number of person vertices is 165,430, and the query vertex id is 70,220. The delta parameter specifies the use of the delta encoding technique. For complete end-to-end LDBC workload execution, please refer to `script/run-is3.sh`, `script/run-ic8.sh`, and `script/run-bi2.sh`.

## Integration with GraphScope

### Serving as the Archive Format

To run the graph loading benchmarking: 

- First, build and install [Vineyard](https://github.com/v6d-io/v6d) (which is GraphScope's default storage backend) and [GraphScope](https://github.com/alibaba/GraphScope), following the instructions in the official documentation.
- Then, run the `script/graphscope_run_writer.sh` and `script/graphscope_run_loader.sh` scripts to dump/load the graph data from/into GraphScope using GraphAr format.

Please refer to this [page](https://graphar.apache.org/docs/libraries/cpp/examples/graphscope) for more details on  integrating GraphAr with GraphScope. Additionally, consult the [documentation](https://graphscope.io/docs/storage_engine/graphar) to learn how to use GraphAr inside GraphScope.

### Serving as the Storage Backend

Leveraging the capabilities for graph-related querying, the graph query engine within GraphScope can execute queries directly on the GraphAr data in an out-of-core manner. 
The source code for this integration is available in the [GraphScope project](https://github.com/shirly121/GraphScope/tree/gie-grin/interactive_engine/executor/assembly/grin_graphar/src).

For running the BI execution benchmarking, please:

- First, build and install the GraphScope project.
- Then, deploy the GIE (GraphScope Interactive Engine) following the instructions in the [documentation](https://graphscope.io/docs/interactive_engine/deployment).
- Finally, run the generic benchmark tool for GIE, following the steps outlined in the [documentation](https://5165d22e.graphscope-docs-preview.pages.dev/interactive_engine/benchmark_tool).


## Publication

- Xue Li, Weibin Zeng, Zhibin Wang, Diwen Zhu, Jingbo Xu, Wenyuan Yu,
  Jingren Zhou. [Enhancing Data Lakes with GraphAr: Efficient Graph Data
  Management with a Specialized Storage
  Scheme\[J\]](https://arxiv.org/abs/2312.09577). arXiv preprint
  arXiv:2312.09577, 2023.

Please cite the paper in your publications if our work helps your research.

``` bibtex
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
