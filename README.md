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

**GraphAr** is developed and tested on Ubuntu 20.04.5 LTS. It should also work on other unix-like distributions. Building GraphAr requires the following softwares installed as dependencies:

- A C++17-enabled compiler. On Linux, gcc 7.1 and higher should be sufficient. For MacOS, at least clang 5 is required
- CMake 3.16 or higher
- On Linux and macOS, ``make`` build utilities
- curl-devel with SSL (Linux) or curl (macOS), for s3 filesystem support


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

Before running the benchmarking components, you need to prepare the graph datasets. You could download from public graph datasets, or generate synthetic graph datasets using our data generator.

### Preparing Topology Graphs

#### Transforming Public Graphs

Suppose we want to use the facebook dataset. First, download the dataset from the [SNAP](https://snap.stanford.edu/data/egonets-Facebook.html) website and extract the dataset.
As an example, we have already included this facebook dataset in the `dataset` directory.

Then, convert the dataset into the Parquet format:

```bash
    $ cd incubator-graphar/build
    $ ./release/Csv2Parquet {input_path} {output_path} {header_line_num}
```
Or, you could use the following command to convert the dataset into the GraphAr format:

```bash
    $ ./release/data-generator {input_path} {output_path} {vertex_num} {is_directed} {is_weighted} {is_sorted} {is_reversed} {delimiter} {header_line_num}
```

For example, running the command for the facebook dataset:

```bash
    $ ./release/data-generator {path_to_graphar}/dataset/facebook/facebook.txt {path_to_graphar}/dataset/facebook/facebook 4039 false false true false space 0
```

The above commands will convert the facebook dataset into the Parquet and GraphAr format and store the output in the `dataset/facebook` directory.

#### Generating Synthetic Graphs

We also provide a data generator to generate synthetic graph datasets. The data generator is located in the `synthetic` directory. You could use the following command to generate a synthetic graph dataset:

```bash
    $ cd synthetic
    $ makedir build
    $ cd build
    $ cmake ..
    $ make
    $ ./DataGenerator {vertex_num} {output_path} # e.g., ./DataGenerator 100 output.csv
```

It will generate a synthetic graph dataset with the specified number of vertices, in the CSV format. Afterward, you could convert this CSV file into the Parquet or GraphAr format using the `Csv2Parquet` or `data-generator` tool, as described above.

### Preparing Labeled Graphs

### Generating Label Data

To enable the label filtering benchmarking component, original label data must be extracted from graphs obtained from various sources. We use the CSV file to store the label data, where each row represents a vertex and each column represents a label, i.e., in the form of a binary matrix. For example, the `dataset/bloom` directory contains the label data for the [bloom](https://github.com/neo4j-graph-examples/bloom/tree/main) dataset. This dataset includes 32,960 vertices and 18 labels.


### Graphs from LDBC Benchmark
Graphs from the LDBC benchmark are generated using the [LDBC SNB Data Generator](https://ldbcouncil.org/post/snb-data-generator-getting-started/) tool. As an illustration, we have included a `SF1` dataset (scale factor 1) in the `dataset` directory. You could use the following command to convert the LDBC dataset into the Parquet and GraphAr format:

```bash
    $ [TODO]
```


## Running Benchmarking Components

### Neighbor Retrieval

For running the neighbor retrieval benchmarking component, you could use the following command:

```bash
    $ ../script/run_neighbor_retrieval.sh {graph_path} {vertex_num} {source_vertex}
```

For example, 

```bash
    $ ../script/run_neighbor_retrieval.sh {path_to_graphar}/dataset/facebook/facebook 4039 1642
```

Other datasets could be used in the same way, with the corresponding parameters specified as needed. We also provide a script in `script/run_neighbor_retrieval_all.sh` for reference.

### Label Filtering

To running the label filtering benchmarking component, please adjust the parameters according to the dataset (refer to `script/label_filtering.md` for reference), at the beginning of the [[TODO]simple condition test]() and [[TODO]complex condition test]().

Then, you could use the following command:

```bash
    $ ./release/parquet-graphar-label-all-example < {graph_path} # simple-condition filtering
    $ ./release/parquet-graphar-label-example < {graph_path}     # complex-condition filtering
```

For example, 

```bash
    $ ./release/parquet-graphar-label-all-example < {path_to_graphar}/dataset/bloom/bloom-43-nodes.csv
    $ ./release/parquet-graphar-label-example < {path_to_graphar}/dataset/bloom/bloom-43-nodes.csv
```

### LDBC Workload

[TODO]

### Integration with GraphScope

[TODO]


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
