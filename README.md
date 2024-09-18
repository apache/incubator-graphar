<h1 align="center" style="clear: both;">
    <img src="docs/images/graphar-logo.svg" width="350" alt="GraphAr">
</h1>
<p align="center">
    An open source, standard data file format for graph data storage and retrieval
</p>

This project is a research initiative by GraphAr (short for "Graph Archive") aimed at providing an efficient storage scheme for graph data in data lakes. It is designed to enhance the efficiency of data lakes utilizing the capabilities of existing formats, with a specific focus on [Apache Parquet](https://github.com/apache/parquet-format). GraphAr ensures seamless integration with existing tools and introduces innovative additions specifically tailored to handle LPGs (Labeled Property Graphs).
Leveraging the strengths of Parquet, GraphAr captures LPG semantics precisely and facilitates graph-specific operations such as neighbor retrieval and label filtering. Through innovative data organization, encoding, and decoding techniques, GraphAr dramatically improves performance. 

## The GraphAr Format

See [GraphAr Format](https://github.com/apache/incubator-graphar/blob/research/GRAPHAR.md) for more details about the GraphAr format.


## Dependencies

**GraphAr** is developed and tested on Ubuntu 20.04.5 LTS. It should also work on other unix-like distributions. Building GraphAr requires the following softwares installed as dependencies:

- A C++17-enabled compiler. On Linux, gcc 7.1 and higher should be sufficient. For MacOS, at least clang 5 is required
- CMake 3.5 or higher
- On Linux and macOS, ``make`` build utilities
- curl-devel with SSL (Linux) or curl (macOS), for s3 filesystem support
- Apache Arrow C++ (>= 12.0.0, requires `arrow-dev`, `arrow-dataset`, `arrow-acero` and `parquet` modules) for Arrow filesystem support


## Building Steps

### Step 1: Clone the Repository

```bash
    $ git clone https://github.com/apache/incubator-graphar.git
    $ cd incubator-graphar"
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

## Preparing Graph Datasets

Before running the benchmarking experiments, you need to prepare the graph datasets. You could download public graph datasets, or generate synthetic graph datasets using our data generator.

### Topology Graphs

#### Step 1
Suppose we want to use the facebook dataset. First, download the dataset from the [SNAP](https://snap.stanford.edu/data/egonets-Facebook.html) website and extract the dataset.
As an example, we have included the facebook dataset in the `dataset' directory.

#### Step 2
Convert the dataset into the Parquet format using the following command:

```bash
    $ ./release/Csv2Parquet {input_path} {output_path} {ignore_line_num}
```
Or, you could use the following command to convert the dataset into the GraphAr format:

```bash
    $ ./release/data-generator {input_path} {output_path} {vertex_num} {is_directed} {is_weighted} {is_sorted} {is_reverse} {delimiter} {ignore_line_num}
```

For example, 

```bash
    $ ./release/data-generator {path_to_graphar}/dataset/facebook/facebook.txt {path_to_graphar}/dataset/facebook/facebook 4039 false false true false space 0
```

The above command will convert the facebook dataset into the Parquet and GraphAr format and store the output in the `dataset` directory.


### Labeled Graphs

### LDBC Graphs

### Graphs Generated using Our Data Generator


## Running Benchmarking Experiments

### Neighbor Retrieval

For testing the neighbor retrieval performance, using the following command:

```bash
    $ ../script/run_neighbor_retrieval.sh {graph_path} {vertex_num} {source_vertex}
```

For example, 

```bash
    $ ../script/run_neighbor_retrieval.sh {path_to_graphar}/dataset/facebook/facebook 4039 1642
```

### Label Filtering

### LDBC Workload


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
