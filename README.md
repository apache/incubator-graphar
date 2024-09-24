<h1 align="center" style="clear: both;">
    <img src="docs/images/graphar-logo.svg" width="100" alt="GraphAr">
</h1>

# Artifacts for GraphAr VLDB2025 Submission

> [!NOTE]  
> This branch is provided as artifacts for VLDB2025.   
> For the latest version of GraphAr, please refer to the branch [main](https://github.com/apache/incubator-graphar).

This repository contains the artifacts for the VLDB2025 submission of GraphAr, with all source code and guide to reproduce the results presented in the paper:

- Xue Li, Weibin Zeng, Zhibin Wang, Diwen Zhu, Jingbo Xu, Wenyuan Yu,
  Jingren Zhou. [Enhancing Data Lakes with GraphAr: Efficient Graph Data
  Management with a Specialized Storage
  Scheme\[J\]](https://arxiv.org/abs/2312.09577). arXiv preprint
  arXiv:2312.09577, 2023.



## Dependencies

**GraphAr** is developed and tested on Ubuntu 20.04.5 LTS. Building GraphAr requires the following software installed as dependencies:

- A C++17-enabled compiler and build-essential tools
- CMake 3.16 or higher
- curl-devel with SSL (Linux) for s3 filesystem support

## Building Artifacts

```bash
# Clone the repository and checkout the research branch
git clone https://github.com/apache/incubator-graphar.git
cd incubator-graphar
git checkout research
git submodule update --init

# Build the artifacts
mkdir build
cd build
chmod +x ../script/build.sh
../script/build.sh
```

## Getting Graph Data

TODO: make it available.    
We stored all graph data listed in the paper Table 1 on an Aliyun OSS bucket.
To download the graphs, please use the command below:

```bash
./script/dl-data.sh
```
The data will be downloaded to the `dataset` directory.


## Micro-Benchmark of Neighbor Retrieval

This section outlines the steps to reproduce the neighbor retrieval benchmarking results reported in Section 6.2 of the paper. You may want to use the following commands.

```bash
../script/run_neighbor_retrieval.sh {graph_path} {vertex_num} {source_vertex}
```

For example: 

```bash
../script/run_neighbor_retrieval.sh {path_to_graphar}/dataset/facebook/facebook 4039 1642
```

Other datasets can be used in the same way, with the corresponding parameters specified as needed. We also provide a script in `script/run_neighbor_retrieval_all.sh` for reference.

## Micro-Benchmark of Label Filtering

This section outlines the steps to reproduce the label filtering benchmarking results reported in Section 6.3 of the paper. 

To run the label filtering benchmarking component, please adjust the parameters according to the dataset (refer to `script/label_filtering.md`) for both [simple condition test](https://github.com/lixueclaire/arrow/blob/encoding-graphar/cpp/examples/parquet/graphar/test-all.cc) and [complex condition test](https://github.com/lixueclaire/arrow/blob/encoding-graphar/cpp/examples/parquet/graphar/test.cc).

Then, run the tests using the following commands:

```bash
# simple-condition filtering
./release/parquet-graphar-label-all-example < {graph_path} 

# complex-condition filtering
./release/parquet-graphar-label-example < {graph_path} 
```

For example:

```bash
./release/parquet-graphar-label-all-example < {path_to_graphar}/dataset/bloom/bloom-43-nodes.csv

./release/parquet-graphar-label-example < {path_to_graphar}/dataset/bloom/bloom-43-nodes.csv
```

## End-to-End Graph Query Workloads

This section contains the scripts to reproduce the end-to-end graph query results reported in Section 6.5 of the paper.

Once the LDBC dataset is converted into Parquet and GraphAr format, you can run the LDBC workload using a command like the following:

```bash
./release/run-work-load {path_to_dataset}/sf-30/person_knows_person {path_to_dataset}/sf-30/person_knows_person-vertex-base 165430 70220 delta
```

This command will run the LDBC workload IS-3 on the SF-30 dataset, formatted in GraphAr. The total number of person vertices is 165,430, and the query vertex id is 70,220. The delta parameter specifies the use of the delta encoding technique. For complete end-to-end LDBC workload execution, please refer to `script/run-is3.sh`, `script/run-ic8.sh`, and `script/run-bi2.sh`.

## Integration with GraphScope

This section contains a brief guide on how to reproduce the integration results with GraphScope, as reported in Section 6.6 of the paper.

### Serving as the Archive Format

To run the graph loading benchmarking: 

- First, build and install [Vineyard](https://github.com/v6d-io/v6d), which is the default storage backend of [GraphScope](https://github.com/alibaba/GraphScope), following the instructions in the official documentation.
- Enter the build directory of Vineyard, and run the `script/graphscope_run_writer.sh` script to dump the graph data into GraphAr format.
- Run the `script/graphscope_run_loader.sh` script to load the graph data into GraphScope using GraphAr format.

Please refer to this [page](https://graphar.apache.org/docs/libraries/cpp/examples/graphscope) for more details on  integrating GraphAr with GraphScope. Additionally, consult the [documentation](https://graphscope.io/docs/storage_engine/graphar) to learn how to use GraphAr inside GraphScope.

### Serving as the Storage Backend

Leveraging the capabilities for graph-related querying, the graph query engine within GraphScope can execute queries directly on the GraphAr data in an out-of-core manner. 
The source code for this integration is available in the [GraphScope project](https://github.com/shirly121/GraphScope/tree/gie-grin/interactive_engine/executor/assembly/grin_graphar/src).

For running the BI execution benchmarking, please:

- First, build and install the GraphScope project with GraphAr integration.
- Then, deploy the GIE (GraphScope Interactive Engine) following the instructions in the [documentation](https://graphscope.io/docs/interactive_engine/deployment).
- Finally, run the generic benchmark tool for GIE, following the steps outlined in the [documentation](https://5165d22e.graphscope-docs-preview.pages.dev/interactive_engine/benchmark_tool).


## Citation

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
