<h1 align="center" style="clear: both;">
    <img src="docs/images/graphar-logo.svg" width="100" alt="GraphAr">
</h1>

# Artifacts for GraphAr VLDB2025 Submission

> [!NOTE]  
> This branch is provided as artifacts for VLDB2025.   
> For the latest version of GraphAr, please refer to the branch [main](https://github.com/apache/incubator-graphar).

This repository contains the artifacts for the VLDB2025 submission of GraphAr, with all source code and guide to reproduce the results presented in the paper:

- Xue Li, Weibin Zeng, Zhibin Wang, Diwen Zhu, Jingbo Xu, Wenyuan Yu, Jingren Zhou. [GraphAr: An Efficient Storage Scheme for Graph Data in Data Lakes\[J\]](https://arxiv.org/abs/2312.09577). arXiv preprint arXiv:2312.09577, 2024.



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

Table 1 of the paper lists the graphs used in the evaluation, sourced from various datasets. We offer [instructions](https://github.com/apache/incubator-graphar/tree/research/dataset) on how to prepare the data for the evaluation, either for public datasets or synthetic datasets.

Additionally, we have stored all graph data for benchmarking in an Aliyun OSS bucket. 
To download the graphs, please use the following command:

```bash
../script/download_data.sh {path_to_dataset}
```

The data will be downloaded to the specified directory.
Please be aware that the total size of the data is approximately 2.2TB, and the download may take a long time.
We also provide some small datasets located in the `dataset` directory for testing purposes.

## Micro-Benchmark of Neighbor Retrieval

This section outlines the steps to reproduce the neighbor retrieval benchmarking results reported in Section 6.2 of the paper. You may want to use the following commands.

```bash
cd incubator-graphar/build
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

## Storage Media

The evaluation of different storage media is reported in Section 6.4 of the paper. This test employs the same methodology as the previously mentioned micro-benchmarks, using graph data stored across various storage options. 
The storage media can be specified in the path, e.g., `OSS:://bucket/dataset/facebook/facebook`, to indicate that the data is stored on OSS rather than relying on the local file system.


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
@misc{li2024grapharefficientstoragescheme,
      title={GraphAr: An Efficient Storage Scheme for Graph Data in Data Lakes}, 
      author={Xue Li and Weibin Zeng and Zhibin Wang and Diwen Zhu and Jingbo Xu and Wenyuan Yu and Jingren Zhou},
      year={2024},
      eprint={2312.09577},
      archivePrefix={arXiv},
      primaryClass={cs.DB},
      url={https://arxiv.org/abs/2312.09577}, 
}
```
