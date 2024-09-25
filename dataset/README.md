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

For example, the `dataset/bloom` directory contains the label data for the [Bloom](https://github.com/neo4j-graph-examples/bloom/tree/main) dataset. This dataset includes 32,960 vertices and 18 labels. The `dataset/label` directory contains extracted label data for various datasets outlined in `script/label_filtering.md`, excluding extremely large datasets.


### Graphs from the LDBC Benchmark

Graphs from the LDBC benchmark are generated using the [LDBC SNB Data Generator](https://ldbcouncil.org/post/snb-data-generator-getting-started/) tool in CSV format. Each dataset consists of multiple CSV files, where each file represents a specific edge or vertex type, e.g., [person_knows_person_0_0.csv](https://github.com/apache/incubator-graphar-testing/blob/main/ldbc_sample/person_knows_person_0_0.csv) and [person_0_0.csv](https://github.com/apache/incubator-graphar-testing/blob/main/ldbc_sample/person_0_0.csv).
Once the original dataset is generated, you can convert it into Parquet/GraphAr format as described above.

The following command will generate the Parquet and GraphAr files for `person_knows_person` data of the SF30 dataset:

```bash
    $ ../script/generate_ldbc.sh {path_to_dataset}/sf30/social_network/dynamic/person_knows_person_0_0.csv  {path_to_dataset}/sf30/social_network/dynamic/person_0_0.csv {path_to_dataset}/sf30/person_knows_person
```

Please refer to `script/generate_ldbc_all.sh` for more details on this preparation process.
