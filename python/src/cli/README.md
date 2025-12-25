# GraphAr Python CLI

GraphAr python cli uses [pybind11][] and [scikit-build-core][] to bind C++ code into Python and build command line tools through Python. Command line tools developed using [typer][].

[pybind11]: https://pybind11.readthedocs.io
[scikit-build-core]: https://scikit-build-core.readthedocs.io
[typer]: https://typer.tiangolo.com/

## Requirements

- Linux (work fine on Ubuntu 22.04)
- Cmake >= 3.15
- Arrow >= 12.0
- Python >= 3.7
- pip == latest


The best testing environment is `ghcr.io/apache/graphar-dev` Docker environment.

And using Python in conda or venv is a good choice. 

## Installation

### Install from Pypi
Install the latest released version from PyPI:

```bash
pip install -U graphar
```

### Install from Source

- Clone this repository
- `pip install ./python` or set verbose level `pip install -v ./python`

## Usage

```bash
graphar --help

# check the metadata, verify whether the vertex edge information and attribute information of the graph are valid
graphar check -p ../testing/neo4j/MovieGraph.graph.yml

# show the vertex
graphar show -p ../testing/neo4j/MovieGraph.graph.yml -v Person

# show the edge
graphar show -p ../testing/neo4j/MovieGraph.graph.yml -es Person -e ACTED_IN -ed Movie

# import graph data by using a config file
graphar import -c ../testing/neo4j/data/import.mini.yml
```

## Import config file

The config file supports `yaml` data type. We provide two reference templates for it: full and mini.

The full version of the configuration file contains all configurable fields, and additional fields will be automatically ignored.

The mini version of the configuration file is a simplified version of the full configuration file, retaining the same functionality. It shows the essential parts of the configuration information. 

For the full configuration file, if all fields can be set to their default values, you can simplify it to the mini version. However, it cannot be further reduced beyond the mini version.

In the full `yaml` config file, we provide brief comments on the fields, which can be used as a reference.

**Example**

To import the movie graph data from the `testing` directory, you first need to prepare data files. Supported file types include `csv`, `json`(as well as`jsonline`, but should have the `.json` extension), `parquet`, and `orc` files. Please ensure the correct file extensions are set in advance, or specify the `file_type` field in the source section of the configuration. The `file_type` field will ignore the file extension.

Next, write a configuration file following the provided sample. Any empty fields in the `graphar` configuration will be filled with default values. In the `import_schema`, empty fields will use the global configuration values from `graphar`. If fields in `import_schema` are not empty, they will override the values from `graphar`.

A few important notes:

1. The sources list specifies configuration for the data source files. For `csv` files, you can set the `delimiter`. The format of the `json` file should be given in the format of `jsonline`.

2. The columns dictionary maps column names in the data source to node or edge properties. Keys represent column names in the data source, and values represent property names.

3. Currently, edge properties cannot have the same names as the edge endpoints' properties; doing so will raise an exception.

4. The following table lists the default fields, more of which are included in the full configuration.


| Field                             | Default value        |
| -----------                       | -----------          |
|  `graphar.vertex_chunk_size`      | `100`                |
|  `graphar.edge_chunk_size`        | `1024`               |
|  `graphar.file_type`              | `parquet`            |
|  `graphar.adj_list_type`          | `ordered_by_source`  |
|  `graphar.validate_level`         | `weak`               |
|  `graphar.version`                | `gar/v1`             |
|  `property.nullable`              | `true`               |




Wish you a happy use！