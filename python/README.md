# GraphAr Python SDK

The GraphAr Python SDK provides Python bindings for the GraphAr C++ library.
It lets Python applications read GraphAr metadata, use the high-level graph APIs,
and run the bundled `graphar` command-line tool.

This package is separate from the PySpark package in [`../pyspark`](../pyspark).

## Requirements

- Python >= 3.9
- pip
- CMake >= 3.15, Apache Arrow >= 12.0, and a C++ toolchain when building from source

## Install

### From PyPI

```bash
pip install -U graphar
```

Verify the installation:

```bash
python -c "import graphar; print(graphar.GraphInfo)"
graphar --help
```

### From Source

Clone the repository, then from its root:

```bash
pip install ./python

# for local development
pip install -e ./python
```

### Docker

The project also publishes a development image:

```bash
docker run -it ghcr.io/apache/graphar-dev
```

## Quick Start

Load graph metadata from a GraphAr YAML file:

```python
import graphar

graph_info = graphar.GraphInfo.load("path/to/graph.graph.yml")

print(graph_info.get_name())
print(graph_info.get_vertex_info("person").get_type())
print(graph_info.get_edge_info("person", "knows", "person").get_edge_type())
```

Replace `path/to/graph.graph.yml` with the path to a GraphAr graph metadata file.

## Modules

The Python SDK exposes the core GraphAr functionality through these modules:

- [`graphar.graph_info`](src/graphar/graph_info.py): graph, vertex, edge, property, and metadata APIs.
- [`graphar.high_level`](src/graphar/high_level.py): high-level vertex and edge collection APIs.
- [`graphar.types`](src/graphar/types.py): GraphAr enum types used by metadata and high-level APIs.

## Examples

Example scripts are available in [`python/example`](example):

- [`graph_info_example.py`](example/graph_info_example.py) shows how to load graph metadata and inspect vertex and edge information.
- [`high_level_example.py`](example/high_level_example.py) shows how to use the high-level vertex and edge collection APIs.

The examples expect `GAR_TEST_DATA` to point to a directory that contains the
`ldbc_sample/parquet/ldbc_sample.graph.yml` test graph:

```bash
bash dev/download_test_data.sh
export GAR_TEST_DATA=/tmp/graphar-testing
python python/example/graph_info_example.py
python python/example/high_level_example.py
```

## Command-Line Interface

The package installs a `graphar` command-line tool:

```bash
graphar --help
graphar show --path path/to/graph.graph.yml
graphar check --path path/to/graph.graph.yml
```

See [`python/src/cli/README.md`](src/cli/README.md) for more CLI examples.

## API Documentation

Build the Python API documentation from the `python` directory:

```bash
make install_docs
make docs
```

The generated documentation is written to `python/docs`.

## Development

Install test dependencies from the repository root:

```bash
pip install -e "./python[test]"
```

Run the Python tests from the `python` directory:

```bash
pytest
```

Some tests require `GAR_TEST_DATA`; use [`dev/download_test_data.sh`](../dev/download_test_data.sh)
if the test data is not available locally.

For general contribution guidelines, see [`../CONTRIBUTING.md`](../CONTRIBUTING.md).

## License

GraphAr is distributed under the Apache License 2.0. See [`../LICENSE`](../LICENSE)
and [`../NOTICE`](../NOTICE) for details.
