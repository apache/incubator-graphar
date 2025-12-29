# GraphAr Python SDK

GraphAr Python SDK provides Python bindings for the GraphAr C++ library, allowing user to work with GraphAr formatted graph data in Python environments. It includes both a high-level API for data manipulation and a command-line interface for common operations.

## Installation

### Prerequisites

- Python >= 3.7
- pip (latest version recommended)
- CMake >= 3.15 (for building from source)
- Apache Arrow >= 12.0 (for building from source)

### Install from Pypi
Install the latest released version from PyPI:

```bash
pip install -U graphar
```

### Install from Source

Clone the repository and install the Python package:

```bash
git clone https://github.com/apache/incubator-graphar.git
cd incubator-graphar
pip install ./python
```

For verbose output during installation:

```bash
pip install -v ./python
```

### Using Docker (Recommended)

The easiest way to get started is by using our pre-configured Docker environment:

```bash
docker run -it ghcr.io/apache/graphar-dev
```

## Quick Start

### Importing the Package

After installation, you can import the GraphAr Python SDK in your Python scripts:

```python
import graphar
```

### Basic Usage

Loading graph information:

```python
import graphar

# Load graph info from a YAML file
graph_info = graphar.graph_info.GraphInfo.load("path/to/graph.yaml")

# Access vertex information
vertex_info = graph_info.get_vertex_info("person")
print(f"Vertex type: {vertex_info.get_type()}")

# Access edge information
edge_info = graph_info.get_edge_info("person", "knows", "person")
print(f"Edge type: {edge_info.get_edge_type()}")
```

## Command-Line Interface

GraphAr Python SDK also provides a command-line interface for common operations such as checking metadata, showing graph information, and importing data.

For detailed information about the CLI functionality, please see [CLI Documentation](src/cli/README.md).

## API Documentation

### build docs
```bash
make install_docs
make docs
```

The Python SDK exposes the core GraphAr functionality through several modules:

- `graphar.graph_info`: Main API for working with graph, vertex, and edge information
- `graphar.high_level`: High-level API for data reading and writing

## Examples
> [!NOTE]
> under development.

You can find various examples in the [examples directory](../cpp/examples/) which demonstrate usage of the underlying C++ library. These concepts translate directly to the Python SDK.

## Development

To contribute to the Python SDK, please follow the guidelines in the main [CONTRIBUTING.md](../CONTRIBUTING.md) file.

## License

**GraphAr** is distributed under [Apache License
2.0](https://github.com/apache/incubator-graphar/blob/main/LICENSE).
Please note that third-party libraries may not have the same license as
GraphAr.
