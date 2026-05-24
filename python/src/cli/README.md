# GraphAr Python CLI

The GraphAr Python package installs a `graphar` command-line tool for inspecting
GraphAr metadata and importing data into GraphAr format.

The CLI is implemented with [Typer][] and uses the same Python bindings as the
[`graphar` Python package](../../README.md).

[Typer]: https://typer.tiangolo.com/

## Requirements

- Python >= 3.9
- pip
- CMake >= 3.15, Apache Arrow >= 12.0, and a C++ toolchain when building from source

## Installation

Install the latest released version from PyPI:

```bash
pip install -U graphar
```

Or install from the repository root:

```bash
pip install ./python
```

Verify the CLI is available:

```bash
graphar --help
```

## Usage

Replace the paths below with paths to your GraphAr metadata or import config
files.

```bash
# Show all graph metadata.
graphar show --path path/to/graph.graph.yml

# Validate graph metadata.
graphar check --path path/to/graph.graph.yml

# Show one vertex type.
graphar show --path path/to/graph.graph.yml --vertex Person

# Show one edge type.
graphar show \
  --path path/to/graph.graph.yml \
  --edge-src Person \
  --edge ACTED_IN \
  --edge-dst Movie

# Import data with a config file.
graphar import --config path/to/import.yml
```

Short options are also available:

```bash
graphar show -p path/to/graph.graph.yml -v Person
graphar show -p path/to/graph.graph.yml -es Person -e ACTED_IN -ed Movie
graphar import -c path/to/import.yml
```

## Import Config

The import command reads a YAML config file. A config describes source files,
GraphAr output settings, and how source columns map to vertex or edge
properties.

Supported source file types are `csv`, `json`, `parquet`, and `orc`. JSON input
uses JSON Lines format and should use the `.json` extension. You can override
extension-based detection by setting `file_type` in the source config.

Important fields:

1. `sources` describes the input files. CSV sources can set a `delimiter`.
2. `columns` maps source column names to GraphAr property names.
3. Edge property names must not duplicate endpoint property names.
4. Empty fields in `import_schema` use values from the top-level `graphar`
   config. Explicit `import_schema` values override the top-level defaults.

Common defaults:

| Field                             | Default value        |
| --------------------------------- | -------------------- |
| `graphar.vertex_chunk_size`       | `100`                |
| `graphar.edge_chunk_size`         | `1024`               |
| `graphar.file_type`               | `parquet`            |
| `graphar.adj_list_type`           | `ordered_by_source`  |
| `graphar.validate_level`          | `weak`               |
| `graphar.version`                 | `gar/v1`             |
| `property.nullable`               | `true`               |
