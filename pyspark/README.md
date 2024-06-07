# GraphAr PySpark (under development)

This directory contains the code and build system for the GraphAr PySpark library. Library is implemented as bindings to GraphAr Scala Spark library and does not contain any real logic.


## Introduction

GraphAr PySpark project provides a PySpark API and utilities for working with GraphAr file format from PySpark. The project has the only python dependency -- `pyspark` itself. Currently only `pysaprk~=3.2` is supported, but in the future the scope of supported versions will be extended.

## Installation

Currently, the only way to install `graphar_pyspark` is to build it from the source code. The project is made with poetry, so it highly recommended to use this building system.

```shell
poetry install
```

It creates a `tar.gz` file in `dist` directory.

## Generating API documentation

To generate API documentation, run the following command:

```shell
poetry install --with=docs
poetry run pdoc -t ./template --output-dir ./docs graphar_pyspark
```

The documentation will be generated in the `docs` directory.
