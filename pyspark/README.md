<!---
 Copyright 2022-2023 Alibaba Group Holding Limited.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->


# GraphAr PySpark

This directory contains the code and build system for the GraphAr PySpark library. Library is implemented as bindings to GraphAr Scala Spark library and does not contain any real logic.


## Introduction

GraphAr PySpark project provides a PySpark API and utilities for working with GAR file format from PySpark. The prject has the only python dependency -- `pyspark` itself. Currently only `pysaprk~=3.2` is supported, but in the future the scope of supported versions will be extended.

## Installation

Currently, the only way to install `graphar_pyspark` is to build it from the source code. The project is made with poetry, so it highly recommended to use this building system.

```shell
poetry install
```

It creates a `tar.gz` file in `dist` directory.
