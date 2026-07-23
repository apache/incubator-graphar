<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# GraphAr Go SDK

Pure-Go SDK for [Apache GraphAr](https://github.com/apache/incubator-graphar).
The SDK reads and writes the same on-disk YAML schema as the C++, Java and
Rust reference implementations, so a graph produced by any of them is
loadable by any other.

> Status: in development. This module currently ships the build and CI
> scaffold only; the metadata layer (`types/`, `info/`) and the data layer
> (`reader/`, `writer/`) are planned.

## Install

```bash
go get github.com/apache/incubator-graphar/go/graphar
```

Requires Go 1.23 or newer.

## Quick example

The `info` package lands in the next stacked PR; the snippet below shows the
intended API once it does.

```go
import (
    "io/fs"
    "os"

    "github.com/apache/incubator-graphar/go/graphar/info"
)

func main() {
    fsys := os.DirFS("/path/to/graph")
    g, err := info.LoadGraphInfo(fsys, "modern_graph.graph.yml")
    if err != nil {
        panic(err)
    }
    if v, ok := g.Vertex("person"); ok {
        for _, name := range v.PropertyGroups().Names() {
            println(name)
        }
    }
}
```

## Packages

The module is currently a build/CI scaffold; the packages below are planned:

| Package | What will live here |
|---|---|
| `graphar/types` | Primitive value types: `DataType`, `FileType`, `AdjListType`, `Cardinality`, `InfoVersion`. No external dependencies. |
| `graphar/info` | Graph metadata model — `Property`, `PropertyGroup`, `VertexInfo`, `EdgeInfo`, `GraphInfo` — plus YAML load/save. Validates against the cpp reference rules so files round-trip across SDKs. |

## Development

```bash
cd go/graphar
make ci         # gofmt + vet + lint + race tests + coverage floor
make coverage   # produce coverage.out and print per-package coverage
```

Once the `info` package lands, a cross-language interop gate
(`info.TestLoadAllFixtures`) will walk the [`testing/`](../testing) submodule
for every `*.graph.yml` and verify it loads and round-trips through
`MarshalGraphInfo`, skipping when the submodule is not initialised.

## Reporting issues / proposing changes

Open an issue at https://github.com/apache/incubator-graphar/issues. For
larger changes (over 300–500 lines of diff), please start a discussion
first as described in
[CONTRIBUTING.md](../CONTRIBUTING.md).
