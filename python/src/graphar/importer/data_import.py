# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from pathlib import Path

import yaml

from .._core import (
    check_edge,
    check_graph,
    check_vertex,
    do_import,
    get_edge_types,
    get_vertex_types,
)
from .config import ImportConfig
from .importer import validate


def check(path: str):
    if not Path(path).exists():
        raise ValueError(f"File not found: {path}")
    path = Path(path).resolve() if Path(path).is_absolute() else Path(Path.cwd(), path).resolve()
    path = str(path)
    vertex_types = get_vertex_types(path)
    for vertex_type in vertex_types:
        if not check_vertex(path, vertex_type):
            raise ValueError(f"Vertex type {vertex_type} is not valid")
    edge_types = get_edge_types(path)
    for edge_type in edge_types:
        if edge_type[0] not in vertex_types:
            raise ValueError(f"Source vertex type {edge_type[0]} not found in the graph")
        if edge_type[2] not in vertex_types:
            raise ValueError(f"Destination vertex type {edge_type[2]} not found in the graph")
        if not check_edge(path, edge_type[0], edge_type[1], edge_type[2]):
            raise ValueError(f"Edge type {edge_type[0]}_{edge_type[1]}_{edge_type[2]} is not valid")
    if not check_graph(path):
        raise ValueError("Graph is not valid")
    return "Graph is valid"


def import_data(config_file: str):
    if not Path(config_file).is_file():
        raise ValueError(f"File not found: {config_file}")

    try:
        with Path(config_file).open(encoding="utf-8") as file:
            config = yaml.safe_load(file)
        import_config = ImportConfig(**config)
        validate(import_config)
    except Exception as e:
        raise ValueError(f"Invalid config: {e}")
    try:
        res = do_import(import_config.model_dump())
    except Exception as e:
        raise ValueError(f"Import failed: {e}")
    return res
