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

from logging import getLogger
from pathlib import Path
from typing import List, Optional

import typer

from graphar._core import (
    get_edge_count,
    get_edge_types,
    get_vertex_count,
    get_vertex_types,
    show_edge,
    show_graph,
    show_vertex,
)
from graphar.logging import setup_logging

from graphar.importer import data_import

from . import __version__

app = typer.Typer(
    help="GraphAr Cli",
    no_args_is_help=True,
    add_completion=False,
    context_settings={"help_option_names": ["-h", "--help"]},
)

setup_logging()
logger = getLogger("graphar_cli")


@app.callback(invoke_without_command=True)
def _callback(
    ctx: typer.Context,
    version: Optional[bool] = typer.Option(
        False, "--version", "-v", help="Show GraphAr version and exit", is_eager=True
    ),
):
    """Top-level callback to support global options like --version."""
    if version:
        # Print version and exit immediately
        typer.echo(f"GraphAr CLI Version: {__version__}")
        raise typer.Exit()


@app.command(
    context_settings={"help_option_names": ["-h", "--help"]},
    help="Show the metadata",
    no_args_is_help=True,
)
def show(
    path: str = typer.Option(None, "--path", "-p", help="Path to the GraphAr config file"),
    vertex: str = typer.Option(None, "--vertex", "-v", help="Vertex type to show"),
    edge_src: str = typer.Option(None, "--edge-src", "-es", help="Source of the edge type to show"),
    edge: str = typer.Option(None, "--edge", "-e", help="Edge type to show"),
    edge_dst: str = typer.Option(
        None, "--edge-dst", "-ed", help="Destination of the edge type to show"
    ),
) -> None:
    if not Path(path).exists():
        logger.error("File not found: %s", path)
        raise typer.Exit(1)
    path = Path(path).resolve() if Path(path).is_absolute() else Path(Path.cwd(), path).resolve()
    path = str(path)
    if vertex:
        vertex_types = get_vertex_types(path)
        if vertex not in vertex_types:
            logger.error("Vertex %s not found in the graph", vertex)
            raise typer.Exit(1)
        logger.info("Vertex count: %s", get_vertex_count(path, vertex))
        logger.info(show_vertex(path, vertex))
        raise typer.Exit()
    if edge or edge_src or edge_dst:
        if not (edge and edge_src and edge_dst):
            logger.error("Edge source, edge, and edge destination must all be set")
            raise typer.Exit(1)
        edge_types: List[List[str]] = get_edge_types(path)
        found = False
        for edge_type in edge_types:
            if edge_type[0] == edge_src and edge_type[1] == edge and edge_type[2] == edge_dst:
                found = True
                break
        if not found:
            logger.error(
                "Edge type with source %s, edge %s, and destination %s not found in the graph",
                edge_src,
                edge,
                edge_dst,
            )
            raise typer.Exit(1)
        logger.info("Edge count: %s", get_edge_count(path, edge_src, edge, edge_dst))
        logger.info(show_edge(path, edge_src, edge, edge_dst))
        raise typer.Exit()
    logger.info(show_graph(path))


@app.command(
    context_settings={"help_option_names": ["-h", "--help"]},
    help="Check the metadata",
    no_args_is_help=True,
)
def check(
    path: str = typer.Option(None, "--path", "-p", help="Path to the GraphAr config file"),
):
    try:
        result_str = data_import.check(path)
    except Exception as e:
        logger.error(e)
        raise typer.Exit(1)
    logger.info(result_str)


@app.command(
    "import",
    context_settings={"help_option_names": ["-h", "--help"]},
    help="Import data",
    no_args_is_help=True,
)
def import_data(
    config_file: str = typer.Option(None, "--config", "-c", help="Path of the GraphAr config file"),
):
    try:
        result_str = data_import.import_data(config_file)
    except Exception as e:
        logger.error(e)
        raise typer.Exit(1)
    logger.info(result_str)


def main() -> None:
    app()
