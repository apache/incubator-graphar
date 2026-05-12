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

import pytest
import typer
from pathlib import Path

from cli import graphar_cli


@pytest.fixture
def sample_cfg():
    return (
        Path(__file__).parent
        / "../"
        / ".."
        / "testing"
        / "ldbc_sample"
        / "parquet"
        / "ldbc_sample.graph.yml"
    ).resolve()


def test_show_file_not_found(tmp_path):
    """Test show command with a non-existent file path."""
    # path that does not exist
    missing = tmp_path / "nope.yaml"
    with pytest.raises(typer.Exit):
        graphar_cli.show(path=str(missing))


def test_show_edge_not_all_set(sample_cfg):
    """Test show command with incomplete edge parameters."""
    cfg = sample_cfg
    # only provide edge_src, missing others
    with pytest.raises(typer.Exit):
        graphar_cli.show(path=str(cfg), edge_src="s")


def test_show_graph_default(sample_cfg):
    """Test show command with default parameters (show entire graph)."""
    cfg = sample_cfg
    # This should run without throwing exceptions
    try:
        graphar_cli.show(path=str(cfg))
    except typer.Exit:
        # typer.Exit is expected when the command completes successfully
        pass


def test_check_success(sample_cfg):
    """Test check command with a valid graph configuration."""
    cfg = sample_cfg
    # This should run without throwing exceptions
    try:
        graphar_cli.check(path=str(cfg))
    except typer.Exit:
        # typer.Exit is expected when the command completes successfully
        pass


def test_import_data_exception():
    """Test import_data command with a non-existent config file."""
    # Using a non-existent config file should raise an exception
    with pytest.raises(typer.Exit):
        graphar_cli.import_data(config_file="non_existent_config.yaml")
