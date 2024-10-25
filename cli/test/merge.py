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

from enum import Enum
from pathlib import Path
from typing import List, Optional

import pandas as pd
import typer
from typing_extensions import Annotated

app = typer.Typer(no_args_is_help=True, context_settings={"help_option_names": ["-h", "--help"]})


support_file_types = {"parquet", "orc", "csv", "json"}


class FileType(str, Enum):
    parquet = "parquet"
    csv = "csv"
    orc = "orc"
    json = "json"


@app.command(
    "merge",
    context_settings={"help_option_names": ["-h", "--help"]},
    help="Merge source files",
    no_args_is_help=True,
)
def merge_data(
    files: Annotated[
        List[str], typer.Option("--file", "-f", help="Files to merge", show_default=False)
    ],
    output_file: Annotated[
        str, typer.Option("--output", "-o", help="Output file", show_default=False)
    ],
    type: Annotated[
        Optional[FileType], typer.Option("--type", "-t", help="Type of data to output", show_default=False)
    ] = None,
):
    if not files:
        typer.echo("No files to merge")
        raise typer.Exit(1)
    if not output_file:
        typer.echo("No output file")
        raise typer.Exit(1)
    data = []
    for file in files:
        path = Path(file)
        if not path.is_file():
            typer.echo(f"File {file} not found")
            raise typer.Exit(1)
        file_type = path.suffix.removeprefix(".")
        if file_type == "":
            typer.echo(f"File {file} has no file type suffix")
            raise typer.Exit(1)
        if file_type not in support_file_types:
            typer.echo(f"File type {file_type} not supported")
            raise typer.Exit(1)
        if file_type == "parquet":
            data.append(pd.read_parquet(file))
        elif file_type == "csv":
            data.append(pd.read_csv(file))
        elif file_type == "orc":
            data.append(pd.read_orc(file))
        elif file_type == "json":
            data.append(pd.read_json(file))
    output_path = Path(output_file)
    if output_path.is_file():
        typer.echo(f"Output file {output_file} already exists")
        if not typer.prompt("Do you want to overwrite it?", default=False):
            raise typer.Exit(1)
    if not type:
        type = output_path.suffix.removeprefix(".")
    result = pd.concat(data, ignore_index=True)
    if type == "parquet":
        result.to_parquet(output_file)
    elif type == "csv":
        result.to_csv(output_file)
    elif type == "orc":
        result.to_orc(output_file)
    elif type == "json":
        result.to_json(output_file, orient="records", lines=True)
    typer.echo(f"Data merged to {output_file}")


if __name__ == "__main__":
    app()
