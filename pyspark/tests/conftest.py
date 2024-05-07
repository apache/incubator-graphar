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

import pytest
from pyspark.sql import SparkSession

JARS_PATH = Path(__file__).parent.parent.parent.joinpath("maven-projects/spark").joinpath("graphar").joinpath("target")
GRAPHAR_SHADED_JAR_PATH = None

for jar_file in JARS_PATH.glob("*.jar"):
    if "shaded" in jar_file.name:
        GRAPHAR_SHADED_JAR_PATH = jar_file.absolute()

if GRAPHAR_SHADED_JAR_PATH is None:
    raise FileNotFoundError("You need to build scala-part first!")


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("graphar-pyspark-local-tests")
        .config("spark.jars", str(GRAPHAR_SHADED_JAR_PATH))
        .getOrCreate()
    )
    yield spark
    spark.stop()
