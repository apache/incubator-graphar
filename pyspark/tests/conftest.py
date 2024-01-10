# copyright 2022-2023 alibaba group holding limited.
#
# licensed under the apache license, version 2.0 (the "license");
# you may not use this file except in compliance with the license.
# you may obtain a copy of the license at
#
#     http://www.apache.org/licenses/license-2.0
#
# unless required by applicable law or agreed to in writing, software
# distributed under the license is distributed on an "as is" basis,
# without warranties or conditions of any kind, either express or implied.
# see the license for the specific language governing permissions and
# limitations under the license.

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

JARS_PATH = Path(__file__).parent.parent.parent.joinpath("spark").joinpath("target")
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
