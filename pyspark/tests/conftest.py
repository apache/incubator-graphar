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
