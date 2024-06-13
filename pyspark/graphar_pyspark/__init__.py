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

"""GraphSession and initialization."""

from pyspark.sql import SparkSession

from graphar_pyspark.errors import GraphArIsNotInitializedError

__version__ = "0.13.0.dev"

class _GraphArSession:
    """Singleton GraphAr helper object, that contains SparkSession and JVM.

    It is implemented as a module-level instance of the class.
    """

    def __init__(self) -> None:
        self.ss = None
        self.sc = None
        self.jvm = None
        self.graphar = None
        self.jsc = None
        self.jss = None

    def set_spark_session(self, spark_session: SparkSession) -> None:
        self.ss = spark_session  # Python SparkSession
        self.sc = spark_session.sparkContext  # Python SparkContext
        self.jvm = spark_session._jvm  # JVM
        self.graphar = spark_session._jvm.org.apache.graphar  # Alias to scala graphar
        self.jsc = spark_session._jsc  # Java SparkContext
        self.jss = spark_session._jsparkSession  # Java SparkSession

    def is_initialized(self) -> bool:
        return self.ss is not None


GraphArSession = _GraphArSession()


def initialize(spark: SparkSession) -> None:
    """Initialize GraphAr session.

    :param spark: pyspark SparkSession object.
    """
    GraphArSession.set_spark_session(
        spark,
    )  # modify the global GraphArSession singleton.


def _check_session() -> None:
    if not GraphArSession.is_initialized():
        msg = "GraphArSession is not initialized. Call `pyspark_graphar.initialize` first!"
        raise GraphArIsNotInitializedError(msg)
