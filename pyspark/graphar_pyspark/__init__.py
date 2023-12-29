"""
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
"""

from pyspark.sql import SparkSession


class _GraphArSession:
    """Singleton GraphAr helper object, that contains SparkSession and JVM.

    It is implemented as a module-level instance of the class.
    """

    def __init__(self) -> None:
        self._ss = None
        self._sc = None
        self._jvm = None
        self._graphar = None
        self._jsc = None
        self._jss = None

    def set_spark_session(self, spark_session: SparkSession) -> None:
        self._ss = spark_session  # Python SparkSession
        self._sc = spark_session.sparkContext  # Python SparkContext
        self._jvm = spark_session._jvm  # JVM
        self._graphar = spark_session._jvm.com.alibaba.graphar  # Alias to scala graphar
        self._jsc = spark_session._jsc  # Java SparkContext
        self._jss = spark_session._jsparkSession  # Java SparkSession


GraphArSession = _GraphArSession()


def initialize(spark: SparkSession) -> None:
    """Initialize GraphAr session.

    :param spark: pyspark SparkSession object.
    """
    GraphArSession.set_spark_session(spark) # modify the global GraphArSession singleton.
