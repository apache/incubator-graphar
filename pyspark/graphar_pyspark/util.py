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

from __future__ import annotations

from pyspark.sql import DataFrame

from graphar_pyspark import GraphArSession, _check_session


class IndexGenerator:
    """IndexGenerator is an object to help generating the indices for vertex/edge DataFrames."""

    @staticmethod
    def construct_vertex_index_mapping(
        vertex_df: DataFrame, primary_key: str
    ) -> DataFrame:
        """Generate a vertex index mapping from the primary key, the result DataFrame
        contains two columns: vertex index & primary key.

        :param vertex_df: input vertex DataFrame.
        :param primary_key: the primary key of vertex
        :returns: a DataFrame contains two columns: vertex index & primary key.
        """
        _check_session()
        return DataFrame(
            GraphArSession.graphar.util.IndexGenerator.constructVertexIndexMapping(
                vertex_df._jdf,
                primary_key,
            ),
            GraphArSession.ss,
        )

    @staticmethod
    def generate_vertex_index_column(vertex_df: DataFrame) -> DataFrame:
        """Add a column contains vertex index to DataFrame.

        :param vertex_df: the input vertex DataFrame.
        :returns: DataFrame that contains a new vertex index column.
        """
        _check_session()
        return DataFrame(
            GraphArSession.graphar.util.IndexGenerator.generateVertexIndexColumn(
                vertex_df._jdf
            ),
            GraphArSession.ss,
        )

    @staticmethod
    def generate_vertex_index_column_and_index_mapping(
        vertex_df: DataFrame, primary_key: str = ""
    ) -> (DataFrame, DataFrame):
        _check_session()
        jvm_res = GraphArSession.graphar.util.IndexGenerator.generateVertexIndexColumnAndIndexMapping(
            vertex_df._jdf,
            primary_key,
        )

        return (
            DataFrame(jvm_res._1(), GraphArSession.ss),
            DataFrame(jvm_res._2(), GraphArSession.ss),
        )
