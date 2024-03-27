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

"""Bindings to com.alibaba.graphar.util."""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame

from graphar_pyspark import GraphArSession, _check_session


class IndexGenerator:
    """IndexGenerator is an object to help generating the indices for vertex/edge DataFrames."""

    @staticmethod
    def construct_vertex_index_mapping(
        vertex_df: DataFrame,
        primary_key: str,
    ) -> DataFrame:
        """Generate a vertex index mapping from the primary key.

        The resulting DataFrame contains two columns: vertex index & primary key.

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
                vertex_df._jdf,
            ),
            GraphArSession.ss,
        )

    @staticmethod
    def generate_vertex_index_column_and_index_mapping(
        vertex_df: DataFrame,
        primary_key: str = "",
    ) -> (DataFrame, DataFrame):
        """Add an index column and generate a new index mapping.

        :param vertex_df: the input vertex DataFrame.
        :param primary_key: the primary key of vertex.
        :returns: the new vertex DataFrame and mapping DataFrame.
        """
        _check_session()
        jvm_res = GraphArSession.graphar.util.IndexGenerator.generateVertexIndexColumnAndIndexMapping(
            vertex_df._jdf,
            primary_key,
        )

        return (
            DataFrame(jvm_res._1(), GraphArSession.ss),
            DataFrame(jvm_res._2(), GraphArSession.ss),
        )

    @staticmethod
    def generate_edge_index_column(edge_df: DataFrame) -> DataFrame:
        """Add a column contains edge index to input edge DataFrame.

        :param edge_df: DataFrame with edges.
        :returns: DataFrame with edges and index.
        """
        _check_session()
        return DataFrame(
            GraphArSession.graphar.util.IndexGenerator.generateEdgeIndexColumn(
                edge_df._jdf,
            ),
            GraphArSession.ss,
        )

    @staticmethod
    def generate_src_index_for_edges_from_mapping(
        edge_df: DataFrame,
        src_column_name: str,
        src_index_mapping: DataFrame,
    ) -> DataFrame:
        """Join the edge table with the vertex index mapping for source column.

        :param edge_df: edges DataFrame
        :param src_column_name: join-column
        :param src_index_mapping: mapping DataFrame
        :returns: DataFrame with index
        """
        _check_session()
        return DataFrame(
            GraphArSession.graphar.util.IndexGenerator.generateSrcIndexForEdgesFromMapping(
                edge_df._jdf,
                src_column_name,
                src_index_mapping._jdf,
            ),
            GraphArSession.ss,
        )

    @staticmethod
    def generate_dst_index_for_edges_from_mapping(
        edge_df: DataFrame,
        dst_column_name: str,
        dst_index_mapping: DataFrame,
    ) -> DataFrame:
        """Join the edge table with the vertex index mapping for destination column.

        :param edge_df: edges DataFrame
        :param dst_column_name: join-column
        :param dst_index_mapping: mapping DataFrame
        :returns: DataFrame with index
        """
        _check_session()
        return DataFrame(
            GraphArSession.graphar.util.IndexGenerator.generateDstIndexForEdgesFromMapping(
                edge_df._jdf,
                dst_column_name,
                dst_index_mapping._jdf,
            ),
            GraphArSession.ss,
        )

    @staticmethod
    def generate_src_and_dst_index_for_edges_from_mapping(
        edge_df: DataFrame,
        src_column_name: Optional[str],
        dst_column_name: Optional[str],
        src_index_mapping: DataFrame,
        dst_index_mapping: DataFrame,
    ) -> DataFrame:
        """Join the edge table with the vertex index mapping for source & destination columns.

        Assumes that the first and second columns are the src and dst columns if they are None.


        :param edge_df: edge DataFrame
        :param src_column_name: src column, optional (the first col from edge_df will be used if None)
        :param dst_column_name: dst column, optional (the second col from edge_df will be used if None)
        :param src_index_mapping: source mapping DataFrame
        :param dst_index_mapping: dest mapping DataFrame
        :returns: DataFrame with indices
        """
        _check_session()
        if src_column_name is None:
            src_column_name = edge_df.columns[0]

        if dst_column_name is None:
            dst_column_name = edge_df.columns[1]

        return DataFrame(
            GraphArSession.graphar.util.IndexGenerator.generateSrcAndDstIndexForEdgesFromMapping(
                edge_df._jdf,
                src_column_name,
                dst_column_name,
                src_index_mapping._jdf,
                dst_index_mapping._jdf,
            ),
            GraphArSession.ss,
        )

    @staticmethod
    def generate_scr_index_for_edges(
        edge_df: DataFrame,
        src_column_name: str,
    ) -> DataFrame:
        """Construct vertex index for source column.

        :param edge_df: edge DataFrame
        :param src_column_name: source column
        :returns: DataFrame with index
        """
        _check_session()
        return DataFrame(
            GraphArSession.graphar.util.IndexGenerator.generateSrcIndexForEdges(
                edge_df._jdf,
                src_column_name,
            ),
            GraphArSession.ss,
        )

    @staticmethod
    def generate_dst_index_for_edges(
        edge_df: DataFrame,
        dst_column_name: str,
    ) -> DataFrame:
        """Construct vertex index for destination column.

        :param edge_df: edge DataFrame
        :param src_column_name: destination column
        :returns: DataFrame with index
        """
        _check_session()
        return DataFrame(
            GraphArSession.graphar.util.IndexGenerator.generateDstIndexForEdges(
                edge_df._jdf,
                dst_column_name,
            ),
            GraphArSession.ss,
        )

    @staticmethod
    def generate_src_and_dst_index_unitedly_for_edges(
        edge_df: DataFrame,
        src_column_name: str,
        dst_column_name: str,
    ) -> DataFrame:
        """Union and construct vertex index for source & destination columns.

        :param edge_df: edge DataFrame
        :param src_column_name: source column name
        :param dst_column_name: destination column name
        :returns: DataFrame with index
        """
        _check_session()
        return DataFrame(
            GraphArSession.graphar.util.IndexGenerator.generateSrcAndDstIndexUnitedlyForEdges(
                edge_df._jdf,
                src_column_name,
                dst_column_name,
            ),
            GraphArSession.ss,
        )
