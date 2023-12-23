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

from typing import Optional, Union

from py4j.java_gateway import JavaObject
from pyspark.sql import DataFrame
from pyspark.sql.readwriter import OptionUtils

from graphar_pysaprk import GraphArSession
from graphar_pysaprk.enums import FileType
from graphar_pysaprk.info import GraphInfo


class GraphReader:
    """The helper object for reading graph through the definitions of graph info."""

    @staticmethod
    def _jvm2py(
        jvm_result: tuple[
            dict[str, JavaObject], dict[tuple[str, str, str], dict[str, JavaObject]]
        ],
    ) -> tuple[dict[str, DataFrame], dict[tuple[str, str, str], dict[str, DataFrame]]]:
        """Helper that convert nested Java DataFrames to PySpark DataFrames."""
        return (
            {
                label: DataFrame(jdf, GraphArSession._ss)
                for label, jdf in jvm_result[0].items()
            },
            {
                complex_label: {
                    adj_type: DataFrame(jdf, GraphArSession._ss)
                    for adj_type, jdf in mapping.items()
                }
                for complex_label, mapping in jvm_result[1].items()
            },
        )

    @staticmethod
    def read_with_graph_info(
        graph_info: GraphInfo,
    ) -> tuple[dict[str, DataFrame], dict[tuple[str, str, str], dict[str, DataFrame]]]:
        """Reading the graph as vertex and edge DataFrames with the graph info object.

        :param graph_info: The info object for the graph.
        :returns: Pair of vertex DataFrames and edge DataFrames, the vertex DataFrames are
        stored as the map of (vertex_label -> DataFrame) the edge DataFrames are stored as a map of
        ((srcLabel, edgeLabel, dstLabel) -> (adj_list_type_str -> DataFrame))
        """
        jvm_result = GraphArSession._graphar.graph.GraphReader.readWithGraphInfo(
            graph_info.to_scala(), GraphArSession._jss
        )
        return GraphReader._jvm2py(jvm_result)

    @staticmethod
    def read(
        graph_info_path: str,
    ) -> tuple[dict[str, DataFrame], dict[tuple[str, str, str], dict[str, DataFrame]]]:
        """Reading the graph as vertex and edge DataFrames with the graph info yaml file.

        :param graph_info_path: The path of the graph info yaml.
        :returns: Pair of vertex DataFrames and edge DataFrames, the vertex DataFrames are
        stored as the map of (vertex_label -> DataFrame) the edge DataFrames are stored as a map of
        ((srcLabel, edgeLabel, dstLabel) -> (adj_list_type_str -> DataFrame))
        """
        graph_info = GraphInfo.load_graph_info(graph_info_path)
        return GraphReader.read_with_graph_info(graph_info)


class GraphWriter:
    def __init__(self, jvm_obj: JavaObject) -> None:
        """One should not use this constructor directly, please use `from_scala` or `from_python`."""
        self._jvm_graph_writer_obj = jvm_obj

    def to_scala(self) -> JavaObject:
        """Transform object to JVM representation.

        :returns: JavaObject
        """
        return self._jvm_graph_writer_obj

    @staticmethod
    def from_scala(jvm_obj) -> "GraphWriter":
        """Create an instance of the Class from the corresponding JVM object.

        :param jvm_obj: scala object in JVM.
        :returns: instance of Python Class.
        """
        return GraphWriter(jvm_obj)

    @staticmethod
    def from_python() -> "GraphWriter":
        """Create an instance of the Class from Python arguments."""
        return GraphWriter(GraphArSession._graphar.graph.GraphWriter())

    def put_vertex_data(self, label: str, df: DataFrame, primary_key: str) -> None:
        """Put the vertex DataFrame into writer.

        :param label: label of vertex.
        :param df: DataFrame of the vertex type.
        :param primary_key: primary key of the vertex type, default is empty, which take the first property column as primary key.
        """
        self._jvm_graph_writer_obj.PutVertexData(label, df._jdf, primary_key)

    def put_edge_data(self, relation: tuple[str, str, str], df: DataFrame) -> None:
        """Put the egde datafrme into writer.

        :param relation: 3-Tuple (source label, edge label, target label) to indicate edge type.
        :param df: data frame of edge type.
        """
        self._jvm_graph_writer_obj.PutEdgeData(relation, df._jdf)

    def write_with_graph_info(self, graph_info: Union[GraphInfo, str]) -> None:
        """Write the graph data in graphar format with graph info.

        Note: original method is `write` but there is not directly overloading in Python.

        :param graph_info: the graph info object for the graph or the path to graph info object.
        """
        if isinstance(graph_info, str):
            self._jvm_graph_writer_obj.write(graph_info, GraphArSession._jss)
        else:
            self._jvm_graph_writer_obj.write(graph_info.to_scala(), GraphArSession._jss)

    def write(
        self,
        path: str,
        name: str = "graph",
        vertex_chunk_size: Optional[int] = None,
        edge_chunk_size: Optional[int] = None,
        file_type: Optional[FileType] = None,
        version: Optional[str] = None,
    ) -> None:
        """Write graph data in graphar format.


        Note: for default parameters check com.alibaba.graphar.GeneralParams;
        For this method None for any of arguments means that the default value will be used.

        :param path: the directory to write.
        :param name: the name of graph, default is 'grpah'
        :param vertex_chunk_size: the chunk size for vertices, default is 2^18
        :param edge_chunk_size: the chunk size for edges, default is 2^22
        :param file_type: the file type for data payload file, support [parquet, orc, csv], default is parquet.
        :param version: version of graphar format, default is v1.
        """
        if vertex_chunk_size is None:
            vertex_chunk_size = (
                GraphArSession._graphar.GeneralParams.defaultVertexChunkSize
            )

        if edge_chunk_size is None:
            edge_chunk_size = GraphArSession._graphar.GeneralParams.defaultEdgeChunkSize

        if file_type is None:
            file_type = GraphArSession._graphar.GeneralParams.defaultFileType
        else:
            file_type = file_type.value

        if version is None:
            version = GraphArSession._graphar.GeneralParams.defaultVersion

        self._jvm_graph_writer_obj.write(
            path,
            GraphArSession._jss,
            name,
            vertex_chunk_size,
            edge_chunk_size,
            file_type,
            version,
        )


class GraphTransformer:
    @staticmethod
    def transform(
        source_graph_info: Union[str, GraphInfo], dest_graph_info: Union[str, GraphInfo]
    ) -> None:
        """Transform the graphs following the meta data provided or defined in info files.

        Note: both arguments should strings or GrapInfo instances! Mixed arguments type is not supported.

        :param source_graph_info: The path of the graph info yaml file for the source graph OR the info object for the source graph.
        :param dest_graph_info: The path of the graph info yaml file for the destination graph OR the info object for the destination graph.
        """
        if isinstance(source_graph_info, str) and isinstance(dest_graph_info, str):
            GraphArSession._graphar.graph.GraphTransformer.transform(
                source_graph_info, dest_graph_info, GraphArSession._jss
            )
        elif isinstance(source_graph_info, GraphInfo) and isinstance(
            dest_graph_info, GraphInfo
        ):
            GraphArSession._graphar.graph.GraphTransformer.transform(
                source_graph_info.to_scala(),
                dest_graph_info.to_scala(),
                GraphArSession._jss,
            )
        else:
            msg = "Both src and dst graph info objects should be of the same type. "
            msg += f"But {type(source_graph_info)} and {type(dest_graph_info)} were provided!"
            raise ValueError(msg)
