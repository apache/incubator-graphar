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

# Copyright 2022-2023 Alibaba Group Holding Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Bidnings to com.alibaba.graphar.graph."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Optional, Union

from py4j.java_gateway import JavaObject
from pyspark.sql import DataFrame

from graphar_pyspark import GraphArSession, _check_session
from graphar_pyspark.enums import FileType
from graphar_pyspark.errors import InvalidGraphFormatError
from graphar_pyspark.info import GraphInfo


@dataclass(frozen=True)
class EdgeLabels:
    """A triplet that describe edge. Contains source, edge and dest labels. Immutable."""

    src_label: str
    edge_label: str
    dst_label: str


@dataclass(frozen=True)
class GraphReaderResult:
    """A simple immutable class, that represent results of reading a graph with GraphReader."""

    vertex_dataframes: Mapping[str, DataFrame]
    edge_dataframes: Mapping[EdgeLabels, Mapping[str, DataFrame]]

    @staticmethod
    def from_scala(
        jvm_result: tuple[
            dict[str, JavaObject],
            dict[tuple[str, str, str], dict[str, JavaObject]],
        ],
    ) -> "GraphReaderResult":
        """Create an instance of the Class from JVM method output.

        :param jvm_result: structure, returned from JVM.
        :returns: instance of Python Class.
        """
        first_dict = {}
        first_scala_map = jvm_result._1()
        first_scala_map_iter = first_scala_map.keySet().iterator()

        while first_scala_map_iter.hasNext():
            k = first_scala_map_iter.next()
            first_dict[k] = DataFrame(first_scala_map.get(k).get(), GraphArSession.ss)

        second_dict = {}
        second_scala_map = jvm_result._2()
        second_scala_map_iter = second_scala_map.keySet().iterator()

        while second_scala_map_iter.hasNext():
            k = second_scala_map_iter.next()
            nested_scala_map = second_scala_map.get(k).get()
            nested_scala_map_iter = nested_scala_map.keySet().iterator()
            inner_dict = {}

            while nested_scala_map_iter.hasNext():
                kk = nested_scala_map_iter.next()
                inner_dict[kk] = DataFrame(
                    nested_scala_map.get(kk).get(),
                    GraphArSession.ss,
                )

            second_dict[EdgeLabels(k._1(), k._2(), k._3())] = inner_dict

        return GraphReaderResult(
            vertex_dataframes=first_dict,
            edge_dataframes=second_dict,
        )


class GraphReader:
    """The helper object for reading graph through the definitions of graph info."""

    @staticmethod
    def read(
        graph_info: Union[GraphInfo, str],
    ) -> GraphReaderResult:
        """Read the graph as vertex and edge DataFrames with the graph info yaml file or GraphInfo object.

        :param graph_info: The path of the graph info yaml or GraphInfo instance.
        :returns: GraphReaderResults, that contains vertex and edge dataframes.
        """
        _check_session()
        if isinstance(graph_info, str):
            graph_info = GraphInfo.load_graph_info(graph_info)

        jvm_result = GraphArSession.graphar.graph.GraphReader.readWithGraphInfo(
            graph_info.to_scala(),
            GraphArSession.jss,
        )
        return GraphReaderResult.from_scala(jvm_result)


class GraphWriter:
    """The helper class for writing graph."""

    def __init__(self, jvm_obj: JavaObject) -> None:
        """One should not use this constructor directly, please use `from_scala` or `from_python`."""
        _check_session()
        self._jvm_graph_writer_obj = jvm_obj

    def to_scala(self) -> JavaObject:
        """Transform object to JVM representation.

        :returns: JavaObject
        """
        return self._jvm_graph_writer_obj

    @staticmethod
    def from_scala(jvm_obj: JavaObject) -> "GraphWriter":
        """Create an instance of the Class from the corresponding JVM object.

        :param jvm_obj: scala object in JVM.
        :returns: instance of Python Class.
        """
        return GraphWriter(jvm_obj)

    @staticmethod
    def from_python() -> "GraphWriter":
        """Create an instance of the Class from Python arguments."""
        return GraphWriter(GraphArSession.graphar.graph.GraphWriter())

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
        relation_jvm = GraphArSession.jvm.scala.Tuple3(
            relation[0], relation[1], relation[2],
        )
        self._jvm_graph_writer_obj.PutEdgeData(relation_jvm, df._jdf)

    def write_with_graph_info(self, graph_info: Union[GraphInfo, str]) -> None:
        """Write the graph data in graphar format with graph info.

        Note: original method is `write` but there is not directly overloading in Python.

        :param graph_info: the graph info object for the graph or the path to graph info object.
        """
        if isinstance(graph_info, str):
            self._jvm_graph_writer_obj.write(graph_info, GraphArSession.jss)
        else:
            self._jvm_graph_writer_obj.write(graph_info.to_scala(), GraphArSession.jss)

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
                GraphArSession.graphar.GeneralParams.defaultVertexChunkSize
            )

        if edge_chunk_size is None:
            edge_chunk_size = GraphArSession.graphar.GeneralParams.defaultEdgeChunkSize

        file_type = (
            GraphArSession.graphar.GeneralParams.defaultFileType
            if file_type is None
            else file_type.value
        )

        if version is None:
            version = GraphArSession.graphar.GeneralParams.defaultVersion

        self._jvm_graph_writer_obj.write(
            path,
            GraphArSession.jss,
            name,
            vertex_chunk_size,
            edge_chunk_size,
            file_type,
            version,
        )


class GraphTransformer:
    """The helper object for transforming graphs through the definitions of their infos."""

    @staticmethod
    def transform(
        source_graph_info: Union[str, GraphInfo],
        dest_graph_info: Union[str, GraphInfo],
    ) -> None:
        """Transform the graphs following the meta data provided or defined in info files.

        Note: both arguments should be strings or GrapInfo instances! Mixed arguments type is not supported.

        :param source_graph_info: The path of the graph info yaml file for the source graph OR the info object for the source graph.
        :param dest_graph_info: The path of the graph info yaml file for the destination graph OR the info object for the destination graph.
        :raise InvalidGraphFormatException: if you pass mixed format of source and dest graph info.
        """
        _check_session()
        if isinstance(source_graph_info, str) and isinstance(dest_graph_info, str):
            GraphArSession.graphar.graph.GraphTransformer.transform(
                source_graph_info,
                dest_graph_info,
                GraphArSession.jss,
            )
        elif isinstance(source_graph_info, GraphInfo) and isinstance(
            dest_graph_info,
            GraphInfo,
        ):
            GraphArSession.graphar.graph.GraphTransformer.transform(
                source_graph_info.to_scala(),
                dest_graph_info.to_scala(),
                GraphArSession.jss,
            )
        else:
            msg = "Both src and dst graph info objects should be of the same type. "
            msg += f"But {type(source_graph_info)} and {type(dest_graph_info)} were provided!"
            raise InvalidGraphFormatError(msg)
