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

from typing import Optional

from py4j.java_gateway import JavaObject
from pyspark.sql import DataFrame

from graphar_pysaprk import GraphArSession
from graphar_pysaprk.enums import AdjListType
from graphar_pysaprk.info import EdgeInfo, PropertyGroup, VertexInfo


class VertexWriter:
    def __init__(
        self,
        prefix: Optional[str],
        vertex_info: Optional[VertexInfo],
        vertex_df: Optional[DataFrame],
        num_vertices: Optional[int],
        jvm_obj: Optional[JavaObject],
    ) -> None:
        """One should not use this constructor directly, please use `from_scala` or `from_python`."""
        if jvm_obj is not None:
            self._jvm_vertex_writer_obj = jvm_obj
        else:
            if num_vertices is not None:
                vertex_writer = GraphArSession._graphar.writer.VertexWriter(
                    prefix,
                    vertex_info.to_scala(),
                    vertex_df._jdf,
                    num_vertices,
                )
            else:
                vertex_writer = GraphArSession._graphar.writer.VertexWriter(
                    prefix,
                    vertex_info.to_scala(),
                    vertex_df._jdf,
                )

            self._jvm_vertex_writer_obj = vertex_writer

    def get_prefix(self) -> str:
        return self._jvm_vertex_writer_obj.prefix()

    def get_vertex_info(self) -> VertexInfo:
        return VertexInfo.from_scala(self._jvm_vertex_writer_obj.vertexInfo())

    def get_vertex_df(self) -> DataFrame:
        return DataFrame(
            self._jvm_vertex_writer_obj.vertexDf(),
            GraphArSession._ss,
        )

    def get_num_vertices(self) -> int:
        return self._jvm_vertex_writer_obj.numVertices()

    def to_scala(self) -> JavaObject:
        """Transform object to JVM representation.

        :returns: JavaObject
        """
        return self._jvm_vertex_writer_obj

    @staticmethod
    def from_scala(jvm_obj: JavaObject) -> "VertexWriter":
        """Create an instance of the Class from the corresponding JVM object.

        :param jvm_obj: scala object in JVM.
        :returns: instance of Python Class.
        """
        return VertexWriter(None, None, None, None, jvm_obj)

    @staticmethod
    def from_python(
        prefix: str,
        vertex_info: VertexInfo,
        vertex_df: DataFrame,
        num_vertices: Optional[int],
    ) -> "VertexWriter":
        """Create an instance of the Class from Python arguments.

        :param prefix: the absolute prefix.
        :param vertex_info: the vertex info that describes the vertex type.
        :param vertex_df: the input vertex DataFrame.
        :param num_vertices: optional
        """
        return VertexWriter(prefix, vertex_info, vertex_df, num_vertices)

    def write_vertex_properties(
        self, property_group: Optional[PropertyGroup] = None
    ) -> None:
        """Generate chunks of the property group (or all property groups) for vertex DataFrame.

        :param property_group: property group (optional, default is None)
        if provided, generate chunks of the property group, otherwise generate for all property groups.
        """
        if property_group is not None:
            self._jvm_vertex_writer_obj.writeVertexProperties(property_group.to_scala())
        else:
            self._jvm_vertex_writer_obj.writeVertexProperties()


class EdgeWriter:
    """Writer for edge DataFrame."""

    def __init__(
        self,
        prefix: Optional[str],
        edge_info: Optional[EdgeInfo],
        adj_list_type: Optional[AdjListType],
        vertex_num: Optional[int],
        edge_df: Optional[DataFrame],
        jvm_obj: Optional[JavaObject],
    ) -> None:
        """One should not use this constructor directly, please use `from_scala` or `from_python`."""
        if jvm_obj is not None:
            self._jvm_edge_writer_obj = jvm_obj
        else:
            self._jvm_edge_writer_obj = GraphArSession._graphar.writer.EdgeWriter(
                prefix,
                edge_info.to_scala(),
                adj_list_type.to_scala(),
                vertex_num,
                edge_df._jdf,
            )

    def get_prefix(self) -> str:
        return self._jvm_edge_writer_obj.prefix()

    def get_edge_info(self) -> EdgeInfo:
        return EdgeInfo.from_scala(self._jvm_edge_writer_obj.edgeInfo())

    def get_adj_list_type(self) -> AdjListType:
        return AdjListType.from_scala(self._jvm_edge_writer_obj.adjListType())

    def get_vertex_num(self) -> int:
        return self._jvm_edge_writer_obj.vertexNum()

    def get_edge_df(self) -> DataFrame:
        return DataFrame(
            self._jvm_edge_writer_obj.edgeDf(),
            GraphArSession._ss,
        )

    def to_scala(self) -> JavaObject:
        """Transform object to JVM representation.

        :returns: JavaObject
        """
        return self._jvm_edge_writer_obj

    @staticmethod
    def from_scala(jvm_obj: JavaObject) -> "EdgeWriter":
        """Create an instance of the Class from the corresponding JVM object.

        :param jvm_obj: scala object in JVM.
        :returns: instance of Python Class.
        """
        return EdgeWriter(None, None, None, None, None, jvm_obj)

    @staticmethod
    def from_python(
        prefix: str,
        edge_info: EdgeInfo,
        adj_list_type: AdjListType,
        vertex_num: int,
        edge_df: DataFrame,
    ) -> "EdgeWriter":
        """Create an instance of the Class from Python arguments.

        :param prefix: the absolute prefix.
        :param edge_info: the edge info that describes the ede type.
        :param adj_list_type: the adj list type for the edge.
        :param vertex_num: vertex number of the primary vertex label
        :param edge_df: the input edge DataFrame.
        """
        return EdgeWriter(prefix, edge_info, adj_list_type, vertex_num, edge_df, None)

    def write_adj_list(self) -> None:
        """Generate the chunks of AdjList from edge DataFrame for this edge type."""
        self._jvm_edge_writer_obj.writeAdjList()

    def write_edge_properties(
        self, property_group: Optional[PropertyGroup] = None
    ) -> None:
        """Generate the chunks of all or selected property groups from edge DataFrame.

        :param property_group: property group (optional, default is None)
        if provided, generate the chunks of selected property group, otherwise generate for all groups.
        """
        if property_group is not None:
            self._jvm_edge_writer_obj.writeEdgeProperties(property_group.to_scala())
        else:
            self._jvm_edge_writer_obj.writeEdgeProperties()

    def write_edges(self) -> None:
        """Generate the chunks for the AdjList and all property groups from edge"""
        self._jvm_edge_writer_obj.writeEdges()
