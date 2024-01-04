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

"""Bindings to com.alibaba.graphar.writer."""


from __future__ import annotations

import os
from typing import Optional

from py4j.java_gateway import JavaObject
from pyspark.sql import DataFrame

from graphar_pyspark import GraphArSession, _check_session
from graphar_pyspark.enums import AdjListType
from graphar_pyspark.info import EdgeInfo, PropertyGroup, VertexInfo


class VertexWriter:
    """Writer for vertex DataFrame."""

    def __init__(
        self,
        prefix: Optional[str],
        vertex_info: Optional[VertexInfo],
        vertex_df: Optional[DataFrame],
        num_vertices: Optional[int],
        jvm_obj: Optional[JavaObject],
    ) -> None:
        """One should not use this constructor directly, please use `from_scala` or `from_python`."""
        _check_session()
        if jvm_obj is not None:
            self._jvm_vertex_writer_obj = jvm_obj
        else:
            num_vertices = -1 if num_vertices is None else num_vertices
            self._jvm_vertex_writer_obj = GraphArSession.graphar.writer.VertexWriter(
                prefix,
                vertex_info.to_scala(),
                vertex_df._jdf,
                num_vertices,
            )

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
        :param num_vertices: the number of vertices, optional
        """
        if not prefix.endswith(os.sep):
            prefix += os.sep
        return VertexWriter(prefix, vertex_info, vertex_df, num_vertices, None)

    def write_vertex_properties(
        self,
        property_group: Optional[PropertyGroup] = None,
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
        _check_session()
        if jvm_obj is not None:
            self._jvm_edge_writer_obj = jvm_obj
        else:
            self._jvm_edge_writer_obj = GraphArSession.graphar.writer.EdgeWriter(
                prefix,
                edge_info.to_scala(),
                adj_list_type.to_scala(),
                vertex_num,
                edge_df._jdf,
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
        if not prefix.endswith(os.sep):
            prefix += os.sep
        return EdgeWriter(prefix, edge_info, adj_list_type, vertex_num, edge_df, None)

    def write_adj_list(self) -> None:
        """Generate the chunks of AdjList from edge DataFrame for this edge type."""
        self._jvm_edge_writer_obj.writeAdjList()

    def write_edge_properties(
        self,
        property_group: Optional[PropertyGroup] = None,
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
        """Generate the chunks for the AdjList and all property groups from edge."""
        self._jvm_edge_writer_obj.writeEdges()
