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

"""Bidnings to com.alibaba.graphar.graph."""

from __future__ import annotations

import os
from typing import Optional

from py4j.java_gateway import JavaObject
from pyspark.sql import DataFrame

from graphar_pyspark import GraphArSession, _check_session
from graphar_pyspark.enums import AdjListType
from graphar_pyspark.info import EdgeInfo, PropertyGroup, VertexInfo


class VertexReader:
    """Reader for vertex chunks."""

    def __init__(
        self,
        prefix: Optional[str],
        vertex_info: Optional[VertexInfo],
        jvm_obj: Optional[JavaObject],
    ) -> None:
        """One should not use this constructor directly, please use `from_scala` or `from_python`."""
        _check_session()
        if jvm_obj is not None:
            self._jvm_vertex_reader_obj = jvm_obj
        else:
            self._jvm_vertex_reader_obj = GraphArSession.graphar.reader.VertexReader(
                prefix,
                vertex_info.to_scala(),
                GraphArSession.jss,
            )

    def to_scala(self) -> JavaObject:
        """Transform object to JVM representation.

        :returns: JavaObject
        """
        return self._jvm_vertex_reader_obj

    @staticmethod
    def from_scala(jvm_obj: JavaObject) -> "VertexReader":
        """Create an instance of the Class from the corresponding JVM object.

        :param jvm_obj: scala object in JVM.
        :returns: instance of Python Class.
        """
        return VertexReader(None, None, jvm_obj)

    @staticmethod
    def from_python(prefix: str, vertex_info: VertexInfo) -> "VertexReader":
        """Create an instance of the Class from Python arguments.

        :param prefix: the absolute prefix.
        :param vertex_info: the vertex info that describes the vertex type.
        """
        if not prefix.endswith(os.sep):
            prefix += os.sep
        return VertexReader(prefix, vertex_info, None)

    def read_vertices_number(self) -> int:
        """Load the total number of vertices for this vertex type.

        :returns: total number of vertices.
        """
        return self._jvm_vertex_reader_obj.readVerticesNumber()

    def read_vertex_property_chunk(
        self,
        property_group: PropertyGroup,
        chunk_index: int,
    ) -> DataFrame:
        """Load a single vertex property chunk as a DataFrame.

        WARNING! Exceptions from the JVM are not checked inside, it is just a proxy-method!

        :param property_group: property group.
        :param chunk_index: index of vertex chunk.
        :returns: vertex property chunk DataFrame. Raise IllegalArgumentException if the property group not contained.
        """
        return DataFrame(
            self._jvm_vertex_reader_obj.readVertexPropertyChunk(
                property_group.to_scala(),
                chunk_index,
            ),
            GraphArSession.ss,
        )

    def read_vertex_property_group(self, property_group: PropertyGroup) -> DataFrame:
        """Load all chunks for a property group as a DataFrame.

        WARNING! Exceptions from the JVM are not checked inside, it is just a proxy-method!

        :param property_group: property group.
        :returns: DataFrame that contains all chunks of property group. Raise IllegalArgumentException if the property group not contained.
        """
        return DataFrame(
            self._jvm_vertex_reader_obj.readVertexPropertyGroup(
                property_group.to_scala(),
            ),
            GraphArSession.ss,
        )

    def read_multiple_vertex_property_groups(
        self,
        property_groups: list[PropertyGroup],
    ) -> DataFrame:
        """Load the chunks for multiple property groups as a DataFrame.

        WARNING! Exceptions from the JVM are not checked inside, it is just a proxy-method!

        :param property_groups: list of property groups.
        :returns: DataFrame that contains all chunks of property group. Raise IllegalArgumentException if the property group not contained.
        """
        return DataFrame(
            self._jvm_vertex_reader_obj.readMultipleVertexPropertyGroups(
                [py_property_group.to_scala() for py_property_group in property_groups],
            ),
            GraphArSession.ss,
        )

    def read_all_vertex_property_groups(self) -> DataFrame:
        """Load the chunks for all property groups as a DataFrame.

        :returns: DataFrame that contains all property group chunks of vertex.
        """
        return DataFrame(
            self._jvm_vertex_reader_obj.readAllVertexPropertyGroups(),
            GraphArSession.ss,
        )


class EdgeReader:
    """Reader for edge chunks."""

    def __init__(
        self,
        prefix: Optional[str],
        edge_info: Optional[EdgeInfo],
        adj_list_type: Optional[AdjListType],
        jvm_obj: Optional[JavaObject],
    ) -> None:
        """One should not use this constructor directly, please use `from_scala` or `from_python`."""
        _check_session()
        if jvm_obj is not None:
            self._jvm_edge_reader_obj = jvm_obj
        else:
            self._jvm_edge_reader_obj = GraphArSession.graphar.reader.EdgeReader(
                prefix,
                edge_info.to_scala(),
                adj_list_type.to_scala(),
                GraphArSession.jss,
            )

    def to_scala(self) -> JavaObject:
        """Transform object to JVM representation.

        :returns: JavaObject
        """
        return self._jvm_edge_reader_obj

    @staticmethod
    def from_scala(jvm_obj: JavaObject) -> "EdgeReader":
        """Create an instance of the Class from the corresponding JVM object.

        :param jvm_obj: scala object in JVM.
        :returns: instance of Python Class.
        """
        return EdgeReader(None, None, None, jvm_obj)

    @staticmethod
    def from_python(
        prefix: str,
        edge_info: EdgeInfo,
        adj_list_type: AdjListType,
    ) -> "EdgeReader":
        """Create an instance of the Class from Python arguments.

        Note that constructor would raise IllegalArgumentException if edge info does not support given adjListType.

        :param prefix: the absolute prefix.
        :param edge_info: the edge info that describes the edge type.
        :param adj_list_type: the adj list type for the edge.
        """
        if not prefix.endswith(os.sep):
            prefix += os.sep
        return EdgeReader(prefix, edge_info, adj_list_type, None)

    def read_vertices_number(self) -> int:
        """Load the total number of src/dst vertices for this edge type.

        :returns: total number of vertices.
        """
        return self._jvm_edge_reader_obj.readVerticesNumber()

    def read_vertex_chunk_number(self) -> int:
        """Load the chunk number of src/dst vertices.

        :returns: chunk number of vertices.
        """
        return self._jvm_edge_reader_obj.readVertexChunkNumber()

    def read_edges_number(self, chunk_index: Optional[int] = None) -> int:
        """Load the number of edges for the vertex chunk or for this edge type.

        :param chunk_index: index of vertex chunk (optional, default is None)
        if not provided, returns the number of edges for this edge type
        if provided, returns the number of edges for the vertex chunk
        :returns: the number of edges
        """
        if chunk_index is None:
            return self._jvm_edge_reader_obj.readEdgesNumber()
        return self._jvm_edge_reader_obj.readEdgesNumber(chunk_index)

    def read_offset(self, chunk_index: int) -> DataFrame:
        """Load a single offset chunk as a DataFrame.

        WARNING! Exceptions from the JVM are not checked inside, it is just a proxy-method!

        :param chunk_index: index of offset chunk
        :returns: offset chunk DataFrame. Raise IllegalArgumentException if adjListType is
        not AdjListType.ordered_by_source or AdjListType.ordered_by_dest.
        """
        return DataFrame(
            self._jvm_edge_reader_obj.readOffset(chunk_index),
            GraphArSession.ss,
        )

    def read_adj_list_chunk(
        self,
        vertex_chunk_index: int,
        chunk_index: int,
    ) -> DataFrame:
        """Load a single AdjList chunk as a DataFrame.

        :param vertex_chunk_index: index of vertex chunk
        :param chunk_index: index of AdjList chunk.
        :returns: AdjList chunk DataFrame
        """
        return DataFrame(
            self._jvm_edge_reader_obj.readAdjListChunk(vertex_chunk_index, chunk_index),
            GraphArSession.ss,
        )

    def read_adj_list_for_vertex_chunk(
        self,
        vertex_chunk_index: int,
        add_index: bool = True,
    ) -> DataFrame:
        """Load all AdjList chunks for a vertex chunk as a DataFrame.

        :param vertex_chunk_index: index of vertex chunk.
        :param add_index: flag that add edge index column or not in the final DataFrame.
        :returns: DataFrame of all AdjList chunks of vertices in given vertex chunk.
        """
        return DataFrame(
            self._jvm_edge_reader_obj.readAdjListForVertexChunk(
                vertex_chunk_index,
                add_index,
            ),
            GraphArSession.ss,
        )

    def read_all_adj_list(self, add_index: bool = True) -> DataFrame:
        """Load all AdjList chunks for this edge type as a DataFrame.

        :param add_index: flag that add index column or not in the final DataFrame.
        :returns: DataFrame of all AdjList chunks.
        """
        return DataFrame(
            self._jvm_edge_reader_obj.readAllAdjList(add_index),
            GraphArSession.ss,
        )

    def read_edge_property_chunk(
        self,
        property_group: PropertyGroup,
        vertex_chunk_index: int,
        chunk_index: int,
    ) -> DataFrame:
        """Load a single edge property chunk as a DataFrame.

        WARNING! Exceptions from the JVM are not checked inside, it is just a proxy-method!

        :param property_group: property group.
        :param vertex_chunk_index: index of vertex chunk.
        :param chunk_index: index of property group chunk.
        :returns: property group chunk DataFrame. If edge info does not contain the
        property group, raise an IllegalArgumentException error.
        """
        return DataFrame(
            self._jvm_edge_reader_obj.readEdgePropertyChunk(
                property_group.to_scala(),
                vertex_chunk_index,
                chunk_index,
            ),
        )

    def read_edge_property_group_for_vertex_chunk(
        self,
        property_group: PropertyGroup,
        vertex_chunk_index: int,
        add_index: bool = True,
    ) -> DataFrame:
        """Load the chunks for a property group of a vertex chunk as a DataFrame.

        WARNING! Exceptions from the JVM are not checked inside, it is just a proxy-method!

        :param property_group: property group.
        :param vertex_chunk_index: index of vertex chunk.
        :param add_index: flag that add edge index column or not in the final DataFrame.
        :returns: DataFrame that contains all property group chunks of vertices in given
        vertex chunk. If edge info does not contain the property group, raise an IllegalArgumentException error.
        """
        return DataFrame(
            self._jvm_edge_reader_obj.readEdgePropertyGroupForVertexChunk(
                property_group.to_scala(),
                vertex_chunk_index,
                add_index,
            ),
            GraphArSession.ss,
        )

    def read_edge_property_group(
        self,
        property_group: PropertyGroup,
        add_index: bool = True,
    ) -> DataFrame:
        """Load all chunks for a property group as a DataFrame.

        WARNING! Exceptions from the JVM are not checked inside, it is just a proxy-method!

        :param property_group: property group.
        :param add_index: flag that add edge index column or not in the final DataFrame.
        :returns: DataFrame that contains all chunks of property group. If edge info does
        not contain the property group, raise an IllegalArgumentException error.
        """
        return DataFrame(
            self._jvm_edge_reader_obj.readEdgePropertyGroup(
                property_group.to_scala(),
                add_index,
            ),
            GraphArSession.ss,
        )

    def read_multiple_edge_property_groups_for_vertex_chunk(
        self,
        property_groups: list[PropertyGroup],
        vertex_chunk_index: int,
        add_index: bool = True,
    ) -> DataFrame:
        """Load the chunks for multiple property groups of a vertex chunk as a DataFrame.

        :param property_groups: list of property groups.
        :param vertex_chunk_index: index of vertex chunk.
        :param add_index: flag that add edge index column or not in the final DataFrame.
        :returns: DataFrame that contains all property groups chunks of a vertex chunk.
        """
        return DataFrame(
            self._jvm_edge_reader_obj.readMultipleEdgePropertyGroupsForVertexChunk(
                [py_property_group.to_scala() for py_property_group in property_groups],
                vertex_chunk_index,
                add_index,
            ),
            GraphArSession.ss,
        )

    def read_multiple_edge_property_groups(
        self,
        property_groups: list[PropertyGroup],
        add_index: bool = True,
    ) -> DataFrame:
        """Load the chunks for multiple property groups as a DataFrame.

        :param property_groups: list of property groups.
        :param add_index: flag that add edge index column or not in the final DataFrame.
        :returns: DataFrame tha contains all property groups chunks of edge.
        """
        return DataFrame(
            self._jvm_edge_reader_obj.readMultipleEdgePropertyGroups(
                [py_property_group.to_scala() for py_property_group in property_groups],
                add_index,
            ),
            GraphArSession.ss,
        )

    def read_all_edge_property_groups_for_vertex_chunk(
        self,
        vertex_chunk_index: int,
        add_index: bool = True,
    ) -> DataFrame:
        """Load the chunks for all property groups of a vertex chunk as a DataFrame.

        :param vertex_chunk_index: index of vertex chunk.
        :param add_index: flag that add edge index column or not in the final DataFrame.
        :returns: DataFrame that contains all property groups chunks of a vertex chunk.
        """
        return DataFrame(
            self._jvm_edge_reader_obj.readAllEdgePropertyGroupsForVertexChunk(
                vertex_chunk_index,
                add_index,
            ),
            GraphArSession.ss,
        )

    def read_all_edge_property_groups(self, add_index: bool = True) -> DataFrame:
        """Load the chunks for all property groups as a DataFrame.

        :param add_index: flag that add edge index column or not in the final DataFrame.
        :returns: DataFrame tha contains all property groups chunks of edge.
        """
        return DataFrame(
            self._jvm_edge_reader_obj.readAllEdgePropertyGroups(add_index),
            GraphArSession.ss,
        )

    def read_edges_for_vertex_chunk(
        self,
        vertex_chunk_index: int,
        add_index: bool = True,
    ) -> DataFrame:
        """Load the chunks for the AdjList and all property groups for a vertex chunk as a DataFrame.

        :param vertex_chunk_index: index of vertex chunk
        :param add_index: flag that add edge index column or not in the final DataFrame.
        :returns: DataFrame that contains all chunks of AdjList and property groups of vertices in given vertex chunk.
        """
        return DataFrame(
            self._jvm_edge_reader_obj.readEdgesForVertexChunk(
                vertex_chunk_index,
                add_index,
            ),
            GraphArSession.ss,
        )

    def read_edges(self, add_index: bool = True) -> DataFrame:
        """Load the chunks for the AdjList and all property groups as a DataFrame.

        :param add_index: flag that add edge index column or not in the final DataFrame.
        :returns: DataFrame that contains all chunks of AdjList and property groups of edges.
        """
        return DataFrame(
            self._jvm_edge_reader_obj.readEdges(add_index),
            GraphArSession.ss,
        )
