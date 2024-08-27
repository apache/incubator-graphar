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

from pathlib import Path

from graphar_pyspark import initialize
from graphar_pyspark.enums import AdjListType
from graphar_pyspark.graph import EdgeTypes, GraphReader
from graphar_pyspark.info import EdgeInfo, GraphInfo, VertexInfo
from graphar_pyspark.reader import EdgeReader, VertexReader

GRAPHAR_TESTS_EXAMPLES = Path(__file__).parent.parent.parent.joinpath("testing")


def test_vertex_reader(spark):
    initialize(spark)

    vertex_info = VertexInfo.load_vertex_info(
        GRAPHAR_TESTS_EXAMPLES.joinpath("modern_graph")
        .joinpath("person.vertex.yml")
        .absolute()
        .__str__()
    )
    vertex_reader = VertexReader.from_python(
        GRAPHAR_TESTS_EXAMPLES.joinpath("modern_graph").absolute().__str__(),
        vertex_info,
    )
    assert VertexReader.from_scala(vertex_reader.to_scala()) is not None
    assert vertex_reader.read_vertices_number() > 0
    assert (
        vertex_reader.read_vertex_property_group(
            vertex_info.get_property_group("name")
        ).count()
        > 0
    )
    assert (
        vertex_reader.read_vertex_property_chunk(
            vertex_info.get_property_groups()[0], 0
        ).count()
        > 0
    )
    assert (
        vertex_reader.read_all_vertex_property_groups().count()
        >= vertex_reader.read_vertex_property_group(
            vertex_info.get_property_group("age")
        ).count()
    )
    assert (
        vertex_reader.read_multiple_vertex_property_groups(
            [vertex_info.get_property_group("name")]
        ).count()
        > 0
    )


def test_edge_reader(spark):
    initialize(spark)

    edge_info = EdgeInfo.load_edge_info(
        GRAPHAR_TESTS_EXAMPLES.joinpath("modern_graph")
        .joinpath("person_knows_person.edge.yml")
        .absolute()
        .__str__(),
    )

    edge_reader = EdgeReader.from_python(
        GRAPHAR_TESTS_EXAMPLES.joinpath("modern_graph").absolute().__str__(),
        edge_info,
        AdjListType.ORDERED_BY_SOURCE,
    )
    assert EdgeReader.from_scala(edge_reader.to_scala()) is not None
    assert (
        "_graphArEdgeIndex"
        in edge_reader.read_edge_property_group(
            edge_info.get_property_group("weight")
        ).columns
    )
    assert (
        edge_reader.read_edge_property_group(
            edge_info.get_property_group("weight")
        ).count()
        > 0
    )
    assert edge_reader.read_vertex_chunk_number() > 0
    assert edge_reader.read_edges_number() > 0
    assert edge_reader.read_edges_number(0) == 0
    assert edge_reader.read_offset(0).count() > 0

def test_vertex_reader_with_json(spark):
    initialize(spark)

    vertex_info = VertexInfo.load_vertex_info(
        GRAPHAR_TESTS_EXAMPLES.joinpath("ldbc_sample/json")
        .joinpath("Person.vertex.yml")
        .absolute()
        .__str__()
    )
    vertex_reader = VertexReader.from_python(
        GRAPHAR_TESTS_EXAMPLES.joinpath("ldbc_sample/json/").absolute().__str__(),
        vertex_info,
    )
    assert VertexReader.from_scala(vertex_reader.to_scala()) is not None
    assert vertex_reader.read_vertices_number() > 0
    assert (
        vertex_reader.read_vertex_property_group(
            vertex_info.get_property_group("firstName")
        ).count()
        > 0
    )
    assert (
        vertex_reader.read_vertex_property_chunk(
            vertex_info.get_property_groups()[0], 0
        ).count()
        > 0
    )
    assert (
        vertex_reader.read_all_vertex_property_groups().count()
        >= vertex_reader.read_vertex_property_group(
            vertex_info.get_property_group("lastName")
        ).count()
    )
    assert (
        vertex_reader.read_multiple_vertex_property_groups(
            [vertex_info.get_property_group("gender")]
        ).count()
        > 0
    )

def test_graph_reader(spark):
    initialize(spark)

    graph_info = GraphReader.read(
        GRAPHAR_TESTS_EXAMPLES.joinpath("modern_graph")
        .joinpath("modern_graph.graph.yml")
        .absolute()
        .__str__()
    )
    assert graph_info is not None
    assert len(graph_info.vertex_dataframes.keys()) > 0
    assert len(graph_info.edge_dataframes.keys()) > 0
    assert "person" in graph_info.vertex_dataframes.keys()
    assert (
        EdgeTypes("person", "created", "software") in graph_info.edge_dataframes.keys()
    )
    assert graph_info.vertex_dataframes["person"].count() > 0
    assert (
        "ordered_by_source"
        in graph_info.edge_dataframes[EdgeTypes("person", "created", "software")]
    )
    assert (
        graph_info.edge_dataframes[EdgeTypes("person", "created", "software")][
            "ordered_by_source"
        ].count()
        > 0
    )

    # test read with graph info
    graph_info_obj = GraphInfo.load_graph_info(
        GRAPHAR_TESTS_EXAMPLES.joinpath("modern_graph")
        .joinpath("modern_graph.graph.yml")
        .absolute()
        .__str__()
    )
    graph_info2 = GraphReader.read(graph_info_obj)
    assert len(graph_info2.vertex_dataframes.keys()) == len(
        graph_info.vertex_dataframes.keys()
    )
