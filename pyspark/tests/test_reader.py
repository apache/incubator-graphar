"""
copyright 2022-2023 alibaba group holding limited.

licensed under the apache license, version 2.0 (the "license");
you may not use this file except in compliance with the license.
you may obtain a copy of the license at

    http://www.apache.org/licenses/license-2.0

unless required by applicable law or agreed to in writing, software
distributed under the license is distributed on an "as is" basis,
without warranties or conditions of any kind, either express or implied.
see the license for the specific language governing permissions and
limitations under the license.
"""

from pathlib import Path

from graphar_pyspark import initialize
from graphar_pyspark.enums import AdjListType
from graphar_pyspark.graph import EdgeLabels, GraphReader
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
    assert vertex_reader.read_vertices_number() > 0
    assert (
        vertex_reader.read_vertex_property_group(
            vertex_info.get_property_group("name")
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
    assert (
        "_graphArEdgeIndex"
        in edge_reader.read_edge_property_group(
            edge_info.get_property_group("weight", AdjListType.ORDERED_BY_SOURCE)
        ).columns
    )
    assert (
        edge_reader.read_edge_property_group(
            edge_info.get_property_group("weight", AdjListType.ORDERED_BY_SOURCE)
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
        EdgeLabels("person", "created", "software") in graph_info.edge_dataframes.keys()
    )
    assert graph_info.vertex_dataframes["person"].count() > 0
    assert (
        "ordered_by_source"
        in graph_info.edge_dataframes[EdgeLabels("person", "created", "software")]
    )
    assert (
        graph_info.edge_dataframes[EdgeLabels("person", "created", "software")][
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
