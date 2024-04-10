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
from graphar_pyspark.info import EdgeInfo, GraphInfo, VertexInfo
from graphar_pyspark.reader import EdgeReader, VertexReader
from graphar_pyspark.util import IndexGenerator
from graphar_pyspark.writer import EdgeWriter, VertexWriter
from graphar_pyspark.graph import GraphWriter

GRAPHAR_TESTS_EXAMPLES = Path(__file__).parent.parent.parent.joinpath("testing")


def test_vertex_writer(spark):
    initialize(spark)
    vertex_info = VertexInfo.load_vertex_info(
        GRAPHAR_TESTS_EXAMPLES.joinpath("nebula")
        .joinpath("player.vertex.yml")
        .absolute()
        .__str__()
    )
    vertex_reader = VertexReader.from_python(
        GRAPHAR_TESTS_EXAMPLES.joinpath("nebula").absolute().__str__(),
        vertex_info,
    )

    vertex_df = vertex_reader.read_all_vertex_property_groups()
    vertex_df_with_index = IndexGenerator.generate_vertex_index_column(vertex_df)
    num_vertices = vertex_reader.read_vertices_number()

    vertex_writer = VertexWriter.from_python(
        "/tmp/nebula",
        vertex_info,
        vertex_df_with_index,
        num_vertices,
    )
    vertex_writer.write_vertex_properties()
    vertex_writer.write_vertex_properties(vertex_info.get_property_groups()[0])
    assert Path("/tmp/nebula").exists()
    assert Path("/tmp/nebula/vertex/player/vertex_count").exists()
    assert Path("/tmp/nebula/vertex/player/_vertexId_name_age/chunk0").exists()

    assert VertexWriter.from_scala(vertex_writer.to_scala()) is not None


def test_edge_writer(spark):
    initialize(spark)
    edge_info = EdgeInfo.load_edge_info(
        GRAPHAR_TESTS_EXAMPLES.joinpath("nebula")
        .joinpath("player_follow_player.edge.yml")
        .absolute()
        .__str__()
    )

    edge_reader = EdgeReader.from_python(
        GRAPHAR_TESTS_EXAMPLES.joinpath("nebula").absolute().__str__(),
        edge_info,
        AdjListType.ORDERED_BY_SOURCE,
    )
    edge_df = edge_reader.read_edges()
    edge_num = edge_reader.read_vertices_number()

    edge_writer = EdgeWriter.from_python(
        "/tmp/nebula",
        edge_info,
        AdjListType.ORDERED_BY_SOURCE,
        edge_num,
        edge_df,
    )
    edge_writer.write_edge_properties()
    assert Path("/tmp/nebula").exists()
    assert Path("/tmp/nebula/edge").exists()

    assert EdgeWriter.from_scala(edge_writer.to_scala()) is not None

    edge_writer.write_edges()
    edge_writer.write_edge_properties(
        edge_info.get_property_group("degree"),
    )
    edge_writer.write_edge_properties()
    edge_writer.write_adj_list()


def test_graph_writer(spark):
    initialize(spark)
    graph_writer = GraphWriter.from_python()

    assert GraphWriter.from_scala(graph_writer.to_scala()) is not None
    vertex_file_path = GRAPHAR_TESTS_EXAMPLES.joinpath("ldbc_sample/person_0_0.csv").absolute().__str__()
    vertex_df = spark.read.option("delimiter", "|").option("header", "true").csv(vertex_file_path)
    label = "person"
    graph_writer.put_vertex_data(label, vertex_df, "id")

    edge_file_path = GRAPHAR_TESTS_EXAMPLES.joinpath("ldbc_sample/person_knows_person_0_0.csv").absolute().__str__()
    edge_df = spark.read.option("delimiter", "|").option("header", "true").csv(edge_file_path)
    tag = ("person", "knows", "person")

    graph_info = GraphInfo.from_python("ldbc", "/tmp/ldbc", ["person.vertex.yml"], ["person_knows_person.yml"], "gar/v1")

    graph_writer.put_edge_data(tag, edge_df)
    graph_writer.write_with_graph_info(graph_info)
    graph_writer.write("/tmp/ldbc", "ldbc")

    assert Path("/tmp/ldbc").exists()
