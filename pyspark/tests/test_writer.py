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
from graphar_pyspark.info import EdgeInfo, VertexInfo
from graphar_pyspark.reader import EdgeReader, VertexReader
from graphar_pyspark.util import IndexGenerator
from graphar_pyspark.writer import EdgeWriter, VertexWriter

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

    vertex_writer = VertexWriter.from_python(
        "/tmp/nebula",
        vertex_info,
        vertex_df_with_index,
    )
    vertex_writer.write_vertex_properties()
    assert Path("/tmp/nebula").exists()
