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

from pathlib import Path

import pytest
import yaml
from graphar_pyspark import initialize
from graphar_pyspark.enums import AdjListType, FileType, GarType
from graphar_pyspark.info import (
    AdjList,
    EdgeInfo,
    GraphInfo,
    Property,
    PropertyGroup,
    VertexInfo,
)
from pyspark.sql.utils import IllegalArgumentException

GRAPHAR_TESTS_EXAMPLES = Path(__file__).parent.parent.parent.joinpath("testing")


def test_property(spark):
    initialize(spark)
    property_from_py = Property.from_python("name", GarType.BOOL, False)

    assert property_from_py == Property.from_scala(property_from_py.to_scala())
    assert property_from_py != 0
    assert property_from_py == Property.from_python("name", GarType.BOOL, False)

    property_from_py.set_name("new_name")
    property_from_py.set_data_type(GarType.INT32)
    property_from_py.set_is_primary(True)

    assert property_from_py.get_name() == "new_name"
    assert property_from_py.get_data_type() == GarType.INT32
    assert property_from_py.get_is_primary() == True


def test_property_group(spark):
    initialize(spark)
    p_group_from_py = PropertyGroup.from_python(
        "prefix",
        FileType.CSV,
        [
            Property.from_python("non_primary", GarType.DOUBLE, False),
            Property.from_python("primary", GarType.INT64, True),
        ],
    )

    assert p_group_from_py == PropertyGroup.from_scala(p_group_from_py.to_scala())
    assert p_group_from_py != 0

    p_group_from_py.set_prefix("new_prefix")
    p_group_from_py.set_file_type(FileType.ORC)
    p_group_from_py.set_properties(
        p_group_from_py.get_properties()
        + [Property("another_one", GarType.LIST, False)]
    )

    assert p_group_from_py.get_prefix() == "new_prefix"
    assert p_group_from_py.get_file_type() == FileType.ORC
    assert all(
        p_left == p_right
        for p_left, p_right in zip(
            p_group_from_py.get_properties(),
            [
                Property.from_python("non_primary", GarType.DOUBLE, False),
                Property.from_python("primary", GarType.INT64, True),
                Property("another_one", GarType.LIST, False),
            ],
        )
    )


def test_adj_list(spark):
    initialize(spark)

    props_list_1 = [
        Property.from_python("non_primary", GarType.DOUBLE, False),
        Property.from_python("primary", GarType.INT64, True),
    ]

    props_list_2 = [
        Property.from_python("non_primary", GarType.DOUBLE, False),
        Property.from_python("primary", GarType.INT64, True),
        Property("another_one", GarType.LIST, False),
    ]

    adj_list_from_py = AdjList.from_python(
        True,
        "dest",
        "prefix",
        FileType.PARQUET,
        [
            PropertyGroup.from_python("prefix1", FileType.PARQUET, props_list_1),
            PropertyGroup.from_python("prefix2", FileType.ORC, props_list_2),
        ],
    )

    assert adj_list_from_py == AdjList.from_scala(adj_list_from_py.to_scala())
    assert adj_list_from_py != 0
    assert adj_list_from_py.get_adj_list_type() == AdjListType.ORDERED_BY_DEST

    adj_list_from_py.set_aligned_by("src")
    assert adj_list_from_py.get_adj_list_type() == AdjListType.ORDERED_BY_SOURCE
    adj_list_from_py.set_ordered(False)
    assert adj_list_from_py.get_adj_list_type() == AdjListType.UNORDERED_BY_SOURCE
    adj_list_from_py.set_aligned_by("dest")
    assert adj_list_from_py.get_adj_list_type() == AdjListType.UNORDERED_BY_DEST
    adj_list_from_py.set_prefix("prefix_new")
    assert adj_list_from_py.get_prefix() == "prefix_new"

    adj_list_from_py.set_file_type(FileType.CSV)
    assert adj_list_from_py.get_file_type() == FileType.CSV

    adj_list_from_py.set_property_groups(
        adj_list_from_py.get_property_groups()
        + [
            PropertyGroup.from_python(
                "prefix3", FileType.CSV, props_list_1 + props_list_2
            )
        ]
    )
    assert all(
        pg_left == pg_right
        for pg_left, pg_right in zip(
            adj_list_from_py.get_property_groups(),
            [
                PropertyGroup.from_python("prefix1", FileType.PARQUET, props_list_1),
                PropertyGroup.from_python("prefix2", FileType.ORC, props_list_2),
                PropertyGroup.from_python(
                    "prefix3", FileType.CSV, props_list_1 + props_list_2
                ),
            ],
        )
    )


def test_vertex_info(spark):
    initialize(spark)

    props_list_1 = [
        Property.from_python("non_primary", GarType.DOUBLE, False),
        Property.from_python("primary", GarType.INT64, True),
    ]

    props_list_2 = [
        Property.from_python("non_primary", GarType.DOUBLE, False),
        Property.from_python("primary", GarType.INT64, True),
        Property("another_one", GarType.LIST, False),
    ]

    vertex_info_from_py = VertexInfo.from_python(
        "label",
        100,
        "prefix",
        [
            PropertyGroup.from_python("prefix1", FileType.PARQUET, props_list_1),
            PropertyGroup.from_python("prefix2", FileType.ORC, props_list_2),
        ],
        "1",
    )

    assert vertex_info_from_py.contain_property_group(
        PropertyGroup.from_python("prefix1", FileType.PARQUET, props_list_1)
    )
    assert (
        vertex_info_from_py.contain_property_group(
            PropertyGroup.from_python("prefix333", FileType.PARQUET, props_list_1)
        )
        == False
    )

    assert vertex_info_from_py.contain_property("primary")
    assert vertex_info_from_py.contain_property("non_primary")
    assert vertex_info_from_py.contain_property("non_existen_one") == False

    yaml_string = vertex_info_from_py.dump()
    restored_py_obj = yaml.safe_load(yaml_string)

    assert restored_py_obj["label"] == "label"
    assert restored_py_obj["prefix"] == "prefix"

    # test setters
    vertex_info_from_py.set_label("new_label")
    assert vertex_info_from_py.get_label() == "new_label"

    vertex_info_from_py.set_chunk_size(101)
    assert vertex_info_from_py.get_chunk_size() == 101

    vertex_info_from_py.set_prefix("new_prefix")
    assert vertex_info_from_py.get_prefix() == "new_prefix"

    vertex_info_from_py.set_version("2")
    assert vertex_info_from_py.get_version() == "2"

    vertex_info_from_py.set_property_groups(
        [
            PropertyGroup.from_python("prefix1", FileType.PARQUET, props_list_1),
            PropertyGroup.from_python("prefix2", FileType.ORC, props_list_2),
            PropertyGroup.from_python(
                "prefix3", FileType.CSV, props_list_1 + props_list_2
            ),
        ],
    )
    assert len(vertex_info_from_py.get_property_groups()) == 3

    # Get property group
    assert vertex_info_from_py.get_property_group("primary") is not None
    assert vertex_info_from_py.get_property_group("non_primary") is not None
    assert vertex_info_from_py.get_property_group("another_one") is not None

    with pytest.raises(IllegalArgumentException) as e:
        vertex_info_from_py.get_property_group("non-exsiten-one")
        assert e == "Property not found: non-exsiten-one"

    assert vertex_info_from_py.get_property_type("primary") == GarType.INT64
    assert vertex_info_from_py.get_property_type("non_primary") == GarType.DOUBLE
    assert vertex_info_from_py.get_property_type("another_one") == GarType.LIST

    with pytest.raises(IllegalArgumentException) as e:
        vertex_info_from_py.get_property_type("non-existen-one")
        assert e == "Property not found: non-exsiten-one"

    # Load from disk
    person_info = VertexInfo.load_vertex_info(
        GRAPHAR_TESTS_EXAMPLES.joinpath("transformer")
        .joinpath("person.vertex.yml")
        .absolute()
        .__str__()
    )
    assert person_info.get_label() == "person"
    assert person_info.get_chunk_size() == 50
    assert person_info.get_prefix() == "vertex/person/"
    assert person_info.get_version() == "gar/v1"
    assert len(person_info.get_property_groups()) == 2
    assert person_info.get_property_type("id") == GarType.INT64
    assert person_info.get_property_type("firstName") == GarType.STRING

    # Primary keys logic
    assert person_info.get_primary_key() == "id"
    assert person_info.is_primary_key("id")
    assert person_info.is_primary_key("firstName") == False

    # Other
    assert (
        person_info.get_vertices_num_file_path()
        == person_info.get_prefix() + "vertex_count"
    )
    assert person_info.is_validated()

    assert (
        VertexInfo.from_scala(person_info.to_scala()).get_prefix()
        == person_info.get_prefix()
    )

    nebula_vi = VertexInfo.load_vertex_info(
        GRAPHAR_TESTS_EXAMPLES.joinpath("nebula")
        .joinpath("team.vertex.yml")
        .absolute()
        .__str__()
    )
    assert (
        GRAPHAR_TESTS_EXAMPLES.joinpath("nebula")
        .joinpath(nebula_vi.get_file_path(nebula_vi.get_property_group("name"), 0))
        .exists()
    )
    assert len(nebula_vi.get_path_prefix(nebula_vi.get_property_group("name"))) > 0


def test_edge_info(spark):
    initialize(spark)

    py_edge_info = EdgeInfo.from_python(
        src_label="src_label",
        edge_label="edge_label",
        dst_label="dst_label",
        chunk_size=100,
        src_chunk_size=101,
        dst_chunk_size=102,
        directed=True,
        prefix="prefix",
        adj_lists=[],
        version="v1",
    )

    # getters/setters
    py_edge_info.set_src_label("new_src_label")
    assert py_edge_info.get_src_label() == "new_src_label"

    py_edge_info.set_dst_label("new_dst_label")
    assert py_edge_info.get_dst_label() == "new_dst_label"

    py_edge_info.set_edge_label("new_edge_label")
    assert py_edge_info.get_edge_label() == "new_edge_label"

    py_edge_info.set_chunk_size(101)
    assert py_edge_info.get_chunk_size() == 101

    py_edge_info.set_src_chunk_size(102)
    assert py_edge_info.get_src_chunk_size() == 102

    py_edge_info.set_dst_chunk_size(103)
    assert py_edge_info.get_dst_chunk_size() == 103

    py_edge_info.set_directed(False)
    assert py_edge_info.get_directed() == False

    py_edge_info.set_prefix("new_prefix")
    assert py_edge_info.get_prefix() == "new_prefix"

    py_edge_info.set_version("v2")
    assert py_edge_info.get_version() == "v2"

    props_list_1 = [
        Property.from_python("non_primary", GarType.DOUBLE, False),
        Property.from_python("primary", GarType.INT64, True),
    ]
    py_edge_info.set_adj_lists(
        [
            AdjList.from_python(
                True,
                "dest",
                "prefix",
                FileType.PARQUET,
                [
                    PropertyGroup.from_python(
                        "prefix1", FileType.PARQUET, props_list_1
                    ),
                ],
            )
        ]
    )
    assert len(py_edge_info.get_adj_lists()) == 1

    # Load from YAML
    person_knows_person_info = EdgeInfo.load_edge_info(
        GRAPHAR_TESTS_EXAMPLES.joinpath("transformer")
        .joinpath("person_knows_person.edge.yml")
        .absolute()
        .__str__()
    )
    assert person_knows_person_info.get_directed() == False
    assert person_knows_person_info.contain_property("creationDate")
    assert (
        person_knows_person_info.get_adj_list_prefix(AdjListType.UNORDERED_BY_DEST)
        is not None
    )
    assert (
        person_knows_person_info.get_adj_list_prefix(AdjListType.ORDERED_BY_SOURCE)
        is not None
    )
    with pytest.raises(IllegalArgumentException) as e:
        person_knows_person_info.get_adj_list_prefix(AdjListType.ORDERED_BY_DEST)
        assert e == "adj list type not found: ordered_by_dest"

    assert person_knows_person_info.contain_adj_list(AdjListType.UNORDERED_BY_DEST)
    assert (
        person_knows_person_info.contain_adj_list(AdjListType.UNORDERED_BY_SOURCE)
        == False
    )

    assert person_knows_person_info.get_chunk_size() == 500
    assert (
        person_knows_person_info.get_offset_path_prefix(AdjListType.ORDERED_BY_SOURCE)
        is not None
    )

    assert (
        person_knows_person_info.get_adj_list_file_type(AdjListType.UNORDERED_BY_DEST)
        == FileType.CSV
    )
    assert (
        person_knows_person_info.get_adj_list_file_type(AdjListType.ORDERED_BY_SOURCE)
        != 0
    )
    assert (
        len(person_knows_person_info.get_property_groups(AdjListType.ORDERED_BY_SOURCE))
        == 1
    )
    assert person_knows_person_info.contain_property_group(
        person_knows_person_info.get_property_groups(AdjListType.UNORDERED_BY_DEST)[0],
        AdjListType.UNORDERED_BY_DEST,
    )
    assert person_knows_person_info.get_property_type("creationDate") == GarType.STRING
    assert person_knows_person_info.is_primary_key("creationDate") == False
    assert person_knows_person_info.get_primary_key() == ""
    assert person_knows_person_info.is_validated()
    assert (
        person_knows_person_info.get_vertices_num_file_path(
            AdjListType.ORDERED_BY_SOURCE
        )
        == "edge/person_knows_person/ordered_by_source/vertex_count"
    )
    assert (
        person_knows_person_info.get_edges_num_path_prefix(
            AdjListType.ORDERED_BY_SOURCE
        )
        == "edge/person_knows_person/ordered_by_source/edge_count"
    )
    assert (
        person_knows_person_info.get_edges_num_file_path(
            0, AdjListType.ORDERED_BY_SOURCE
        )
        == "edge/person_knows_person/ordered_by_source/edge_count0"
    )
    assert (
        person_knows_person_info.get_adj_list_offset_file_path(
            0, AdjListType.ORDERED_BY_SOURCE
        )
        == "edge/person_knows_person/ordered_by_source/offset/chunk0"
    )
    assert (
        person_knows_person_info.get_adj_list_file_path(
            0, 0, AdjListType.ORDERED_BY_SOURCE
        )
        == "edge/person_knows_person/ordered_by_source/adj_list/part0/chunk0"
    )
    assert (
        person_knows_person_info.get_adj_list_path_prefix(
            None, AdjListType.ORDERED_BY_SOURCE
        )
        is not None
    )
    assert (
        person_knows_person_info.get_adj_list_path_prefix(
            0, AdjListType.ORDERED_BY_SOURCE
        )
        is not None
    )
    assert (
        person_knows_person_info.get_property_file_path(
            person_knows_person_info.get_property_group(
                "creationDate", AdjListType.ORDERED_BY_SOURCE
            ),
            AdjListType.ORDERED_BY_SOURCE,
            0,
            0,
        )
        is not None
    )
    assert (
        person_knows_person_info.get_property_group_path_prefix(
            person_knows_person_info.get_property_group(
                "creationDate", AdjListType.ORDERED_BY_SOURCE
            ),
            AdjListType.ORDERED_BY_SOURCE,
            0,
        )
    ) is not None
    assert (
        person_knows_person_info.get_property_group_path_prefix(
            person_knows_person_info.get_property_group(
                "creationDate", AdjListType.ORDERED_BY_SOURCE
            ),
            AdjListType.ORDERED_BY_SOURCE,
            None,
        )
    ) is not None
    assert person_knows_person_info.get_concat_key() == "person_knows_person"
    yaml_string = person_knows_person_info.dump()
    parsed_dict = yaml.safe_load(yaml_string)
    assert "prefix" in parsed_dict.keys()


def test_graph_info(spark):
    initialize(spark)

    modern_graph_person = GraphInfo.load_graph_info(
        GRAPHAR_TESTS_EXAMPLES.joinpath("modern_graph")
        .joinpath("modern_graph.graph.yml")
        .absolute()
        .__str__()
    )
    assert len(modern_graph_person.get_edges()) == 2
    assert modern_graph_person.get_name() == "modern_graph"
    assert len(modern_graph_person.get_vertex_infos().keys()) == 2
    assert "person" in modern_graph_person.get_vertex_infos().keys()
    assert "software" in modern_graph_person.get_vertex_infos().keys()
    assert len(modern_graph_person.get_edge_infos()) == 2
    assert "person_knows_person" in modern_graph_person.get_edge_infos().keys()
    assert "person_created_software" in modern_graph_person.get_edge_infos().keys()

    assert modern_graph_person.get_edge_info("person", "knows", "person") is not None
    assert modern_graph_person.get_vertex_info("person") is not None

    # YAML
    yaml_dict = yaml.safe_load(modern_graph_person.dump())
    assert "name" in yaml_dict
    assert yaml_dict["version"] == "gar/v1"

    # Python constructor and setters
    py_graph_info = GraphInfo.from_python(
        "name", "prefix", ["person", "software"], ["person_knnows_person"], "v1"
    )
    py_graph_info.set_name("new_name")
    assert py_graph_info.get_name() == "new_name"
    py_graph_info.set_prefix("new_prefix")
    assert py_graph_info.get_prefix() == "new_prefix"

    init_vertices_size = py_graph_info.get_vertices().__len__()
    new_vertices = py_graph_info.get_vertices()
    new_vertices.append("new_one")
    py_graph_info.set_vertices(new_vertices)
    assert len(py_graph_info.get_vertices()) > init_vertices_size

    init_edges_size = py_graph_info.get_edges().__len__()
    new_edges = py_graph_info.get_edges()
    new_edges.append("new_one")
    py_graph_info.set_edges(new_edges)
    assert len(py_graph_info.get_edges()) > init_edges_size

    py_graph_info.set_version("v2")
    assert py_graph_info.get_version() == "v2"

    py_graph_info.add_edge_info(
        EdgeInfo.from_python(
            "src_label100",
            "edge_label100",
            "dst_label100",
            10,
            100,
            100,
            True,
            "prefix",
            [],
            "v1",
        )
    )
    assert len(py_graph_info.get_edge_infos()) == 1

    py_graph_info.add_vertex_info(
        VertexInfo.from_python("some", 100, "prefix", [], "v1")
    )
    assert len(py_graph_info.get_vertex_infos()) == 1
