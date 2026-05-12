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

import pytest
import typer

import graphar as gar


@pytest.fixture
def sample_graph(test_data_root):
    return test_data_root + "/ldbc_sample/csv/" + "ldbc_sample.graph.yml"


@pytest.fixture
def sample_graph_info(sample_graph):
    return gar.graph_info.GraphInfo.load(sample_graph)


@pytest.fixture
def sample_graph_vertex(sample_graph_info):
    return sample_graph_info.get_vertex_info("person")


@pytest.fixture
def sample_graph_edge(sample_graph_info):
    return sample_graph_info.get_edge_info("person", "knows", "person")


def test_graph_info_basics(sample_graph_info):
    """Test basic graph info functionality."""
    assert sample_graph_info is not None
    assert sample_graph_info.get_name() == "ldbc_sample"

    # Test vertex and edge info counts
    assert len(sample_graph_info.get_vertex_infos()) == 1
    assert sample_graph_info.vertex_info_num() == 1
    assert len(sample_graph_info.get_edge_infos()) == 1
    assert sample_graph_info.edge_info_num() == 1

    # Test getting specific vertex and edge info
    person_vertex_info = sample_graph_info.get_vertex_info("person")
    assert person_vertex_info is not None

    knows_edge_info = sample_graph_info.get_edge_info("person", "knows", "person")
    assert knows_edge_info is not None

    # Test version
    assert sample_graph_info.version().get_version() == 1


def test_person_vertex_info_basics(sample_graph_vertex):
    """Test person vertex info basics."""
    assert sample_graph_vertex.get_type() == "person"
    assert sample_graph_vertex.get_chunk_size() == 100
    assert sample_graph_vertex.get_prefix() == "vertex/person/"
    assert sample_graph_vertex.property_group_num() == 2
    assert sample_graph_vertex.version().get_version() == 1


def test_person_vertex_property_groups(sample_graph_vertex):
    """Test person vertex property groups."""
    # Test first property group (id)
    id_property_group = sample_graph_vertex.get_property_group_by_index(0)
    assert id_property_group is not None
    assert id_property_group.get_prefix() == "id/"
    assert id_property_group.get_file_type() == gar.types.FileType.CSV

    # Check id property
    assert sample_graph_vertex.has_property("id")
    id_property_type = sample_graph_vertex.get_property_type("id")
    assert id_property_type.to_type_name() == "int64"
    assert sample_graph_vertex.is_primary_key("id")
    assert not sample_graph_vertex.is_nullable_key("id")

    # Test second property group (firstName_lastName_gender)
    name_property_group = sample_graph_vertex.get_property_group_by_index(1)
    assert name_property_group is not None
    assert name_property_group.get_prefix() == "firstName_lastName_gender/"
    assert name_property_group.get_file_type() == gar.types.FileType.CSV

    # Check name properties
    assert sample_graph_vertex.has_property("firstName")
    first_name_type = sample_graph_vertex.get_property_type("firstName")
    assert first_name_type.to_type_name() == "string"

    assert sample_graph_vertex.has_property("lastName")
    last_name_type = sample_graph_vertex.get_property_type("lastName")
    assert last_name_type.to_type_name() == "string"

    assert sample_graph_vertex.has_property("gender")
    gender_type = sample_graph_vertex.get_property_type("gender")
    assert gender_type.to_type_name() == "string"


def test_knows_edge_info_basics(sample_graph_edge):
    """Test knows edge info basics."""
    assert sample_graph_edge.get_edge_type() == "knows"
    assert sample_graph_edge.get_chunk_size() == 1024
    assert sample_graph_edge.get_src_type() == "person"
    assert sample_graph_edge.get_src_chunk_size() == 100
    assert sample_graph_edge.get_dst_type() == "person"
    assert sample_graph_edge.get_dst_chunk_size() == 100
    assert not sample_graph_edge.is_directed()
    assert sample_graph_edge.get_prefix() == "edge/person_knows_person/"
    assert sample_graph_edge.version().get_version() == 1


def test_knows_edge_adjacency_lists(sample_graph_edge):
    """Test knows edge adjacency lists."""
    # Check that edge has both ordered_by_source and ordered_by_dest adjacency lists
    assert sample_graph_edge.has_adjacent_list_type(gar.types.AdjListType.ordered_by_source)
    assert sample_graph_edge.has_adjacent_list_type(gar.types.AdjListType.ordered_by_dest)

    # Test ordered_by_source adjacency list
    adj_by_source = sample_graph_edge.get_adjacent_list(gar.types.AdjListType.ordered_by_source)
    assert adj_by_source is not None
    assert adj_by_source.get_file_type() == gar.types.FileType.CSV
    assert adj_by_source.get_type() == gar.types.AdjListType.ordered_by_source
    assert adj_by_source.get_prefix() == "ordered_by_source/"


def test_knows_edge_property_groups(sample_graph_edge):
    """Test knows edge property groups."""
    assert sample_graph_edge.property_group_num() == 1

    # Test property group
    property_group = sample_graph_edge.get_property_group_by_index(0)
    assert property_group is not None
    assert property_group.get_prefix() == "creationDate/"
    assert property_group.get_file_type() == gar.types.FileType.CSV

    # Check creationDate property
    assert sample_graph_edge.has_property("creationDate")
    creation_date_type = sample_graph_edge.get_property_type("creationDate")
    assert creation_date_type.to_type_name() == "string"
    assert not sample_graph_edge.is_primary_key("creationDate")
    assert sample_graph_edge.is_nullable_key("creationDate")


def test_graph_validation(sample_graph_info, sample_graph_vertex, sample_graph_edge):
    """Test graph validation."""
    # Test that the sample graph is validated
    assert sample_graph_info.is_validated()
    assert sample_graph_vertex.is_validated()
    assert sample_graph_edge.is_validated()
