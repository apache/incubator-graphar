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
import tempfile

from graphar import GraphInfo
from graphar.types import AdjListType, ValidateLevel
from graphar.high_level import (
    VerticesCollection,
    EdgesCollection,
    BuilderVertex,
    VerticesBuilder,
    BuilderEdge,
    EdgesBuilder,
)


@pytest.fixture
def ldbc_sample_graph(test_data_root):
    return test_data_root + "/ldbc_sample/parquet/ldbc_sample.graph.yml"


@pytest.fixture
def sample_graph_info(ldbc_sample_graph):
    return GraphInfo.load(ldbc_sample_graph)


@pytest.fixture
def sample_graph_vertex(sample_graph_info):
    return sample_graph_info.get_vertex_info("person")


@pytest.fixture
def sample_graph_edge(sample_graph_info):
    return sample_graph_info.get_edge_info("person", "knows", "person")


def test_vertices_collection(sample_graph_info):
    """Test vertices collection reading functionality."""
    # Construct vertices collection
    type_name = "person"
    vertices = VerticesCollection.Make(sample_graph_info, type_name)
    # Use vertices collection
    count = 0
    # Iterate through vertices collection
    for vertex in vertices:
        count += 1
        # Access data through vertex
        assert vertex.id() >= 0
        # Try to access properties
        try:
            vertex.property("id")
            vertex.property("firstName")
        except Exception:
            pass  # Properties might not exist in all test data=

    # Test size
    assert count == vertices.size()


def test_edges_collection(sample_graph_info):
    """Test edges collection reading functionality."""
    # Construct edges collection
    src_type = "person"
    edge_type = "knows"
    dst_type = "person"
    adj_list_type = AdjListType.ordered_by_source

    edges = EdgesCollection.Make(sample_graph_info, src_type, edge_type, dst_type, adj_list_type)

    # Use edges collection
    count = 0
    # Iterate through edges collection
    for edge in edges:
        count += 1
        # Access data through edge
        assert edge.source() >= 0
        assert edge.destination() >= 0
        # Try to access properties
        try:
            edge.property("creationDate")
        except Exception:
            pass  # Properties might not exist in all test data

    # Test size
    assert count == edges.size()


def test_vertices_builder(sample_graph_vertex):
    """Test vertices builder functionality."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Construct vertices builder
        start_index = 0
        builder = VerticesBuilder.Make(sample_graph_vertex, temp_dir, start_index)

        # Set validate level
        builder.SetValidateLevel(ValidateLevel.strong_validate)

        # Prepare vertex data
        vertex_count = 3
        property_names = ["id", "firstName", "lastName", "gender"]
        id_values = [0, 1, 2]
        firstName_values = ["John", "Jane", "Alice"]
        lastName_values = ["Smith", "Doe", "Wonderland"]
        gender_values = ["male", "female", "female"]

        # Add vertices
        for i in range(vertex_count):
            v = BuilderVertex()
            v.AddProperty(property_names[0], id_values[i])
            v.AddProperty(property_names[1], firstName_values[i])
            v.AddProperty(property_names[2], lastName_values[i])
            v.AddProperty(property_names[3], gender_values[i])
            builder.AddVertex(v)

        # Test vertex count
        assert builder.GetNum() == vertex_count

        # Dump
        builder.Dump()

        # Clear vertices
        builder.Clear()
        assert builder.GetNum() == 0


def test_edges_builder(sample_graph_edge):
    """Test edges builder functionality."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Construct edges builder
        vertex_count = 3
        adj_list_type = AdjListType.ordered_by_dest
        builder = EdgesBuilder.Make(sample_graph_edge, temp_dir, adj_list_type, vertex_count)

        # Set validate level
        builder.SetValidateLevel(ValidateLevel.strong_validate)

        # Prepare edge data
        edge_count = 4
        src_values = [1, 0, 0, 2]
        dst_values = [0, 1, 2, 1]
        creationDate_values = ["2010-01-01", "2011-01-01", "2012-01-01", "2013-01-01"]

        # Add edges
        for i in range(edge_count):
            e = BuilderEdge(src_values[i], dst_values[i])
            e.AddProperty("creationDate", creationDate_values[i])
            builder.AddEdge(e)

        # Test edge count
        assert builder.GetNum() == edge_count

        # Dump
        builder.Dump()

        # Clear edges
        builder.Clear()
        assert builder.GetNum() == 0


def test_vertex_iter_operations(sample_graph_info):
    """Test vertex iterator operations."""
    # Construct vertices collection
    type_name = "person"
    vertices = VerticesCollection.Make(sample_graph_info, type_name)

    # Test iterator operations
    it = vertices.begin()
    if it != vertices.end():
        # Test accessing vertex id through iterator
        vertex_id = it.id()
        assert vertex_id >= 0


def test_edge_iter_operations(sample_graph_info):
    """Test edge iterator operations."""
    # Construct edges collection
    src_type = "person"
    edge_type = "knows"
    dst_type = "person"
    adj_list_type = AdjListType.ordered_by_source

    edges = EdgesCollection.Make(sample_graph_info, src_type, edge_type, dst_type, adj_list_type)

    # Test iterator operations
    it = edges.begin()
    if it != edges.end():
        # Test accessing edge source and destination through iterator
        source = it.source()
        destination = it.destination()
        assert source >= 0
        assert destination >= 0
