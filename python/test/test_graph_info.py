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
import graphar.graph_info_api as graphInfo


@pytest.fixture
def sample_graph(test_data_root):
  return test_data_root +"/ldbc_sample/parquet/" + "ldbc_sample.graph.yml";

@pytest.fixture
def sample_graph_vertex(test_data_root):
  return test_data_root +"/ldbc_sample/parquet/" + "person.vertex.yml";

@pytest.fixture
def sample_graph_edge(test_data_root):
  return test_data_root +"/ldbc_sample/parquet/" + "person_knows_person.edge.yml";

def test_data_type_binding():
    dataType = gar.DataType()
    print(dataType.id())
    dataType = gar.DataType(gar.Type.INT64)
    print(dataType.id())

def test_graph_info_load(sample_graph):
    #load file and print
    sample_graph = graphInfo.GraphInfo.load(sample_graph)
    print(sample_graph.dump())
    pereson_info = sample_graph.get_vertex_info("person")
    print(pereson_info.dump())

def test_vertex_info_load(sample_graph_vertex):
    #load file and print
    person_info = graphInfo.VertexInfo.load(sample_graph_vertex)
    print(person_info.dump())
