#!/usr/bin/env python3
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

import os
import sys
import graphar
import graphar.high_level as gar_api
import graphar.types as types


def load_graph_info():
    """
    Load graph information from YAML files.
    """
    # Get the testing data path
    test_data_dir = os.environ.get("GAR_TEST_DATA")
    if not test_data_dir:
        print("Test data path not set. Please set GAR_TEST_DATA environment variable.")
        return None

    # Path to the graph YAML file
    graph_yaml_path = os.path.join(test_data_dir, "ldbc_sample/parquet/ldbc_sample.graph.yml")

    # Load graph info from YAML
    try:
        graph_info = graphar.GraphInfo.load(graph_yaml_path)
        return graph_info
    except Exception as e:
        print(f"Error loading graph info: {e}")
        return None


def demonstrate_vertices_collection(graph_info):
    """
    Demonstrates how to use VerticesCollection to read vertex data.
    """
    print("\n--- Vertices Collection ---")

    # Create vertices collection for "person" type
    try:
        vertices = gar_api.VerticesCollection.Make(graph_info, "person")
        print(f"Total number of vertices: {vertices.size()}")

        # Iterate through vertices and print first 5
        count = 0
        for vertex in vertices:
            if count >= 5:
                break
            vertex_id = vertex.id()
            first_name = vertex.property("firstName")
            last_name = vertex.property("lastName")
            print(f"Vertex ID: {vertex_id}, Name: {first_name} {last_name}")
            count += 1

        if count == 0:
            print("No vertices found")

    except Exception as e:
        print(f"Error working with vertices collection: {e}")


def demonstrate_edges_collection(graph_info):
    """
    Demonstrates how to use EdgesCollection to read edge data.
    """
    print("\n--- Edges Collection ---")

    # Create edges collection for "person_knows_person" with ordered_by_source
    try:
        edges = gar_api.EdgesCollection.Make(
            graph_info, "person", "knows", "person", types.AdjListType.ordered_by_source
        )
        print(f"Total number of edges: {edges.size()}")

        # Iterate through edges and print first 5
        count = 0
        for edge in edges:
            if count >= 5:
                break
            source_id = edge.source()
            destination_id = edge.destination()
            creation_date = edge.property("creationDate")
            print(f"Edge: {source_id} -> {destination_id}, Creation Date: {creation_date}")
            count += 1

        if count == 0:
            print("No edges found")

    except Exception as e:
        print(f"Error working with edges collection: {e}")


def main():
    print("GraphAr High-Level API Example")
    print("==============================")

    # Load graph info
    graph_info = load_graph_info()
    if not graph_info:
        return

    # Demonstrate vertices collection
    demonstrate_vertices_collection(graph_info)

    # Demonstrate edges collection
    demonstrate_edges_collection(graph_info)

    print("\nExample completed successfully!")


if __name__ == "__main__":
    main()
