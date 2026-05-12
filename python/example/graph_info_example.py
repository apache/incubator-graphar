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
import graphar.types as types


def load_graph_info():
    """
    Demonstrates how to load graph information from YAML files.
    """
    # Get the testing data path
    test_data_dir = os.environ.get("GAR_TEST_DATA")
    if not test_data_dir:
        print("Test data path not set. Please set GAR_TEST_DATA environment variable.")
        return

    # Path to the graph YAML file
    graph_yaml_path = os.path.join(test_data_dir, "ldbc_sample/parquet/ldbc_sample.graph.yml")

    # Load graph info from YAML
    try:
        graph_info = graphar.GraphInfo.load(graph_yaml_path)
        print(f"Graph name: {graph_info.get_name()}")
        print(f"Graph prefix: {graph_info.get_prefix()}")
        return graph_info
    except Exception as e:
        print(f"Error loading graph info: {e}")
        return None


def access_vertex_info(graph_info):
    """
    Demonstrates how to access vertex information from graph info.
    """
    print("\n--- Vertex Info ---")
    # Get vertex info for "person" type
    vertex_info = graph_info.get_vertex_info("person")
    if vertex_info:
        print(f"Vertex type: {vertex_info.get_type()}")
        print(f"Chunk size: {vertex_info.get_chunk_size()}")
        print(f"Vertex prefix: {vertex_info.get_prefix()}")

        # List all properties
        property_groups = vertex_info.get_property_groups()
        print("Properties:")
        for i, pg in enumerate(property_groups):
            print(f"  Group {i}:")
            for prop in pg.get_properties():
                print(f"    - {prop.name}: {prop.type} (primary: {prop.is_primary})")


def access_edge_info(graph_info):
    """
    Demonstrates how to access edge information from graph info.
    """
    print("\n--- Edge Info ---")
    # Get edge info for "person_knows_person" type
    edge_info = graph_info.get_edge_info("person", "knows", "person")
    if edge_info:
        print(f"Edge type: {edge_info.get_edge_type()}")
        print(f"Source type: {edge_info.get_src_type()}")
        print(f"Destination type: {edge_info.get_dst_type()}")
        print(f"Chunk size: {edge_info.get_chunk_size()}")
        print(f"Directed: {edge_info.is_directed()}")

        # List adjacency list types - we need to check each type individually
        print("Adjacency list types:")
        adj_list_types = [
            types.AdjListType.unordered_by_source,
            types.AdjListType.ordered_by_source,
            types.AdjListType.unordered_by_dest,
            types.AdjListType.ordered_by_dest,
        ]

        for adj_list_type in adj_list_types:
            if edge_info.has_adjacent_list_type(adj_list_type):
                adj_list = edge_info.get_adjacent_list(adj_list_type)
                print(f"  - {adj_list.get_type()}: {adj_list.get_file_type()}")

        # List all properties
        property_groups = edge_info.get_property_groups()
        print("Properties:")
        for i, pg in enumerate(property_groups):
            print(f"  Group {i}:")
            for prop in pg.get_properties():
                print(f"    - {prop.name}: {prop.type}")


def main():
    print("GraphAr GraphInfo Example")
    print("=========================")

    # Load graph info
    graph_info = load_graph_info()
    if not graph_info:
        return

    # Access vertex information
    access_vertex_info(graph_info)

    # Access edge information
    access_edge_info(graph_info)

    print("\nExample completed successfully!")


if __name__ == "__main__":
    main()
