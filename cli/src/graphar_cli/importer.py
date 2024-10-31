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

from logging import getLogger

from .config import ImportConfig

logger = getLogger("graphar_cli")


def validate(import_config: ImportConfig):
    vertex_types = set()
    for vertex in import_config.import_schema.vertices:
        if vertex.type in vertex_types:
            msg = f"Duplicate vertex type {vertex.type}"
            raise ValueError(msg)
        vertex_types.add(vertex.type)

        prop_names = set()
        primary_keys = []
        for prop_group in vertex.property_groups:
            for prop in prop_group.properties:
                if prop.name in prop_names:
                    msg = f"Duplicate property '{prop.name}' in vertex '{vertex.type}'"
                    raise ValueError(msg)
                prop_names.add(prop.name)
                if prop.is_primary:
                    if len(primary_keys):
                        msg = (
                            f"Multiple primary keys '{primary_keys[0]}' and '{prop.name}' "
                            f"found in vertex '{vertex.type}'"
                        )
                        raise ValueError(msg)
                    primary_keys.append(prop.name)
                    if prop.nullable:
                        msg = f"Primary key '{prop.name}' in '{vertex.type}' cannot be nullable"
                        raise ValueError(msg)
        source_values = [value for source in vertex.sources for value in source.columns.values()]
        for prop_name in prop_names:
            if prop_name not in source_values:
                msg = (
                    f"Property '{prop_name}' in vertex '{vertex.type}' not found in source columns"
                )
                raise ValueError(msg)
        msg = f"Source columns are more than the properties in vertex '{vertex.type}'"
        assert len(source_values) == len(prop_names), msg
        logger.debug("Validated vertex %s", vertex.type)

    edge_types = set()
    for edge in import_config.import_schema.edges:
        if edge.edge_type in edge_types:
            msg = f"Duplicate edge type {edge.type}"
            raise ValueError(msg)
        edge_types.add(edge.edge_type)

        if edge.src_type not in vertex_types:
            msg = f"Source vertex type {edge.src_type} not found"
            raise ValueError(msg)
        if edge.dst_type not in vertex_types:
            msg = f"Destination vertex type {edge.dst_type} not found"
            raise ValueError(msg)
        src_vertex = next(
            vertex
            for vertex in import_config.import_schema.vertices
            if vertex.type == edge.src_type
        )
        if edge.src_prop not in [
            prop.name for prop_group in src_vertex.property_groups for prop in prop_group.properties
        ]:
            msg = (
                f"Source property '{edge.src_prop}' "
                f"not found in source vertex '{edge.src_type}' "
                f"in edge '{edge.edge_type}'"
            )
            raise ValueError(msg)
        dst_vertex = next(
            vertex
            for vertex in import_config.import_schema.vertices
            if vertex.type == edge.dst_type
        )
        if edge.dst_prop not in [
            prop.name for prop_group in dst_vertex.property_groups for prop in prop_group.properties
        ]:
            msg = (
                f"Destination property '{edge.dst_prop}' "
                f"not found in destination vertex '{edge.dst_type}' "
                f"in edge '{edge.edge_type}'"
            )
            raise ValueError(msg)
        prop_names = set()
        for prop_group in edge.property_groups:
            for prop in prop_group.properties:
                if prop.name in prop_names:
                    msg = f"Duplicate property '{prop.name}' in edge '{edge.edge_type}'"
                    raise ValueError(msg)
                prop_names.add(prop.name)

        source_values = [value for source in edge.sources for value in source.columns.values()]
        for prop_name in prop_names:
            if prop_name not in source_values:
                msg = (
                    f"Property '{prop_name}' in edge "
                    f"'{edge.dst_prop}_{edge.edge_type}_{edge.edge_type}' "
                    f"not found in source columns"
                )
                raise ValueError(msg)
        # TODO: Validate source columns
        logger.debug("Validated edge %s %s %s", edge.src_type, edge.edge_type, edge.dst_type)
