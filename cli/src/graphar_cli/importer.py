from .config import ImportConfig


def validate(config: ImportConfig):
    vertex_types = set()
    for vertex in config.import_schema.vertices:
        if vertex.type in vertex_types:
            msg = f"Duplicate vertex type {vertex.type}"
            raise ValueError(msg)
        vertex_types.add(vertex.type)

        prop_names = set()
        for prop in vertex.properties:
            if prop.name in prop_names:
                msg = f"Duplicate property {prop.name} in vertex properties"
                raise ValueError(msg)
            prop_names.add(prop.name)
            if prop.name == vertex.primary and prop.nullable:
                msg = f"Primary key {vertex.primary} cannot be nullable"
                raise ValueError(msg)
        if vertex.primary not in prop_names:
            msg = f"Primary key {vertex.primary} not found in vertex properties"
            raise ValueError(msg)
        prop_names_in_groups = set()
        for prop_group in vertex.propertyGroups:
            for prop_name in prop_group:
                if prop_name not in prop_names:
                    msg = f"Property {prop} not found in vertex properties"
                    raise ValueError(msg)
                if prop_name in prop_names_in_groups:
                    msg = f"Property {prop} found in multiple property groups"
                    raise ValueError(msg)
                prop_names_in_groups.add(prop_name)
        for prop_name in prop_names:
            if prop_name not in vertex.source.columns:
                msg = f"Property {prop_name} not found in source columns"
                raise ValueError(msg)
        
    edge_types = set()
    for edge in config.import_schema.edges:
        if edge.type in edge_types:
            msg = f"Duplicate edge type {edge.type}"
            raise ValueError(msg)
        edge_types.add(edge.type)

        if edge.srcType not in vertex_types:
            msg = f"Source vertex type {edge.srcType} not found"
            raise ValueError(msg)
        if edge.dstType not in vertex_types:
            msg = f"Destination vertex type {edge.dstType} not found"
            raise ValueError(msg)
        # TODO: Check that source and destination properties are present in the source vertex
        prop_names = set()
        for prop in edge.properties:
            if prop.name in prop_names:
                msg = f"Duplicate property {prop.name} in edge properties"
                raise ValueError(msg)
            prop_names.add(prop.name)
        prop_names_in_groups = set()
        for prop_group in edge.propertyGroups:
            for prop_name in prop_group:
                if prop_name not in prop_names:
                    msg = f"Property {prop} not found in edge properties"
                    raise ValueError(msg)
                if prop_name in prop_names_in_groups:
                    msg = f"Property {prop} found in multiple property groups"
                    raise ValueError(msg)
                prop_names_in_groups.add(prop_name)
        for prop_name in prop_names:
            if prop_name not in edge.source.columns:
                msg = f"Property {prop_name} not found in source columns"
                raise ValueError(msg)
        

def do_import(config: ImportConfig):
    pass