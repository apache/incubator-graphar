from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, field_validator, model_validator
from typing_extensions import Self

default_file_type = "parquet"
default_source_file_type = "csv"
default_adj_list_type = "ordered_by_source"
default_regular_separator = "_"
default_version = "gar/v1"


class GraphArConfig(BaseModel):
    path: str
    name: str
    vertex_chunk_size: Optional[int] = 100
    edge_chunk_size: Optional[int] = 1024
    file_type: Literal["parquet", "orc", "csv", "json"] = default_file_type
    adj_list_type: Literal[
        "ordered_by_source", "ordered_by_dest", "unordered_by_source", "unordered_by_dest"
    ] = default_adj_list_type
    source_file_type: Literal["parquet", "orc", "csv", "json"] = default_source_file_type
    version: Optional[str] = default_version


class Property(BaseModel):
    name: str
    data_type: Literal["bool", "int32", "int64", "float", "double", "string", "date", "timestamp"]
    is_primary: bool = False
    nullable: Optional[bool] = None

    @model_validator(mode="after")
    def check_nullable(self) -> Self:
        if self.is_primary and self.nullable:
            msg = f"Primary key `{self.name}` must not be nullable."
            raise ValueError(msg)
        if self.is_primary:
            self.nullable = False
        elif self.nullable is None:
                self.nullable = True
        return self


class PropertyGroup(BaseModel):
    properties: List[Property]
    file_type: Optional[Literal["parquet", "orc", "csv", "json"]] = None

    @field_validator("properties")
    def check_properties_length(cls, v):
        if len(v) == 0:
            msg = "properties must not be empty."
            raise ValueError(msg)
        return v


class Source(BaseModel):
    file_type: Optional[Literal["parquet", "orc", "csv", "json"]] = None
    path: str
    delimiter: str = ","
    columns: Dict[str, str]

    @field_validator("path")
    def check_path(cls, v):
        if not Path(v).is_file():
            msg = f"{v} is not a file."
            raise ValueError(msg)
        return v

    @field_validator("delimiter")
    def check_delimiter(cls, v):
        if len(v) != 1:
            msg = "delimiter must be a single character."
            raise ValueError(msg)
        return v


class Vertex(BaseModel):
    type: str
    labels: List[str] = []
    chunk_size: Optional[int] = None
    prefix: Optional[str] = None
    property_groups: List[PropertyGroup]
    sources: List[Source]

    @field_validator("property_groups")
    def check_property_groups_length(cls, v):
        if len(v) == 0:
            msg = "property_groups must not be empty."
            raise ValueError(msg)
        return v

    @field_validator("sources")
    def check_sources_length(cls, v):
        if len(v) == 0:
            msg = "sources must not be empty."
            raise ValueError(msg)
        return v

    @model_validator(mode="after")
    def check_vertex_prefix(self) -> Self:
        prefix = self.prefix
        type = self.type
        if not prefix:
            self.prefix = f"vertex/{type}/"
        return self


class AdjList(BaseModel):
    ordered: bool
    aligned_by: Literal["src", "dst"]
    file_type: Literal["parquet", "orc", "csv", "json"]


class Edge(BaseModel):
    edge_type: str
    src_type: str
    src_prop: Optional[str] = None
    dst_type: str
    dst_prop: Optional[str] = None
    labels: List[str] = []
    chunk_size: Optional[int] = None
    adj_lists: List[AdjList] = []
    property_groups: List[PropertyGroup] = []
    sources: List[Source]
    prefix: Optional[str] = None

    @field_validator("sources")
    def check_sources_length(cls, v):
        if len(v) == 0:
            msg = "sources must not be empty."
            raise ValueError(msg)
        return v

    @model_validator(mode="after")
    def check_prefix(self) -> Self:
        prefix = self.prefix
        src_type = self.src_type
        edge_type = self.edge_type
        dst_type = self.dst_type
        if not prefix:
            self.prefix = (
                f"edge/{src_type}"
                f"{default_regular_separator}{edge_type}"
                f"{default_regular_separator}{dst_type}/"
            )
        return self


class ImportSchema(BaseModel):
    vertices: List[Vertex]
    edges: List[Edge]

    @field_validator("vertices")
    def check_property_groups_length(cls, v):
        if len(v) == 0:
            msg = "vertices must not be empty."
            raise ValueError(msg)
        return v


class ImportConfig(BaseModel):
    graphar: GraphArConfig
    import_schema: ImportSchema

    @field_validator("import_schema")
    def replace_none_values(cls, v: ImportSchema, values: Dict[str, Any]):
        config = values.data["graphar"]
        for vertex in v.vertices:
            if vertex.chunk_size is None:
                vertex.chunk_size = config.vertex_chunk_size
            for property_group in vertex.property_groups:
                if property_group.file_type is None:
                    property_group.file_type = config.file_type
            for source in vertex.sources:
                if source.file_type is None:
                    source.file_type = config.source_file_type
        for edge in v.edges:
            if edge.chunk_size is None:
                edge.chunk_size = config.edge_chunk_size
            if len(edge.adj_lists) == 0:
                if config.adj_list_type == "ordered_by_source":
                    edge.adj_lists.append(
                        AdjList(ordered=True, aligned_by="src", file_type=config.file_type)
                    )
                elif config.adj_list_type == "ordered_by_dest":
                    edge.adj_lists.append(
                        AdjList(ordered=True, aligned_by="dst", file_type=config.file_type)
                    )
                elif config.adj_list_type == "unordered_by_source":
                    edge.adj_lists.append(
                        AdjList(ordered=False, aligned_by="src", file_type=config.file_type)
                    )
                elif config.adj_list_type == "unordered_by_dest":
                    edge.adj_lists.append(
                        AdjList(ordered=False, aligned_by="dst", file_type=config.file_type)
                    )
                else:
                    msg = "Invalid adj_list_type"
                    raise ValueError(msg)
            else:
                for adj_list in edge.adj_lists:
                    if adj_list.file_type is None:
                        adj_list.file_type = config.file_type
            for property_group in edge.property_groups:
                if property_group.file_type is None:
                    property_group.file_type = config.file_type
            for source in edge.sources:
                if source.file_type is None:
                    source.file_type = config.source_file_type
        return v
