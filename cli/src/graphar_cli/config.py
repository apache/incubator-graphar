from logging import getLogger
from pathlib import Path
from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, field_validator, model_validator
from typing_extensions import Self

logger = getLogger("graphar_cli")

# TODO: convert to constants
default_file_type = "parquet"
default_adj_list_type = "ordered_by_source"
default_regular_separator = "_"
default_validate_level = "weak"
default_version = "gar/v1"
support_file_types = {"parquet", "orc", "csv", "json"}


class GraphArConfig(BaseModel):
    path: str
    name: str
    vertex_chunk_size: Optional[int] = 100
    edge_chunk_size: Optional[int] = 1024
    file_type: Literal["parquet", "orc", "csv", "json"] = default_file_type
    adj_list_type: Literal[
        "ordered_by_source", "ordered_by_dest", "unordered_by_source", "unordered_by_dest"
    ] = default_adj_list_type
    validate_level: Literal["no", "weak", "strong"] = default_validate_level
    version: Optional[str] = default_version

    @field_validator("path")
    def check_path(cls, v):
        path = Path(v)
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
        elif any(path.iterdir()):
            msg = f"Warning: Path {v} already exists and contains files."
            logger.warning(msg)
        return v


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

    @model_validator(mode="after")
    def check_file_type(self) -> Self:
        if not self.file_type:
            file_type = Path(self.path).suffix.removeprefix(".")
            if file_type == "":
                msg = f"File {self.path} has no file type suffix"
                raise ValueError(msg)
            if file_type not in support_file_types:
                msg = f"File type {file_type} not supported"
                raise ValueError(msg)
            self.file_type = file_type
        return self


class Vertex(BaseModel):
    type: str
    labels: List[str] = []
    chunk_size: Optional[int] = None
    validate_level: Optional[Literal["no", "weak", "strong"]] = None
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
    validate_level: Optional[Literal["no", "weak", "strong"]] = None
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

    @model_validator(mode="after")
    def check_none_types(self) -> Self:
        for vertex in self.import_schema.vertices:
            if vertex.chunk_size is None:
                vertex.chunk_size = self.graphar.vertex_chunk_size
            if vertex.validate_level is None:
                vertex.validate_level = self.graphar.validate_level
            for property_group in vertex.property_groups:
                if property_group.file_type is None:
                    property_group.file_type = self.graphar.file_type
        for edge in self.import_schema.edges:
            if edge.chunk_size is None:
                edge.chunk_size = self.graphar.edge_chunk_size
            if edge.validate_level is None:
                edge.validate_level = self.graphar.validate_level
            if len(edge.adj_lists) == 0:
                if self.graphar.adj_list_type == "ordered_by_source":
                    edge.adj_lists.append(
                        AdjList(ordered=True, aligned_by="src", file_type=self.graphar.file_type)
                    )
                elif self.graphar.adj_list_type == "ordered_by_dest":
                    edge.adj_lists.append(
                        AdjList(ordered=True, aligned_by="dst", file_type=self.graphar.file_type)
                    )
                elif self.graphar.adj_list_type == "unordered_by_source":
                    edge.adj_lists.append(
                        AdjList(ordered=False, aligned_by="src", file_type=self.graphar.file_type)
                    )
                elif self.graphar.adj_list_type == "unordered_by_dest":
                    edge.adj_lists.append(
                        AdjList(ordered=False, aligned_by="dst", file_type=self.graphar.file_type)
                    )
                else:
                    msg = f"Invalid adj_list_type '{self.graphar.adj_list_type}'"
                    raise ValueError(msg)
            for property_group in edge.property_groups:
                if property_group.file_type is None:
                    property_group.file_type = self.graphar.file_type

        return self
