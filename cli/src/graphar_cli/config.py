from typing import Dict, List, Literal, Optional

from pydantic import BaseModel


class GraphArConfig(BaseModel):
    path: str
    name: str
    vertexChunkSize: int = 100
    edgeChunkSize: int = 1024
    fileType: Literal["parquet", "orc", "csv", "json"] = "parquet"
    adjListType: Literal[
        "ordered_by_source", "ordered_by_dest", "unordered_by_source", "unordered_by_dest"
    ] = "ordered_by_source"


class Property(BaseModel):
    name: str
    type: Literal["bool", "int32", "int64", "float", "double", "string", "date", "timestamp"]
    nullable: bool = False


class Source(BaseModel):
    type: Literal["parquet", "orc", "csv", "json"] = "csv"
    path: str
    delimiter: str = ","
    columns: Dict[str, str]


class Vertex(BaseModel):
    type: str
    labels: List[str]
    primary: str
    properties: List[Property]
    propertyGroups: List[List[str]] = []
    source: Source


class Edge(BaseModel):
    type: str
    srcType: str
    srcProp: str
    dstType: str
    dstProp: str
    properties: List[Property] = []
    propertyGroups: List[List[str]] = []
    source: Source


class SchemaConfig(BaseModel):
    vertices: List[Vertex]
    edges: List[Edge]


class ImportConfig(BaseModel):
    graphar: GraphArConfig
    import_schema: SchemaConfig
