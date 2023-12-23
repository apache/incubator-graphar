"""
Copyright 2022-2023 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from enum import Enum

from py4j.java_gateway import JavaObject

from graphar_pysaprk import GraphArSession


class GarType(Enum):
    """Main data type in gar enumeration."""
    BOOL = "bool"
    INT32 = "int32"
    INT64 = "int64"
    FLOAT = "float"
    DOUBLE = "double"
    STRING = "string"
    ARRAY = "array"

    @staticmethod
    def from_scala(jvm_obj: JavaObject) -> "GarType":
        return GarType(GraphArSession._graphar.GarType.GarTypeToString(jvm_obj))

    def to_scala(self) -> JavaObject:
        return GraphArSession._graphar.GarType.StringToGarType(self.value)


class FileType(Enum):
    """Type of  file format."""
    CSV = "csv"
    PARQUET = "parquet"
    ORC = "orc"

    @staticmethod
    def from_scala(jvm_obj: JavaObject) -> "FileType":
        return FileType(GraphArSession._graphar.FileType.FileTypeToString(jvm_obj))

    def to_scala(self) -> JavaObject:
        return GraphArSession._graphar.FileType.StringToFileType(self.value)


class AdjListType(Enum):
    """Adj list type enumeration for adjacency list of graph."""
    UNORDERED_BY_SOURCE = "unordered_by_source"
    UNORDERED_BY_DEST = "unordered_by_dest"
    ORDERED_BY_SOURCE = "ordered_by_source"
    ORDERED_BY_DEST = "ordered_by_dest"

    @staticmethod
    def from_scala(jvm_obj: JavaObject) -> "AdjListType":
        return AdjListType(GraphArSession._graphar.AdjListType.AdjListTypeToString(jvm_obj))

    def to_scala(self) -> JavaObject:
        return GraphArSession._graphar.AdjListType.StringToAdjListType(self.value)
