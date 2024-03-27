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

"""Enumerations and constants."""

from enum import Enum

from py4j.java_gateway import JavaObject

from graphar_pyspark import GraphArSession, _check_session


class GarType(Enum):
    """Main data type in gar enumeration."""

    BOOL = "bool"
    INT32 = "int32"
    INT64 = "int64"
    FLOAT = "float"
    DOUBLE = "double"
    STRING = "string"
    LIST = "list"

    @staticmethod
    def from_scala(jvm_obj: JavaObject) -> "GarType":
        """Create an instance of the Class from the corresponding JVM object.

        :param jvm_obj: scala object in JVM.
        :returns: instance of Python Class.
        """
        _check_session()
        return GarType(GraphArSession.graphar.GarType.GarTypeToString(jvm_obj))

    def to_scala(self) -> JavaObject:
        """Transform object to JVM representation.

        :returns: JavaObject
        """
        _check_session()
        return GraphArSession.graphar.GarType.StringToGarType(self.value)


class FileType(Enum):
    """Type of  file format."""

    CSV = "csv"
    PARQUET = "parquet"
    ORC = "orc"

    @staticmethod
    def from_scala(jvm_obj: JavaObject) -> "FileType":
        """Create an instance of the Class from the corresponding JVM object.

        :param jvm_obj: scala object in JVM.
        :returns: instance of Python Class.
        """
        _check_session()
        return FileType(GraphArSession.graphar.FileType.FileTypeToString(jvm_obj))

    def to_scala(self) -> JavaObject:
        """Transform object to JVM representation.

        :returns: JavaObject
        """
        _check_session()
        return GraphArSession.graphar.FileType.StringToFileType(self.value)


class AdjListType(Enum):
    """Adj list type enumeration for adjacency list of graph."""

    UNORDERED_BY_SOURCE = "unordered_by_source"
    UNORDERED_BY_DEST = "unordered_by_dest"
    ORDERED_BY_SOURCE = "ordered_by_source"
    ORDERED_BY_DEST = "ordered_by_dest"

    @staticmethod
    def from_scala(jvm_obj: JavaObject) -> "AdjListType":
        """Create an instance of the Class from the corresponding JVM object.

        :param jvm_obj: scala object in JVM.
        :returns: instance of Python Class.
        """
        _check_session()
        return AdjListType(
            GraphArSession.graphar.AdjListType.AdjListTypeToString(jvm_obj),
        )

    def to_scala(self) -> JavaObject:
        """Transform object to JVM representation.

        :returns: JavaObject
        """
        _check_session()
        return GraphArSession.graphar.AdjListType.StringToAdjListType(self.value)
