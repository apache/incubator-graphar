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

from graphar_pyspark import initialize
from graphar_pyspark.enums import GarType, FileType, AdjListType


def test_gar_type(spark):
    initialize(spark)

    gar_string = GarType.STRING
    gar_int = GarType.INT32
    gar_long = GarType.INT64
    gar_float = GarType.FLOAT
    gar_double = GarType.DOUBLE
    gar_bool = GarType.BOOL
    gar_list = GarType.LIST

    assert gar_string == GarType.from_scala(gar_string.to_scala())
    assert gar_int == GarType.from_scala(gar_int.to_scala())
    assert gar_long == GarType.from_scala(gar_long.to_scala())
    assert gar_float == GarType.from_scala(gar_float.to_scala())
    assert gar_double == GarType.from_scala(gar_double.to_scala())
    assert gar_bool == GarType.from_scala(gar_bool.to_scala())
    assert gar_list == GarType.from_scala(gar_list.to_scala())

def test_file_type(spark):
    initialize(spark)

    file_type_csv = FileType.CSV
    file_type_orc = FileType.ORC
    file_type_parquet = FileType.PARQUET

    assert file_type_csv == FileType.from_scala(file_type_csv.to_scala())
    assert file_type_orc == FileType.from_scala(file_type_orc.to_scala())
    assert file_type_parquet == FileType.from_scala(file_type_parquet.to_scala())


def test_adj_list_type(spark):
    initialize(spark)

    ordered_by_dest = AdjListType.ORDERED_BY_DEST
    ordered_by_src = AdjListType.ORDERED_BY_SOURCE
    unordered_by_dest = AdjListType.UNORDERED_BY_DEST
    unordered_by_src = AdjListType.UNORDERED_BY_SOURCE

    assert ordered_by_dest == AdjListType.from_scala(ordered_by_dest.to_scala())
    assert ordered_by_src == AdjListType.from_scala(ordered_by_src.to_scala())
    assert unordered_by_dest == AdjListType.from_scala(unordered_by_dest.to_scala())
    assert unordered_by_src == AdjListType.from_scala(unordered_by_src.to_scala())
