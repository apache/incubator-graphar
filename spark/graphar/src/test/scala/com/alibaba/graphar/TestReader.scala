/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Copyright 2022-2023 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphar

import com.alibaba.graphar.reader.{VertexReader, EdgeReader}

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class ReaderSuite extends AnyFunSuite {
  val spark = SparkSession
    .builder()
    .enableHiveSupport()
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("Error")

  test("read chunk files directly") {
    val cond = "id < 1000"
    // read vertex chunk files in Parquet
    val parquet_file_path = "gar-test/ldbc_sample/parquet/"
    val parquet_prefix =
      getClass.getClassLoader.getResource(parquet_file_path).getPath
    val parquet_read_path = parquet_prefix + "vertex/person/id"
    val df1 = spark.read
      .option("fileFormat", "parquet")
      .format("com.alibaba.graphar.datasources.GarDataSource")
      .load(parquet_read_path)
    // validate reading results
    assert(df1.rdd.getNumPartitions == 10)
    assert(df1.count() == 903)
    var df_pd = df1.filter(cond)

    /**
     * ==Physical Plan==
     * (1) Filter (isnotnull(id#0L) AND (id#0L < 1000))
     * +- *(1) ColumnarToRow
     * +- BatchScan[id#0L] GarScan DataFilters: [isnotnull(id#0L), (id#0L <
     * 1000)], Format: gar, Location: InMemoryFileIndex(1
     * paths)[file:/path/to/code/cpp/GraphAr/spark/src/test/resources/gar-test/l...,
     * PartitionFilters: [], PushedFilters: [IsNotNull(id), LessThan(id,1000)],
     * ReadSchema: struct<id:bigint>, PushedFilters: [IsNotNull(id),
     * LessThan(id,1000)] RuntimeFilters: []
     */
    df_pd.explain()
    df_pd.show()

    // read vertex chunk files in Orc
    val orc_file_path = "gar-test/ldbc_sample/orc/"
    val orc_prefix = getClass.getClassLoader.getResource(orc_file_path).getPath
    val orc_read_path = orc_prefix + "vertex/person/id"
    val df2 = spark.read
      .option("fileFormat", "orc")
      .format("com.alibaba.graphar.datasources.GarDataSource")
      .load(orc_read_path)
    // validate reading results
    assert(df2.rdd.collect().deep == df1.rdd.collect().deep)
    df_pd = df1.filter(cond)

    /**
     * ==Physical Plan==
     * (1) Filter (isnotnull(id#0L) AND (id#0L < 1000))
     * +- *(1) ColumnarToRow
     * +- BatchScan[id#0L] GarScan DataFilters: [isnotnull(id#0L), (id#0L <
     * 1000)], Format: gar, Location: InMemoryFileIndex(1
     * paths)[file:/path/to/GraphAr/spark/src/test/resources/gar-test/l...,
     * PartitionFilters: [], PushedFilters: [IsNotNull(id), LessThan(id,1000)],
     * ReadSchema: struct<id:bigint>, PushedFilters: [IsNotNull(id),
     * LessThan(id,1000)] RuntimeFilters: []
     */
    df_pd.explain()
    df_pd.show()

    // read adjList chunk files recursively in CSV
    val csv_file_path = "gar-test/ldbc_sample/csv/"
    val csv_prefix = getClass.getClassLoader.getResource(csv_file_path).getPath
    val csv_read_path =
      csv_prefix + "edge/person_knows_person/ordered_by_source/adj_list"
    val df3 = spark.read
      .option("fileFormat", "csv")
      .option("header", "true")
      .option("recursiveFileLookup", "true")
      .format("com.alibaba.graphar.datasources.GarDataSource")
      .load(csv_read_path)
    // validate reading results
    assert(df3.rdd.getNumPartitions == 11)
    assert(df3.count() == 6626)

    // throw an exception for unsupported file formats
    assertThrows[IllegalArgumentException](
      spark.read
        .option("fileFormat", "invalid")
        .format("com.alibaba.graphar.datasources.GarDataSource")
        .load(csv_read_path)
    )
  }

  test("read vertex chunks") {
    // construct the vertex information
    val file_path = "gar-test/ldbc_sample/parquet/"
    val prefix = getClass.getClassLoader.getResource(file_path).getPath
    val vertex_yaml = getClass.getClassLoader
      .getResource(file_path + "person.vertex.yml")
      .getPath
    val vertex_info = VertexInfo.loadVertexInfo(vertex_yaml, spark)

    // construct the vertex reader
    val reader = new VertexReader(prefix, vertex_info, spark)

    // test reading the number of vertices
    assert(reader.readVerticesNumber() == 903)
    val property_group = vertex_info.getPropertyGroup("gender")

    // test reading a single property chunk
    val single_chunk_df = reader.readVertexPropertyChunk(property_group, 0)
    assert(single_chunk_df.columns.length == 4)
    assert(single_chunk_df.count() == 100)
    val cond = "gender = 'female'"
    var df_pd = single_chunk_df.select("firstName", "gender").filter(cond)

    /**
     * ==Physical Plan==
     * (1) Filter (isnotnull(gender#2) AND (gender#2 = female))
     * +- *(1) ColumnarToRow
     * +- BatchScan[firstName#0, gender#2] GarScan DataFilters:
     * [isnotnull(gender#2), (gender#2 = female)], Format: gar, Location:
     * InMemoryFileIndex(1
     * paths)[file:/path/to/GraphAr/spark/src/test/resources/gar-test/l...,
     * PartitionFilters: [], PushedFilters: [IsNotNull(gender),
     * EqualTo(gender,female)], ReadSchema:
     * struct<firstName:string,gender:string>, PushedFilters:
     * [IsNotNull(gender), EqualTo(gender,female)] RuntimeFilters: []
     */
    df_pd.explain()
    df_pd.show()

    // test reading all chunks for a property group
    val property_df =
      reader.readVertexPropertyGroup(property_group)
    assert(property_df.columns.length == 4)
    assert(property_df.count() == 903)
    df_pd = property_df.select("firstName", "gender").filter(cond)

    /**
     * ==Physical Plan==
     * (1) Filter (isnotnull(gender#31) AND (gender#31 = female))
     * +- *(1) ColumnarToRow
     * +- BatchScan[firstName#29, gender#31] GarScan DataFilters:
     * [isnotnull(gender#31), (gender#31 = female)], Format: gar, Location:
     * InMemoryFileIndex(1
     * paths)[file:/path/to/code/cpp/GraphAr/spark/src/test/resources/gar-test/l...,
     * PartitionFilters: [], PushedFilters: [IsNotNull(gender),
     * EqualTo(gender,female)], ReadSchema:
     * struct<firstName:string,gender:string>, PushedFilters:
     * [IsNotNull(gender), EqualTo(gender,female)] RuntimeFilters: []
     */
    df_pd.explain()
    df_pd.show()

    // test reading chunks for multiple property groups
    val property_group_1 = vertex_info.getPropertyGroup("id")
    val property_groups = new java.util.ArrayList[PropertyGroup]()
    property_groups.add(property_group_1)
    property_groups.add(property_group)
    val multiple_property_df =
      reader.readMultipleVertexPropertyGroups(property_groups)
    assert(multiple_property_df.columns.length == 5)
    assert(multiple_property_df.count() == 903)
    df_pd = multiple_property_df.filter(cond)
    df_pd.explain()
    df_pd.show()
    // test reading chunks for all property groups and optionally adding indices
    val vertex_df = reader.readAllVertexPropertyGroups()
    assert(vertex_df.columns.length == 5)
    assert(vertex_df.count() == 903)
    df_pd = vertex_df.filter(cond)
    df_pd.explain()
    df_pd.show()
    val vertex_df_with_index = reader.readAllVertexPropertyGroups()
    assert(vertex_df_with_index.columns.length == 5)
    assert(vertex_df_with_index.count() == 903)
    df_pd = vertex_df_with_index.filter(cond).select("firstName", "gender")
    df_pd.explain()
    df_pd.show()

    // throw an exception for non-existing property groups
    val invalid_property_group = new PropertyGroup()
    assertThrows[IllegalArgumentException](
      reader.readVertexPropertyChunk(invalid_property_group, 0)
    )
    assertThrows[IllegalArgumentException](
      reader.readVertexPropertyGroup(invalid_property_group)
    )
  }

  test("read edge chunks") {
    // construct the edge information
    val file_path = "gar-test/ldbc_sample/csv/"
    val prefix = getClass.getClassLoader.getResource(file_path).getPath
    val edge_yaml = getClass.getClassLoader
      .getResource(file_path + "person_knows_person.edge.yml")
      .getPath
    val edge_info = EdgeInfo.loadEdgeInfo(edge_yaml, spark)

    // construct the edge reader
    val adj_list_type = AdjListType.ordered_by_source
    val reader = new EdgeReader(prefix, edge_info, adj_list_type, spark)

    // test reading the number of vertices & edges
    assert(reader.readVerticesNumber() == 903)
    assert(reader.readVertexChunkNumber() == 10)
    assert(reader.readEdgesNumber(2) == 1077)
    assert(reader.readEdgesNumber() == 6626)

    // test reading a offset chunk
    val offset_df = reader.readOffset(0)
    assert(offset_df.columns.size == 1)
    assert(offset_df.count() == 101)

    // test reading adjList chunks
    val single_adj_list_df = reader.readAdjListChunk(2, 0)
    assert(single_adj_list_df.columns.size == 2)
    assert(single_adj_list_df.count() == 1024)
    val adj_list_df_chunk_2 = reader.readAdjListForVertexChunk(2, false)
    assert(adj_list_df_chunk_2.columns.size == 2)
    assert(adj_list_df_chunk_2.count() == 1077)
    val adj_list_df = reader.readAllAdjList(false)
    assert(adj_list_df.columns.size == 2)
    assert(adj_list_df.count() == 6626)

    // test reading a single property group
    val property_group =
      edge_info.getPropertyGroup("creationDate")
    val single_property_df = reader.readEdgePropertyChunk(property_group, 2, 0)
    assert(single_property_df.columns.size == 1)
    assert(single_property_df.count() == 1024)
    val property_df_chunk_2 =
      reader.readEdgePropertyGroupForVertexChunk(property_group, 2, false)
    assert(property_df_chunk_2.columns.size == 1)
    assert(property_df_chunk_2.count() == 1077)
    val property_df = reader.readEdgePropertyGroup(property_group, false)
    assert(property_df.columns.size == 1)
    assert(property_df.count() == 6626)

    // test reading multiple property groups
    var property_groups = new java.util.ArrayList[PropertyGroup]()
    property_groups.add(property_group)
    val multiple_property_df_chunk_2 = reader
      .readMultipleEdgePropertyGroupsForVertexChunk(property_groups, 2, false)
    assert(multiple_property_df_chunk_2.columns.size == 1)
    assert(multiple_property_df_chunk_2.count() == 1077)
    val multiple_property_df =
      reader.readMultipleEdgePropertyGroups(property_groups, false)
    assert(multiple_property_df.columns.size == 1)
    assert(multiple_property_df.count() == 6626)

    // test reading all property groups
    val all_property_df_chunk_2 =
      reader.readAllEdgePropertyGroupsForVertexChunk(2, false)
    assert(all_property_df_chunk_2.columns.size == 1)
    assert(all_property_df_chunk_2.count() == 1077)
    val all_property_df = reader.readAllEdgePropertyGroups(false)
    assert(all_property_df.columns.size == 1)
    assert(all_property_df.count() == 6626)

    // test reading edges and optionally adding indices
    val edge_df_chunk_2 = reader.readEdgesForVertexChunk(2, false)
    edge_df_chunk_2.show()
    assert(edge_df_chunk_2.columns.size == 3)
    assert(edge_df_chunk_2.count() == 1077)
    val edge_df_chunk_2_with_index = reader.readEdgesForVertexChunk(2)
    edge_df_chunk_2_with_index.show()
    assert(edge_df_chunk_2_with_index.columns.size == 4)
    assert(edge_df_chunk_2_with_index.count() == 1077)
    val edge_df = reader.readEdges(false)
    edge_df.show()
    assert(edge_df.columns.size == 3)
    assert(edge_df.count() == 6626)
    val edge_df_with_index = reader.readEdges()
    edge_df_with_index.show()
    assert(edge_df_with_index.columns.size == 4)
    assert(edge_df_with_index.count() == 6626)

    // throw an exception for non-existing property groups
    val invalid_property_group = new PropertyGroup()
    assertThrows[IllegalArgumentException](
      reader.readEdgePropertyChunk(invalid_property_group, 0, 0)
    )
    assertThrows[IllegalArgumentException](
      reader.readEdgePropertyGroupForVertexChunk(invalid_property_group, 0)
    )
    assertThrows[IllegalArgumentException](
      reader.readEdgePropertyGroup(invalid_property_group)
    )

    // throw an exception for non-existing adjList types
    val invalid_adj_list_type = AdjListType.unordered_by_dest
    assertThrows[IllegalArgumentException](
      new EdgeReader(prefix, edge_info, invalid_adj_list_type, spark)
    )
  }
}
