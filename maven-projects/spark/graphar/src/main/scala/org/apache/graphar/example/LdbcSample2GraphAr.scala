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

package org.apache.graphar.example

import org.apache.graphar.graph.GraphWriter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{
  IntegerType,
  StringType,
  StructField,
  StructType
}

object LdbcSample2GraphAr {

  def main(args: Array[String]): Unit = {
    // initialize a Spark session
    val spark = SparkSession
      .builder()
      .appName("LdbcSample Data to GraphAr")
      .config("spark.master", "local")
      .getOrCreate()

    // initialize a graph writer
    val writer: GraphWriter = new GraphWriter()

    // person vertex csv input path
    val personInputPath: String = args(0)
    // person_knows_person edge csv input path
    val personKnowsPersonInputPath: String = args(1)
    // output directory
    val outputPath: String = args(2)
    // vertex chunk size
    val vertexChunkSize: Long = args(3).toLong
    // edge chunk size
    val edgeChunkSize: Long = args(4).toLong
    // file type
    val fileType: String = args(5)

    // put Ldbc sample graph data into writer
    readAndPutDataIntoWriter(
      writer,
      spark,
      personInputPath,
      personKnowsPersonInputPath
    )

    // write in GraphAr format
    writer.write(
      outputPath,
      spark,
      "LdbcSample",
      vertexChunkSize,
      edgeChunkSize,
      fileType
    )
  }

  // read data from local ldbc sample csv files and put into writer
  def readAndPutDataIntoWriter(
      writer: GraphWriter,
      spark: SparkSession,
      personInputPath: String,
      personKnowsPersonInputPath: String
  ): Unit = {
    // read vertices with label "Person" from given path as a DataFrame
    val person_df = spark.read
      .option("delimiter", "|")
      .option("header", "true")
      .option("inferSchema", "true")
      .format("csv")
      .load(personInputPath)
    // put into writer, vertex label is "Person"
    writer.PutVertexData("Person", person_df)

    // read edges with type "Person"->"Knows"->"Person" from given path as a DataFrame
    val knows_edge_df = spark.read
      .option("delimiter", "|")
      .option("header", "true")
      .option("inferSchema", "true")
      .format("csv")
      .load(personKnowsPersonInputPath)
    // put into writer, source vertex label is "Person", edge label is "Knows"
    // target vertex label is "Person"
    writer.PutEdgeData(("Person", "Knows", "Person"), knows_edge_df)
  }
}
