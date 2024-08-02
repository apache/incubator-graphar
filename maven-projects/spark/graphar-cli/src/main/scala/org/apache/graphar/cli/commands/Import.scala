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

package org.apache.graphar.cli.commands

import org.apache.graphar.cli.Main
import org.apache.graphar.cli.util.Config
import org.apache.graphar.cli.util.Utils.parseStringToSparkType
import org.apache.graphar.graph.GraphWriter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}
import org.json4s.{DefaultFormats, JValue}
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.Logger
import picocli.CommandLine.{Command, Option, ParentCommand}

import java.util.concurrent.Callable
import scala.io.Source

/**
 * Read the configuration file and import data.
 */
@Command(
  name = "import",
  mixinStandardHelpOptions = true,
  description = Array("Show the GraphAr information.")
)
class Import extends Callable[Int] {
  @ParentCommand
  private var parent: Main = _
  @Option(
    names = Array("-c", "--config"),
    description = Array(
      "The path of the import config file."
    ),
    required = true
  )
  private var confPath: String = _

  def call(): Int = {
    val console: Logger = parent.console
    val source = Source.fromFile(confPath)
    val confString =
      try source.mkString
      finally source.close()
    val confJson: JValue = parse(confString)

//  TODO: validate the config file
    implicit val formats: DefaultFormats.type = DefaultFormats
    val config = (confJson \ "graphar").extract[Config]
    console.info("Config read successfully.")

    config.sourceType match {
      case "csv" =>
        val spark = SparkSession.builder
          .appName("GraphAr Cli")
          .master("local[*]")
          .getOrCreate()
        val writer: GraphWriter = new GraphWriter()
        val schema = config.schema

        for (vertex <- schema.vertices) {
          val source = vertex.source
          val dfSchema = StructType(
            vertex.properties.map(property =>
              StructField(
                property.name,
                parseStringToSparkType(property.`type`),
                nullable = property.nullable.getOrElse(true)
              )
            )
          )
          val vertex_df = spark.read
            .option("header", "true")
            .schema(dfSchema)
            .option("delimiter", source.delimiter)
            .csv(source.path.get)

          writer.PutVertexData(
            vertex.label,
            vertex_df,
            primaryKey = vertex.primary
          )
        }

        for (edge <- schema.edges) {
          val source = edge.source
          val srcStructField = StructField(
            edge.srcProp,
            parseStringToSparkType(
              schema.vertices
                .find(_.label == edge.srcLabel)
                .get
                .properties
                .find(_.name == edge.srcProp)
                .get
                .`type`
            )
          )
          val dstStructField = StructField(
            edge.dstProp,
            parseStringToSparkType(
              schema.vertices
                .find(_.label == edge.dstLabel)
                .get
                .properties
                .find(_.name == edge.dstProp)
                .get
                .`type`
            )
          )
          val dfSchema = StructType(
            srcStructField +:
              dstStructField +:
              edge.properties.map(property =>
                StructField(
                  property.name,
                  parseStringToSparkType(property.`type`),
                  nullable = property.nullable.getOrElse(true)
                )
              )
          )
          val edge_df = spark.read
            .option("header", "true")
            .schema(dfSchema)
            .option("delimiter", source.delimiter)
            .csv(source.path.get)

          writer.PutEdgeData(
            (edge.srcLabel, edge.label, edge.dstLabel),
            edge_df
          )
        }

    }

    val spark = SparkSession.builder
      .appName("GraphAr Cli")
      .master("local[*]")
      .getOrCreate()
    spark.close()
    1
  }
}
