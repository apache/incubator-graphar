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

import org.apache.graphar.GraphInfo
import org.apache.graphar.cli.Main
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger
import picocli.CommandLine.{Command, Option, ParentCommand}

import java.util.concurrent.Callable

/**
 * Check the graphar meta information.
 */
@Command(
  name = "show",
  mixinStandardHelpOptions = true,
  description = Array("Check the graphar meta information")
)
class Check extends Callable[Int] {
  @ParentCommand
  private var parent: Main = _

  @Option(
    names = Array("-p", "--path"),
    description = Array(
      "The path of the YAML file."
    ),
    required = true
  )
  private var path: String = _

//  TODO(ljj): Support check all the meta information and files.
//  @Option(
//    names = Array("-a", "--all"),
//    description = Array(
//      "Check all the meta information and files."
//    )
//  )
//  private var checkALL: Boolean = false

  def call(): Int = {
    val console: Logger = parent.console
    val spark = SparkSession.builder
      .appName("GraphArCli")
      .master("local[*]")
      .getOrCreate()

    try {
      val graphInfo = GraphInfo.loadGraphInfo(path, spark)
      val vertexInfos = graphInfo.getVertexInfos()
      for ((vertexLabel, vertexInfo) <- vertexInfos) {
        if (!vertexInfo.isValidated()) {
          console.error(s"VertexInfo ${vertexLabel} is not validated.")
          return 0
        }
        console.info(s"VertexInfo ${vertexLabel} is validated.")
      }
      val vertexLabels = vertexInfos.keys.toList
      val edgeInfos = graphInfo.getEdgeInfos()
      for ((concatKeys, edgeInfo) <- edgeInfos) {
        if (!edgeInfo.isValidated()) {
          console.error(s"EdgeInfo ${concatKeys} is not validated.")
          return 0
        }
        console.info(s"EdgeInfo ${concatKeys} is validated.")
        val srcVertexLabel = edgeInfo.src_label
        if (!vertexLabels.contains(srcVertexLabel)) {
          console.error(
            s"The src_label ${srcVertexLabel} of the edge ${concatKeys} does not exist."
          )
          return 0
        }
        val dstVertexLabel = edgeInfo.dst_label
        if (!vertexLabels.contains(dstVertexLabel)) {
          console.error(
            s"The dst_label ${dstVertexLabel} of the edge ${concatKeys} does not exist."
          )
          return 0
        }
      }
      console.info("All vertexInfos and edgeInfos are validated.")
      spark.close()
      1
    } catch {
      case e: Exception =>
        console.error(s"Check failed: ${e.getMessage}")
        0
    }

  }
}
