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
 * Show the graphar information.
 */
@Command(
  name = "show",
  mixinStandardHelpOptions = true,
  description = Array("Show the GraphAr information.")
)
class Show extends Callable[Int] {
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

//  TODO(ljj): Support show more information and more options.

  def call(): Int = {
    val console: Logger = parent.console
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()
    val graphInfo = GraphInfo.loadGraphInfo(path, spark)
    console.info(graphInfo.dump())
    spark.close()
    1
  }
}
