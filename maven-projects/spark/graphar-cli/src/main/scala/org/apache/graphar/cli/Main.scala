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

package org.apache.graphar.cli

import org.apache.commons.logging.LogFactory
import org.apache.graphar.cli.commands.{Check, Import, Show}
import org.apache.log4j.PropertyConfigurator
import org.slf4j.{Logger, LoggerFactory}
import picocli.CommandLine
import picocli.CommandLine.Command

import java.util.concurrent.Callable

/**
 * Main class for the GraphAr Cli Tool.
 */
@Command(
  name = "main",
  subcommands = Array(classOf[Show], classOf[Check], classOf[Import]),
  mixinStandardHelpOptions = true,
  description = Array("GraphAr Cli Tool")
)
class Main extends Callable[Int] {
  val console: Logger = LoggerFactory.getLogger(classOf[Main])
  def call: Int = {
    console.info(
      "GraphAr Cli Tool. Use -h or --help to see available commands."
    )
    1
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure(
      classOf[Main].getResource("/log4j.properties")
    )
    LogFactory.getFactory.setAttribute(
      "org.apache.commons.logging.Log",
      "org.apache.commons.logging.impl.Log4JLogger"
    )
    val exitCode = new CommandLine(new Main()).execute(args: _*)
    System.exit(exitCode)
  }
}
