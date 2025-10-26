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

package org.apache.graphar.datasources.ldbc.bridge

import org.apache.graphar.datasources.ldbc.LdbcTestBase
import java.nio.file.{Files, Paths}

/**
 * Test suite for LdbcGraphArBridge core functionality
 *
 * Tests the main conversion pipeline from LDBC SNB to GraphAr format
 */
class LdbcGraphArBridgeTest extends LdbcTestBase {

  test("should convert SF0.003 dataset successfully") {
    val outputPath = createTempDir()
    val bridge = new LdbcGraphArBridge()

    val result = bridge.write(
      path = outputPath,
      spark = spark,
      name = "test_graph",
      vertex_chunk_size = 100,
      edge_chunk_size = 100,
      file_type = "parquet"
    )

    assert(
      result.isSuccess,
      s"Conversion should succeed, but failed with: ${result.failed.map(_.getMessage).getOrElse("Unknown error")}"
    )

    // Verify graph metadata file exists
    val graphYmlPath = Paths.get(outputPath, "test_graph.graph.yml")
    assert(Files.exists(graphYmlPath), "Graph YAML metadata file should exist")
    assert(
      Files.size(graphYmlPath) > 0,
      "Graph YAML metadata should not be empty"
    )
  }
}
