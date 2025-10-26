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

package org.apache.graphar.datasources.ldbc.integration

import org.apache.graphar.datasources.ldbc.LdbcTestBase
import org.apache.graphar.datasources.ldbc.bridge.LdbcGraphArBridge
import java.nio.file.{Files, Paths}

/**
 * Integration test suite for GraphAr output validation
 *
 * Tests end-to-end conversion and validates output structure
 */
class OutputValidationTest extends LdbcTestBase {

  test("should generate valid GraphAr directory structure") {
    val outputPath = createTempDir()
    val bridge = new LdbcGraphArBridge()

    bridge.write(outputPath, spark, "ldbc_test", 256, 256, "parquet")

    // Verify basic directory structure
    assert(Files.exists(Paths.get(outputPath, "vertex")), "vertex directory should exist")
    assert(Files.exists(Paths.get(outputPath, "edge")), "edge directory should exist")

    // Verify graph metadata
    val graphYmlPath = Paths.get(outputPath, "ldbc_test.graph.yml")
    assert(Files.exists(graphYmlPath), "graph metadata file should exist")
    assert(Files.size(graphYmlPath) > 0, "graph metadata should not be empty")

    // Verify Person vertex directory exists (core entity)
    val personVertexPath = Paths.get(outputPath, "vertex", "Person")
    assert(Files.exists(personVertexPath), "Person vertex directory should exist")
  }
}
