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

package org.apache.graphar

import org.apache.graphar.graph.GraphWriter

class TestGraphWriterSuite extends BaseTestSuite {

  test("write graphs with data frames") {
    // initialize a graph writer
    val writer = new GraphWriter()

    // put the vertex data and edge data into writer
    val vertex_file_path = testData + "/ldbc_sample/person_0_0.csv"
    val vertex_df = spark.read
      .option("delimiter", "|")
      .option("header", "true")
      .csv(vertex_file_path)
    val label = "person"
    writer.PutVertexData(label, vertex_df, "id")

    val file_path = testData + "/ldbc_sample/person_knows_person_0_0.csv"
    val edge_df = spark.read
      .option("delimiter", "|")
      .option("header", "true")
      .csv(file_path)
    val tag = ("person", "knows", "person")
    writer.PutEdgeData(tag, edge_df)

    // conduct writing
    writer.write("/tmp/ldbc", spark, "ldbc")
  }
}
