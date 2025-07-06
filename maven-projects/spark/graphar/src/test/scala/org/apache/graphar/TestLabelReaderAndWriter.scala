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

import org.apache.graphar.graph.GraphReader.readVertexWithLabels
import org.apache.graphar.reader.VertexReader
import org.apache.graphar.writer.VertexWriter

class LabelReaderAndWriterSuite extends BaseTestSuite {

  test("read vertices with labels from parquet file") {
    val prefix = testData + "/ldbc/parquet/"
    val vertex_yaml = prefix + "organisation.vertex.yml"
    val vertex_info = VertexInfo.loadVertexInfo(vertex_yaml, spark)
    val frame = readVertexWithLabels(prefix, vertex_info, spark)
    frame.select(GeneralParams.kLabelCol).show()
  }

  test("write vertices with labels to parquet file") {
    // read vertex DataFrame
    val prefix = testData + "/ldbc/parquet/"
    val vertex_yaml = prefix + "organisation.vertex.yml"
    val vertex_info = VertexInfo.loadVertexInfo(vertex_yaml, spark)
    val frame = readVertexWithLabels(prefix, vertex_info, spark)
    frame.show()
    val output_prefix: String = "/tmp/"
    val writer = new VertexWriter(output_prefix, vertex_info, frame)
    writer.writeVertexLabels()
    val reader = new VertexReader(output_prefix, vertex_info, spark)
    val frame2 = reader.readVertexLabels().select(GeneralParams.kLabelCol)
    frame2.show()
    val frame1 = frame.select(GeneralParams.kLabelCol)
    val diff1 = frame1.except(frame2)
    val diff2 = frame2.except(frame1)
    assert(diff1.isEmpty)
    assert(diff2.isEmpty)
  }
}
