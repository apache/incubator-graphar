/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphar.writers;

import com.alibaba.graphar.arrow.ArrowTable;
import com.alibaba.graphar.graphinfo.VertexInfo;
import com.alibaba.graphar.stdcxx.StdSharedPtr;
import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.types.ValidateLevel;
import com.alibaba.graphar.util.Yaml;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.Assert;
import org.junit.Test;

import static com.alibaba.graphar.graphinfo.GraphInfoTest.root;

public class VertexPropertyWriterTest {
  @Test
  public void test1() {
    String uri = "file:" + root + "/ldbc_sample/person_0_0_comma.csv";
    ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
    StdSharedPtr<ArrowTable> table = null;
    try (BufferAllocator allocator = new RootAllocator();
         DatasetFactory datasetFactory =
                 new FileSystemDatasetFactory(
                         allocator, NativeMemoryPool.getDefault(), FileFormat.CSV, uri);
         Dataset dataset = datasetFactory.finish();
         Scanner scanner = dataset.newScan(options);
         ArrowReader reader = scanner.scanBatches()) {
      reader.loadNextBatch();
      try (VectorSchemaRoot vectorSchemaRoot = reader.getVectorSchemaRoot()) {
        table = ArrowTable.fromVectorSchemaRoot(allocator, vectorSchemaRoot, reader);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    String vertexMetaFile = root + "/ldbc_sample/parquet/" + "person.vertex.yml";
    StdSharedPtr<Yaml> vertexMeta = Yaml.loadFile(StdString.create(vertexMetaFile)).value();
    VertexInfo vertexInfo = VertexInfo.load(vertexMeta).value();
    Assert.assertEquals("person", vertexInfo.getLabel().toJavaString());
    VertexPropertyWriter writer = VertexPropertyWriter.factory.create(vertexInfo, StdString.create("/tmp/"));

    // Get & set validate level
    Assert.assertEquals(ValidateLevel.no_validate, writer.getValidateLevel());
    writer.setValidateLevel(ValidateLevel.strong_validate);
    Assert.assertEquals(ValidateLevel.strong_validate, writer.getValidateLevel());

    // Valid cases
    // Write the table
    Assert.assertTrue(writer.writeTable(table, 0).ok());
    // Write the number of vertices
    Assert.assertTrue(writer.writeVerticesNum(table.get().num_rows()).ok());

    // Invalid cases
    // Invalid vertices number
    Assert.assertTrue(writer.writeVerticesNum(-1).isInvalid());
    // Out of range
    Assert.assertTrue(writer.writeChunk(table, 0, writer.getValidateLevel()).isInvalid());
  }
}
