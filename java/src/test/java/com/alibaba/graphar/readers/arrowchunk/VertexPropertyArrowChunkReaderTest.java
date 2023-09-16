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

package com.alibaba.graphar.readers.arrowchunk;

import com.alibaba.graphar.arrow.ArrowTable;
import com.alibaba.graphar.graphinfo.GraphInfo;
import com.alibaba.graphar.graphinfo.PropertyGroup;
import com.alibaba.graphar.stdcxx.StdPair;
import com.alibaba.graphar.stdcxx.StdSharedPtr;
import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.util.GrapharStaticFunctions;
import com.alibaba.graphar.util.Result;
import org.junit.Assert;
import org.junit.Test;

import static com.alibaba.graphar.graphinfo.GraphInfoTest.root;

public class VertexPropertyArrowChunkReaderTest {
  @Test
  public void test1() {
    String path = root + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
    Result<GraphInfo> maybeGraphInfo = GraphInfo.load(path);
    Assert.assertTrue(maybeGraphInfo.status().ok());
    GraphInfo graphInfo = maybeGraphInfo.value();

    // construct vertex chunk reader
    StdString label = StdString.create("person");
    StdString propertyName = StdString.create("id");
    Assert.assertTrue(graphInfo.getVertexInfo(label).status().ok());
    Result<PropertyGroup> maybeGroup = graphInfo.getVertexPropertyGroup(label, propertyName);
    Assert.assertTrue(maybeGroup.status().ok());
    PropertyGroup group = maybeGroup.value();
    Result<VertexPropertyArrowChunkReader> maybeReader =
            GrapharStaticFunctions.INSTANCE.constructVertexPropertyArrowChunkReader(
                    graphInfo, label, group);
    Assert.assertTrue(maybeReader.status().ok());
    VertexPropertyArrowChunkReader reader = maybeReader.value();
    Result<StdSharedPtr<ArrowTable>> result = reader.getChunk();
    Assert.assertTrue(result.status().ok());
    StdPair<Long, Long> range = reader.getRange().value();
    StdSharedPtr<ArrowTable> table = result.value();
    Assert.assertEquals(100, table.get().num_rows());
    Assert.assertEquals(0, (long) range.getFirst());
    Assert.assertEquals(100, (long) range.getSecond());

    // seek
    Assert.assertTrue(reader.seek(100).ok());
    result = reader.getChunk();
    Assert.assertTrue(result.status().ok());
    range = reader.getRange().value();
    table = result.value();
    Assert.assertEquals(100, table.get().num_rows());
    Assert.assertEquals(100, (long) range.getFirst());
    Assert.assertEquals(200, (long) range.getSecond());
    Assert.assertTrue(reader.nextChunk().ok());
    result = reader.getChunk();
    Assert.assertTrue(result.status().ok());
    range = reader.getRange().value();
    table = result.value();
    Assert.assertEquals(100, table.get().num_rows());
    Assert.assertEquals(200, (long) range.getFirst());
    Assert.assertEquals(300, (long) range.getSecond());
    Assert.assertTrue(reader.seek(900).ok());
    result = reader.getChunk();
    Assert.assertTrue(result.status().ok());
    range = reader.getRange().value();
    table = result.value();
    Assert.assertEquals(3, table.get().num_rows());
    Assert.assertEquals(900, (long) range.getFirst());
    Assert.assertEquals(903, (long) range.getSecond());
    Assert.assertEquals(10, reader.getChunkNum());
    Assert.assertTrue(reader.nextChunk().isIndexError());

    Assert.assertTrue(reader.seek(1024).isIndexError());


  }
}
