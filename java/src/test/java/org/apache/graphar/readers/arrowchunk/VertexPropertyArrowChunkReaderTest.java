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

package org.apache.graphar.readers.arrowchunk;

import static org.apache.graphar.graphinfo.GraphInfoTest.root;

import org.apache.graphar.arrow.ArrowTable;
import org.apache.graphar.graphinfo.GraphInfo;
import org.apache.graphar.graphinfo.PropertyGroup;
import org.apache.graphar.stdcxx.StdSharedPtr;
import org.apache.graphar.stdcxx.StdString;
import org.apache.graphar.util.GrapharStaticFunctions;
import org.apache.graphar.util.Result;
import org.junit.Assert;
import org.junit.Test;

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
        StdSharedPtr<ArrowTable> table = result.value();
        Assert.assertEquals(100, table.get().num_rows());

        // seek
        Assert.assertTrue(reader.seek(100).ok());
        result = reader.getChunk();
        Assert.assertTrue(result.status().ok());
        table = result.value();
        Assert.assertEquals(100, table.get().num_rows());
        Assert.assertTrue(reader.nextChunk().ok());
        result = reader.getChunk();
        Assert.assertTrue(result.status().ok());
        table = result.value();
        Assert.assertEquals(100, table.get().num_rows());
        Assert.assertTrue(reader.seek(900).ok());
        result = reader.getChunk();
        Assert.assertTrue(result.status().ok());
        table = result.value();
        Assert.assertEquals(3, table.get().num_rows());
        Assert.assertEquals(10, reader.getChunkNum());
        Assert.assertTrue(reader.nextChunk().isIndexError());

        Assert.assertTrue(reader.seek(1024).isIndexError());
    }
}
