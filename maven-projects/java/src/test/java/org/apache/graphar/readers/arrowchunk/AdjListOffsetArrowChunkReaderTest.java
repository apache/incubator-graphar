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

import org.apache.graphar.arrow.ArrowArray;
import org.apache.graphar.graphinfo.GraphInfo;
import org.apache.graphar.stdcxx.StdSharedPtr;
import org.apache.graphar.stdcxx.StdString;
import org.apache.graphar.types.AdjListType;
import org.apache.graphar.util.GrapharStaticFunctions;
import org.apache.graphar.util.Result;
import org.junit.Assert;
import org.junit.Test;

public class AdjListOffsetArrowChunkReaderTest {
    @Test
    public void test1() {
        String path = root + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
        Result<StdSharedPtr<GraphInfo>> maybeGraphInfo = GraphInfo.load(path);
        Assert.assertTrue(maybeGraphInfo.status().ok());
        StdSharedPtr<GraphInfo> graphInfo = maybeGraphInfo.value();

        // construct adj list chunk reader
        StdString srcLabel = StdString.create("person");
        StdString edgeLabel = StdString.create("knows");
        StdString dstLabel = StdString.create("person");
        Assert.assertNotNull(graphInfo.get().getEdgeInfo(srcLabel, edgeLabel, dstLabel).get());
        Result<StdSharedPtr<AdjListOffsetArrowChunkReader>> maybeReader =
                GrapharStaticFunctions.INSTANCE.constructAdjListOffsetArrowChunkReader(
                        graphInfo, srcLabel, edgeLabel, dstLabel, AdjListType.ordered_by_source);
        Assert.assertTrue(maybeReader.status().ok());
        AdjListOffsetArrowChunkReader reader = maybeReader.value().get();
        Result<StdSharedPtr<ArrowArray>> result = reader.getChunk();
        Assert.assertTrue(result.status().ok());
        StdSharedPtr<ArrowArray> array = result.value();
        Assert.assertEquals(101, array.get().length());
        Assert.assertTrue(reader.nextChunk().ok());
        result = reader.getChunk();
        Assert.assertTrue(result.status().ok());
        array = result.value();
        Assert.assertEquals(101, array.get().length());

        // seek
        Assert.assertTrue(reader.seek(900).ok());
        result = reader.getChunk();
        Assert.assertTrue(result.status().ok());
        array = result.value();
        Assert.assertEquals(4, array.get().length());
        Assert.assertTrue(reader.nextChunk().isIndexError());
        Assert.assertTrue(reader.seek(1024).isIndexError());
    }
}
