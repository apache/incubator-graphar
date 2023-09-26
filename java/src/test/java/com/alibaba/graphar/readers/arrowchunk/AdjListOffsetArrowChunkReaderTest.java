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

import static com.alibaba.graphar.graphinfo.GraphInfoTest.root;

import com.alibaba.graphar.arrow.ArrowArray;
import com.alibaba.graphar.graphinfo.GraphInfo;
import com.alibaba.graphar.stdcxx.StdSharedPtr;
import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.types.AdjListType;
import com.alibaba.graphar.util.GrapharStaticFunctions;
import com.alibaba.graphar.util.Result;
import org.junit.Assert;
import org.junit.Test;

public class AdjListOffsetArrowChunkReaderTest {
    @Test
    public void test1() {
        String path = root + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
        Result<GraphInfo> maybeGraphInfo = GraphInfo.load(path);
        Assert.assertTrue(maybeGraphInfo.status().ok());
        GraphInfo graphInfo = maybeGraphInfo.value();

        // construct adj list chunk reader
        StdString srcLabel = StdString.create("person");
        StdString edgeLabel = StdString.create("knows");
        StdString dstLabel = StdString.create("person");
        Assert.assertTrue(graphInfo.getEdgeInfo(srcLabel, edgeLabel, dstLabel).status().ok());
        Result<AdjListOffsetArrowChunkReader> maybeReader =
                GrapharStaticFunctions.INSTANCE.constructAdjListOffsetArrowChunkReader(
                        graphInfo, srcLabel, edgeLabel, dstLabel, AdjListType.ordered_by_source);
        Assert.assertTrue(maybeReader.status().ok());
        AdjListOffsetArrowChunkReader reader = maybeReader.value();
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
