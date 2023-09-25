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

import com.alibaba.graphar.arrow.ArrowTable;
import com.alibaba.graphar.graphinfo.GraphInfo;
import com.alibaba.graphar.graphinfo.PropertyGroup;
import com.alibaba.graphar.stdcxx.StdSharedPtr;
import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.types.AdjListType;
import com.alibaba.graphar.util.GrapharStaticFunctions;
import com.alibaba.graphar.util.Result;
import org.junit.Assert;
import org.junit.Test;

public class AdjListPropertyArrowChunkReaderTest {
    @Test
    public void test1() {
        String path = root + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
        Result<GraphInfo> maybeGraphInfo = GraphInfo.load(path);
        Assert.assertTrue(maybeGraphInfo.status().ok());
        GraphInfo graphInfo = maybeGraphInfo.value();

        StdString srcLabel = StdString.create("person");
        StdString edgeLabel = StdString.create("knows");
        StdString dstLabel = StdString.create("person");
        StdString propertyName = StdString.create("creationDate");
        Result<PropertyGroup> maybeGroup =
                graphInfo.getEdgePropertyGroup(
                        srcLabel, edgeLabel, dstLabel, propertyName, AdjListType.ordered_by_source);
        Assert.assertTrue(maybeGroup.status().ok());
        PropertyGroup group = maybeGroup.value();
        Result<AdjListPropertyArrowChunkReader> maybeReader =
                GrapharStaticFunctions.INSTANCE.constructAdjListPropertyArrowChunkReader(
                        graphInfo,
                        srcLabel,
                        edgeLabel,
                        dstLabel,
                        group,
                        AdjListType.ordered_by_source);
        Assert.assertTrue(maybeReader.status().ok());
        AdjListPropertyArrowChunkReader reader = maybeReader.value();
        Result<StdSharedPtr<ArrowTable>> result = reader.getChunk();
        Assert.assertTrue(result.status().ok());
        StdSharedPtr<ArrowTable> table = result.value();
        Assert.assertEquals(667, table.get().num_rows());

        // seek
        Assert.assertTrue(reader.seek(100).ok());
        result = reader.getChunk();
        Assert.assertTrue(result.status().ok());
        table = result.value();
        Assert.assertEquals(567, table.get().num_rows());
        Assert.assertTrue(reader.nextChunk().ok());
        result = reader.getChunk();
        Assert.assertTrue(result.status().ok());
        table = result.value();
        Assert.assertEquals(644, table.get().num_rows());
        Assert.assertTrue(reader.seek(1024).isIndexError());

        // seek src & dst
        Assert.assertTrue(reader.seekSrc(100).ok());
        result = reader.getChunk();
        Assert.assertTrue(result.status().ok());
        table = result.value();
        Assert.assertEquals(644, table.get().num_rows());
        Assert.assertFalse(reader.seekDst(100).ok());

        Assert.assertTrue(reader.seekSrc(900).ok());
        result = reader.getChunk();
        Assert.assertTrue(result.status().ok());
        table = result.value();
        Assert.assertEquals(4, table.get().num_rows());

        Assert.assertTrue(reader.nextChunk().isIndexError());
    }
}
