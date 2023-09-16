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

package com.alibaba.graphar.readers.chunkinfo;

import static com.alibaba.graphar.graphinfo.GraphInfoTest.root;

import com.alibaba.graphar.graphinfo.GraphInfo;
import com.alibaba.graphar.graphinfo.PropertyGroup;
import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.util.GrapharStaticFunctions;
import com.alibaba.graphar.util.Result;
import org.junit.Assert;
import org.junit.Test;

public class VertexPropertyChunkInfoReaderTest {
    @Test
    public void test1() {
        // read file and construct graph info
        String path = root + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
        Result<GraphInfo> maybeGraphInfo = GraphInfo.load(path);
        Assert.assertTrue(maybeGraphInfo.status().ok());
        GraphInfo graphInfo = maybeGraphInfo.value();
        Assert.assertEquals(1, graphInfo.getVertexInfos().size());
        Assert.assertEquals(1, graphInfo.getEdgeInfos().size());

        // construct vertex property info reader
        StdString label = StdString.create("person");
        StdString propertyName = StdString.create("id");
        Assert.assertTrue(graphInfo.getVertexInfo(label).status().ok());
        Result<PropertyGroup> maybeGroup = graphInfo.getVertexPropertyGroup(label, propertyName);
        Assert.assertFalse(maybeGroup.hasError());
        PropertyGroup group = maybeGroup.value();
        Result<VertexPropertyChunkInfoReader> maybeReader =
                GrapharStaticFunctions.INSTANCE.constructVertexPropertyChunkInfoReader(
                        graphInfo, label, group);
        Assert.assertFalse(maybeReader.hasError());
        VertexPropertyChunkInfoReader reader = maybeReader.value();

        // get chunk file path & validate
        Result<StdString> maybeChunkPath = reader.getChunk();
        Assert.assertTrue(maybeChunkPath.status().ok());
        StdString chunkPath = maybeChunkPath.value();
        Assert.assertEquals(
                root + "/ldbc_sample/parquet/vertex/person/id/chunk0", chunkPath.toJavaString());
        Assert.assertTrue(reader.seek(520).ok());
        maybeChunkPath = reader.getChunk();
        Assert.assertTrue(maybeChunkPath.status().ok());
        chunkPath = maybeChunkPath.value();
        Assert.assertEquals(
                root + "/ldbc_sample/parquet/vertex/person/id/chunk5", chunkPath.toJavaString());
        Assert.assertTrue(reader.nextChunk().ok());
        maybeChunkPath = reader.getChunk();
        Assert.assertTrue(maybeChunkPath.status().ok());
        chunkPath = maybeChunkPath.value();
        Assert.assertEquals(
                root + "/ldbc_sample/parquet/vertex/person/id/chunk6", chunkPath.toJavaString());
        Assert.assertTrue(reader.seek(900).ok());
        maybeChunkPath = reader.getChunk();
        Assert.assertTrue(maybeChunkPath.status().ok());
        chunkPath = maybeChunkPath.value();
        Assert.assertEquals(
                root + "/ldbc_sample/parquet/vertex/person/id/chunk9", chunkPath.toJavaString());
        // now is end of the chunks
        Assert.assertTrue(reader.nextChunk().isIndexError());

        // test seek the id not in the chunks
        Assert.assertTrue(reader.seek(100000).isIndexError());

        // test Get vertex property chunk num through vertex property chunk info
        // reader
        Assert.assertEquals(10, reader.getChunkNum());
    }
}
