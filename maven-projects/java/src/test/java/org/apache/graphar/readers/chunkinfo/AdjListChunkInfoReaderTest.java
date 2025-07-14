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

package org.apache.graphar.readers.chunkinfo;

import static org.apache.graphar.graphinfo.GraphInfoTest.root;

import org.apache.graphar.graphinfo.GraphInfo;
import org.apache.graphar.stdcxx.StdSharedPtr;
import org.apache.graphar.stdcxx.StdString;
import org.apache.graphar.types.AdjListType;
import org.apache.graphar.util.GrapharStaticFunctions;
import org.apache.graphar.util.Result;
import org.junit.Assert;
import org.junit.Test;

public class AdjListChunkInfoReaderTest {
    @Test
    public void test1() {
        // read file and construct graph info
        String path = root + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
        Result<StdSharedPtr<GraphInfo>> maybeGraphInfo = GraphInfo.load(path);
        Assert.assertTrue(maybeGraphInfo.status().ok());
        StdSharedPtr<GraphInfo> graphInfo = maybeGraphInfo.value();
        Assert.assertEquals(1, graphInfo.get().getVertexInfos().size());
        Assert.assertEquals(1, graphInfo.get().getEdgeInfos().size());

        // construct adj list info reader
        StdString srcLabel = StdString.create("person");
        StdString edgeLabel = StdString.create("knows");
        StdString dstLabel = StdString.create("person");
        Result<StdSharedPtr<AdjListChunkInfoReader>> maybeReader =
                GrapharStaticFunctions.INSTANCE.constructAdjListChunkInfoReader(
                        graphInfo, srcLabel, edgeLabel, dstLabel, AdjListType.ordered_by_source);
        Assert.assertTrue(maybeGraphInfo.status().ok());
        AdjListChunkInfoReader reader = maybeReader.value().get();

        // get chunk file path & validate
        Result<StdString> maybeChunkPath = reader.getChunk();
        Assert.assertTrue(maybeChunkPath.status().ok());
        StdString chunkPath = maybeChunkPath.value();
        Assert.assertEquals(
                root
                        + "/ldbc_sample/parquet/edge/person_knows_person/"
                        + "ordered_by_source/adj_list/part0/chunk0",
                chunkPath.toJavaString());
        Assert.assertTrue(reader.seek(100).ok());
        maybeChunkPath = reader.getChunk();
        Assert.assertTrue(maybeChunkPath.status().ok());
        chunkPath = maybeChunkPath.value();
        Assert.assertEquals(
                root
                        + "/ldbc_sample/parquet/edge/person_knows_person/"
                        + "ordered_by_source/adj_list/part0/chunk0",
                chunkPath.toJavaString());
        Assert.assertTrue(reader.nextChunk().ok());
        maybeChunkPath = reader.getChunk();
        Assert.assertTrue(maybeChunkPath.status().ok());
        chunkPath = maybeChunkPath.value();
        Assert.assertEquals(
                root
                        + "/ldbc_sample/parquet/edge/person_knows_person/"
                        + "ordered_by_source/adj_list/part1/chunk0",
                chunkPath.toJavaString());
        Assert.assertTrue(reader.nextChunk().ok());

        // seek_src & seek_dst
        Assert.assertTrue(reader.seekSrc(100).ok());
        maybeChunkPath = reader.getChunk();
        Assert.assertTrue(maybeChunkPath.status().ok());
        chunkPath = maybeChunkPath.value();
        Assert.assertEquals(
                root
                        + "/ldbc_sample/parquet/edge/person_knows_person/"
                        + "ordered_by_source/adj_list/part1/chunk0",
                chunkPath.toJavaString());
        Assert.assertTrue(reader.seekSrc(900).ok());
        maybeChunkPath = reader.getChunk();
        Assert.assertTrue(maybeChunkPath.status().ok());
        chunkPath = maybeChunkPath.value();
        Assert.assertEquals(
                root
                        + "/ldbc_sample/parquet/edge/person_knows_person/"
                        + "ordered_by_source/adj_list/part9/chunk0",
                chunkPath.toJavaString());

        // seek an invalid src id
        Assert.assertTrue(reader.seekSrc(1000).isIndexError());
        Assert.assertTrue(reader.seekDst(100).isInvalid());

        // test reader to read ordered by dest
        Result<StdSharedPtr<AdjListChunkInfoReader>> maybeDstReader =
                GrapharStaticFunctions.INSTANCE.constructAdjListChunkInfoReader(
                        graphInfo, srcLabel, edgeLabel, dstLabel, AdjListType.ordered_by_dest);
        Assert.assertTrue(maybeDstReader.status().ok());
        AdjListChunkInfoReader dstReader = maybeDstReader.value().get();
        Assert.assertTrue(dstReader.seekDst(100).ok());
        maybeChunkPath = dstReader.getChunk();
        Assert.assertTrue(maybeChunkPath.status().ok());
        chunkPath = maybeChunkPath.value();
        Assert.assertEquals(
                root
                        + "/ldbc_sample/parquet/edge/person_knows_person/"
                        + "ordered_by_dest/adj_list/part1/chunk0",
                chunkPath.toJavaString());

        // seek an invalid dst id
        Assert.assertTrue(dstReader.seekDst(1000).isIndexError());
        Assert.assertTrue(dstReader.seekSrc(100).isInvalid());
    }
}
