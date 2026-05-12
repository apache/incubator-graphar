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

import com.alibaba.fastffi.CXXReference;
import org.apache.graphar.graphinfo.GraphInfo;
import org.apache.graphar.graphinfo.PropertyGroup;
import org.apache.graphar.graphinfo.VertexInfo;
import org.apache.graphar.stdcxx.StdSharedPtr;
import org.apache.graphar.stdcxx.StdString;
import org.apache.graphar.util.GrapharStaticFunctions;
import org.apache.graphar.util.Result;
import org.junit.Assert;
import org.junit.Test;

public class VertexPropertyChunkInfoReaderTest {
    @Test
    public void test1() {
        // read file and construct graph info
        String path = root + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
        Result<StdSharedPtr<GraphInfo>> maybeGraphInfo = GraphInfo.load(path);
        Assert.assertTrue(maybeGraphInfo.status().ok());
        StdSharedPtr<GraphInfo> graphInfo = maybeGraphInfo.value();
        Assert.assertEquals(1, graphInfo.get().getVertexInfos().size());
        Assert.assertEquals(1, graphInfo.get().getEdgeInfos().size());

        // construct vertex property info reader
        StdString label = StdString.create("person");
        StdString propertyName = StdString.create("id");
        StdSharedPtr<@CXXReference VertexInfo> vertexInfoStdSharedPtr =
                graphInfo.get().getVertexInfo(label);
        Assert.assertNotNull(vertexInfoStdSharedPtr.get());
        StdSharedPtr<PropertyGroup> groupStdSharedPtr =
                vertexInfoStdSharedPtr.get().getPropertyGroup(propertyName);
        Assert.assertNotNull(groupStdSharedPtr.get());
        PropertyGroup group = groupStdSharedPtr.get();
        StdSharedPtr<PropertyGroup> groupPtr =
                GrapharStaticFunctions.INSTANCE.createPropertyGroup(
                        group.getProperties(), group.getFileType(), group.getPrefix());
        Result<StdSharedPtr<VertexPropertyChunkInfoReader>> maybeReader =
                GrapharStaticFunctions.INSTANCE.constructVertexPropertyChunkInfoReader(
                        graphInfo, label, groupPtr);
        Assert.assertFalse(maybeReader.hasError());
        VertexPropertyChunkInfoReader reader = maybeReader.value().get();

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
