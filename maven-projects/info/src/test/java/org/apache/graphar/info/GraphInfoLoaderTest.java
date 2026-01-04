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

package org.apache.graphar.info;

import java.io.IOException;
import java.net.URI;
import org.apache.graphar.info.loader.GraphInfoLoader;
import org.apache.graphar.info.loader.impl.LocalFileSystemReaderGraphInfoLoader;
import org.apache.graphar.info.loader.impl.LocalFileSystemStreamGraphInfoLoader;
import org.apache.graphar.info.loader.impl.LocalFileSystemStringGraphInfoLoader;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class GraphInfoLoaderTest {

    @BeforeClass
    public static void init() {
        TestUtil.checkTestData();
    }

    @AfterClass
    public static void clean() {}

    @Test
    public void testStringLoader() throws IOException {
        final URI GRAPH_PATH_URI = TestUtil.getCSVLdbcSampleGraphURI();
        GraphInfoLoader loader = new LocalFileSystemStringGraphInfoLoader();
        final GraphInfo graphInfo = loader.loadGraphInfo(GRAPH_PATH_URI);
        testGraphInfo(graphInfo);
    }

    @Test
    public void testStreamLoader() throws IOException {
        final URI GRAPH_PATH_URI = TestUtil.getCSVLdbcSampleGraphURI();
        GraphInfoLoader loader = new LocalFileSystemStreamGraphInfoLoader();
        final GraphInfo graphInfo = loader.loadGraphInfo(GRAPH_PATH_URI);
        testGraphInfo(graphInfo);
    }

    @Test
    public void testReaderLoader() throws IOException {
        final URI GRAPH_PATH_URI = TestUtil.getCSVLdbcSampleGraphURI();
        GraphInfoLoader loader = new LocalFileSystemReaderGraphInfoLoader();
        final GraphInfo graphInfo = loader.loadGraphInfo(GRAPH_PATH_URI);
        testGraphInfo(graphInfo);
    }

    private void testGraphInfo(GraphInfo graphInfo) {
        Assert.assertNotNull(graphInfo);
        Assert.assertNotNull(graphInfo.getEdgeInfos());
        Assert.assertEquals(1, graphInfo.getEdgeInfos().size());
        for (EdgeInfo edgeInfo : graphInfo.getEdgeInfos()) {
            Assert.assertNotNull(edgeInfo.getConcat());
        }
        Assert.assertNotNull(graphInfo.getVertexInfos());
        Assert.assertEquals(1, graphInfo.getVertexInfos().size());
        for (VertexInfo vertexInfo : graphInfo.getVertexInfos()) {
            Assert.assertNotNull(vertexInfo.getType());
        }
    }
}
