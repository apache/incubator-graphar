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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import org.apache.graphar.info.loader.GraphLoader;
import org.apache.graphar.info.loader.LocalYamlGraphLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class GraphLoaderTest {

    @BeforeAll
    public static void init() {
        TestUtil.checkTestData();
    }

    @AfterAll
    public static void clean() {}

    @Test
    public void testLoad() {
        final GraphLoader graphLoader = new LocalYamlGraphLoader();
        final String GRAPH_PATH = TestUtil.getLdbcSampleGraphPath();
        try {
            final GraphInfo graphInfo = graphLoader.load(GRAPH_PATH);
            assertNotNull(graphInfo);
            assertNotNull(graphInfo.getEdgeInfos());
            assertEquals(1, graphInfo.getEdgeInfos().size());
            for (EdgeInfo edgeInfo : graphInfo.getEdgeInfos()) {
                assertNotNull(edgeInfo.getConcat());
            }
            assertNotNull(graphInfo.getVertexInfos());
            assertEquals(1, graphInfo.getVertexInfos().size());
            for (VertexInfo vertexInfo : graphInfo.getVertexInfos()) {
                assertNotNull(vertexInfo.getType());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
