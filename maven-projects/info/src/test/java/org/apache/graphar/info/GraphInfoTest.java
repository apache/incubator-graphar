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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.apache.graphar.util.GrapharStaticFunctions;
import org.apache.graphar.util.YamlUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class GraphInfoTest {
    private static GraphInfo graphInfo;
    private static JsonNode graphInfoRootNode;

    @BeforeAll
    public static void setUp() throws IOException {
        String basePath =
                Paths.get(
                                System.getProperty("user.dir"),
                                "..",
                                "info",
                                "src",
                                "test",
                                "resources",
                                "gar_test_data")
                        .normalize()
                        .toString();
        String graphInfoPath = Paths.get(basePath, "graph.info.yml").toString();
        graphInfo = GraphInfo.loadGraphInfo(graphInfoPath);
        graphInfoRootNode = YamlUtil.INSTANCE.loadTree(new FileInputStream(graphInfoPath));
    }

    @Test
    public void testLoadGraphInfo() {
        assertNotNull(graphInfo);
        assertEquals("academic_graph", graphInfo.getName());
        assertEquals("gar/v1", graphInfo.getVersion().toString());
        assertEquals("gar_test_data/", graphInfo.getPrefix());

        // Verify vertex info file paths
        List<String> vertexInfoPaths = graphInfo.getVertexInfos();
        assertNotNull(vertexInfoPaths);
        assertEquals(2, vertexInfoPaths.size());
        assertTrue(vertexInfoPaths.contains("student.vertex.yml"));
        assertTrue(vertexInfoPaths.contains("lecturer.vertex.yml"));

        // Verify edge info file paths
        List<String> edgeInfoPaths = graphInfo.getEdgeInfos();
        assertNotNull(edgeInfoPaths);
        assertEquals(1, edgeInfoPaths.size());
        assertTrue(edgeInfoPaths.contains("teaches.edge.yml"));

        // Verify extra_info
        Map<String, String> extraInfo = graphInfo.getExtraInfo();
        assertNotNull(extraInfo);
        assertEquals(2, extraInfo.size());
        assertEquals("A simple academic graph for testing.", extraInfo.get("description"));
        assertEquals("2023-10-27", extraInfo.get("creation_date"));

        // Test getVertexInfo & getEdgeInfo methods
        assertNotNull(graphInfo.getVertexInfo("student"));
        assertEquals("student", graphInfo.getVertexInfo("student").getType());
        assertNotNull(graphInfo.getEdgeInfo("lecturer", "teaches", "student"));
        assertEquals(
                "teaches",
                graphInfo.getEdgeInfo("lecturer", "teaches", "student").getEdgeType());

        // Test dump
        String dumpString = graphInfo.dump();
        JsonNode dumpedNode = null;
        try {
            dumpedNode = YamlUtil.INSTANCE.loadTree(dumpString);
        } catch (IOException e) {
            assertTrue(false, "Failed to parse dumped string: " + e.getMessage());
        }
        assertNotNull(dumpedNode);
        assertEquals(graphInfoRootNode.get("name").asText(), dumpedNode.get("name").asText());
        assertEquals(
                graphInfoRootNode.get("prefix").asText(), dumpedNode.get("prefix").asText());
        assertEquals(
                graphInfoRootNode.get("version").asText(), dumpedNode.get("version").asText());
    }
}
