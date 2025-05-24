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
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.FileInputStream;
import java.io.IOException;
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
        graphInfo = GrapharStaticFunctions.INSTANCE.loadGraphInfo(graphInfoPath);
        graphInfoRootNode = YamlUtil.INSTANCE.loadTree(new FileInputStream(graphInfoPath));
    }

    @Test
    public void testLoadGraphInfo() {
        assertNotNull(graphInfo);
        assertEquals("academic_graph", graphInfo.getName());
        assertNotNull(graphInfo.getVersion());
        assertEquals("gar/v1", graphInfo.getVersion().toString());
        assertEquals("gar_test_data/", graphInfo.getPrefix());

        List<VertexInfo> vertexInfos = graphInfo.getVertexInfos();
        assertNotNull(vertexInfos);
        assertEquals(2, vertexInfos.size());
        assertTrue(vertexInfos.stream().anyMatch(vi -> "student".equals(vi.getType())));
        assertTrue(vertexInfos.stream().anyMatch(vi -> "lecturer".equals(vi.getType())));

        List<EdgeInfo> edgeInfos = graphInfo.getEdgeInfos();
        assertNotNull(edgeInfos);
        assertEquals(1, edgeInfos.size());
        assertTrue(
                edgeInfos.stream()
                        .anyMatch(ei -> "lecturer_teaches_student".equals(ei.getConcatKey())));

        Map<String, String> extraInfo = graphInfo.getExtraInfo();
        assertNotNull(extraInfo);
        assertEquals(2, extraInfo.size());
        assertEquals("A simple academic graph for testing.", extraInfo.get("description"));
        assertEquals("2023-10-27", extraInfo.get("creation_date"));

        VertexInfo studentInfo = graphInfo.getVertexInfo("student");
        assertNotNull(studentInfo);
        assertEquals("student", studentInfo.getType());

        EdgeInfo teachesInfo = graphInfo.getEdgeInfo("lecturer", "teaches", "student");
        assertNotNull(teachesInfo);
        assertEquals("teaches", teachesInfo.getEdgeType());

        String dumpString = graphInfo.dump();
        JsonNode dumpedNode = null;
        try {
            dumpedNode = YamlUtil.INSTANCE.loadTree(dumpString);
        } catch (IOException e) {
            fail("Failed to parse dumped string: " + e.getMessage());
        }
        assertNotNull(dumpedNode);
        assertEquals(graphInfoRootNode.get("name").asText(), dumpedNode.get("name").asText());
        assertEquals(graphInfoRootNode.get("prefix").asText(), dumpedNode.get("prefix").asText());
        assertEquals(graphInfoRootNode.get("version").asText(), dumpedNode.get("version").asText());
    }
}
