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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import org.apache.graphar.types.AdjListType;
import org.apache.graphar.types.DataType;
import org.apache.graphar.types.FileType;
import org.apache.graphar.util.GrapharStaticFunctions;
import org.apache.graphar.util.YamlUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class EdgeInfoTest {
    private static EdgeInfo teachesEdgeInfo;
    private static JsonNode teachesEdgeInfoRootNode;

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
        String teachesEdgeInfoPath = Paths.get(basePath, "teaches.edge.yml").toString();

        teachesEdgeInfo = GrapharStaticFunctions.INSTANCE.loadEdgeInfo(teachesEdgeInfoPath);
        teachesEdgeInfoRootNode =
                YamlUtil.INSTANCE.loadTree(new FileInputStream(teachesEdgeInfoPath));
    }

    @Test
    public void testLoadTeachesEdgeInfo() {
        assertNotNull(teachesEdgeInfo);
        assertEquals("teaches", teachesEdgeInfo.getEdgeType());
        assertEquals("lecturer", teachesEdgeInfo.getSrcType());
        assertEquals("student", teachesEdgeInfo.getDstType());
        assertEquals("gar/v1", teachesEdgeInfo.getVersion().toString());
        assertEquals("teaches_edge_data/", teachesEdgeInfo.getPrefix());
        assertEquals(200, teachesEdgeInfo.getChunkSize());
        assertEquals(50, teachesEdgeInfo.getSrcChunkSize());
        assertEquals(100, teachesEdgeInfo.getDstChunkSize());
        assertTrue(teachesEdgeInfo.isDirected());
        assertEquals("lecturer_teaches_student", teachesEdgeInfo.getConcatKey());

        List<AdjList> adjLists = teachesEdgeInfo.getAdjLists();
        assertNotNull(adjLists);
        assertEquals(2, adjLists.size());
        assertTrue(teachesEdgeInfo.hasAdjList(AdjListType.ordered_by_source));
        assertTrue(teachesEdgeInfo.hasAdjList(AdjListType.ordered_by_dest));
        assertFalse(teachesEdgeInfo.hasAdjList(AdjListType.unordered_by_source));

        AdjList adjListObs = teachesEdgeInfo.getAdjList(AdjListType.ordered_by_source);
        assertNotNull(adjListObs);
        assertEquals(FileType.PARQUET, adjListObs.getFileType());
        assertEquals(AdjListType.ordered_by_source, adjListObs.getAdjListType());
        assertEquals("adj_src_ordered/", adjListObs.getPrefix());

        List<PropertyGroup> propertyGroups = teachesEdgeInfo.getPropertyGroups();
        assertNotNull(propertyGroups);
        assertEquals(2, propertyGroups.size());

        PropertyGroup pg1 = teachesEdgeInfo.getPropertyGroup("course_id");
        assertNotNull(pg1);
        Property courseIdProp =
                pg1.getProperties().stream()
                        .filter(p -> p.getName().equals("course_id"))
                        .findFirst()
                        .orElse(null);
        assertNotNull(courseIdProp);
        assertEquals("course_id", courseIdProp.getName());
        assertEquals(DataType.Type.STRING, courseIdProp.getDataType().getType());
        assertFalse(courseIdProp.isPrimary());
        assertFalse(courseIdProp.isNullable());
        assertEquals(DataType.Type.STRING, teachesEdgeInfo.getPropertyType("course_id").getType());
        assertFalse(teachesEdgeInfo.isPrimaryKey("course_id"));
        assertFalse(teachesEdgeInfo.isNullableKey("course_id"));

        Property courseYearProp =
                pg1.getProperties().stream()
                        .filter(p -> p.getName().equals("course_year"))
                        .findFirst()
                        .orElse(null);
        assertNotNull(courseYearProp);
        assertEquals("course_year", courseYearProp.getName());
        assertEquals(DataType.Type.INT32, courseYearProp.getDataType().getType());
        assertFalse(courseYearProp.isPrimary());
        assertFalse(courseYearProp.isNullable());
        assertEquals(DataType.Type.INT32, teachesEdgeInfo.getPropertyType("course_year").getType());
        assertFalse(teachesEdgeInfo.isPrimaryKey("course_year"));
        assertFalse(teachesEdgeInfo.isNullableKey("course_year"));

        PropertyGroup pg2 = teachesEdgeInfo.getPropertyGroup("semester");
        assertNotNull(pg2);
        Property semesterProp =
                pg2.getProperties().stream()
                        .filter(p -> p.getName().equals("semester"))
                        .findFirst()
                        .orElse(null);
        assertNotNull(semesterProp);
        assertEquals("semester", semesterProp.getName());
        assertEquals(DataType.Type.STRING, semesterProp.getDataType().getType());
        assertFalse(semesterProp.isPrimary());
        assertTrue(semesterProp.isNullable());
        assertEquals(DataType.Type.STRING, teachesEdgeInfo.getPropertyType("semester").getType());
        assertFalse(teachesEdgeInfo.isPrimaryKey("semester"));
        assertTrue(teachesEdgeInfo.isNullableKey("semester"));

        AdjListType adjType = AdjListType.ordered_by_source;
        assertNotNull(teachesEdgeInfo.getAdjListPathPrefix(adjType));
        assertNotNull(teachesEdgeInfo.getAdjListFilePath(0, 0, adjType));
        assertNotNull(teachesEdgeInfo.getOffsetPathPrefix(adjType));
        assertNotNull(teachesEdgeInfo.getAdjListOffsetFilePath(0, adjType));
        assertNotNull(teachesEdgeInfo.getEdgesNumFilePath(0, adjType));
        assertNotNull(teachesEdgeInfo.getVerticesNumFilePath(adjType));
        assertNotNull(teachesEdgeInfo.getPropertyGroupPathPrefix(pg1, adjType));
        assertNotNull(teachesEdgeInfo.getPropertyFilePath(pg1, adjType, 0, 0));

        String dumpString = teachesEdgeInfo.dump();
        JsonNode dumpedNode = null;
        try {
            dumpedNode = YamlUtil.INSTANCE.loadTree(dumpString);
        } catch (IOException e) {
            fail("Failed to parse dumped string: " + e.getMessage());
        }
        assertNotNull(dumpedNode);
        assertEquals(
                teachesEdgeInfoRootNode.get("edge_type").asText(),
                dumpedNode.get("edge_type").asText());

        assertFalse(teachesEdgeInfo.hasProperty("non_existent_property"));
        assertNull(teachesEdgeInfo.getPropertyGroup("non_existent_property"));
        assertThrows(
                IllegalArgumentException.class,
                () -> teachesEdgeInfo.getPropertyType("non_existent_property"));
        assertThrows(
                IllegalArgumentException.class,
                () -> teachesEdgeInfo.isPrimaryKey("non_existent_property"));
        assertThrows(
                IllegalArgumentException.class,
                () -> teachesEdgeInfo.isNullableKey("non_existent_property"));
    }
}
