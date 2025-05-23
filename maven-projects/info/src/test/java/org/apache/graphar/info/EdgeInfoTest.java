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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import org.apache.graphar.graphinfo.EdgeInfo;
import org.apache.graphar.graphinfo.Property;
import org.apache.graphar.graphinfo.PropertyGroup;
import org.apache.graphar.types.AdjListType;
import org.apache.graphar.types.DataType;
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

        // Verify adjacent lists
        assertTrue(teachesEdgeInfo.hasAdjList(AdjListType.ordered_by_source));
        assertTrue(teachesEdgeInfo.hasAdjList(AdjListType.ordered_by_dest));
        assertFalse(teachesEdgeInfo.hasAdjList(AdjListType.unordered_by_source));
        // ... (other adj list tests are fine and already present)

        // Verify property groups & specific properties
        List<PropertyGroup> propertyGroups = teachesEdgeInfo.getPropertyGroups();
        assertNotNull(propertyGroups);
        assertEquals(2, propertyGroups.size());

        // Property: course_id (in first property group)
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
        assertFalse(courseIdProp.isIsPrimary()); // Edge properties not primary
        assertFalse(courseIdProp.isIsNullable());
        assertEquals(DataType.Type.STRING, teachesEdgeInfo.getPropertyType("course_id").getType());
        assertFalse(teachesEdgeInfo.isPrimaryKey("course_id"));
        assertFalse(teachesEdgeInfo.isNullableKey("course_id"));

        // Property: course_year (in first property group)
        Property courseYearProp =
                pg1.getProperties().stream()
                        .filter(p -> p.getName().equals("course_year"))
                        .findFirst()
                        .orElse(null);
        assertNotNull(courseYearProp);
        assertEquals("course_year", courseYearProp.getName());
        assertEquals(DataType.Type.INT32, courseYearProp.getDataType().getType());
        assertFalse(courseYearProp.isIsPrimary());
        assertFalse(courseYearProp.isIsNullable());
        assertEquals(DataType.Type.INT32, teachesEdgeInfo.getPropertyType("course_year").getType());
        assertFalse(teachesEdgeInfo.isPrimaryKey("course_year"));
        assertFalse(teachesEdgeInfo.isNullableKey("course_year"));

        // Property: semester (in second property group)
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
        assertFalse(semesterProp.isIsPrimary());
        assertTrue(semesterProp.isIsNullable()); // Nullable as per YAML
        assertEquals(DataType.Type.STRING, teachesEdgeInfo.getPropertyType("semester").getType());
        assertFalse(teachesEdgeInfo.isPrimaryKey("semester"));
        assertTrue(teachesEdgeInfo.isNullableKey("semester"));

        // Test dump
        String dumpString = teachesEdgeInfo.dump();
        JsonNode dumpedNode = null;
        try {
            dumpedNode = YamlUtil.INSTANCE.loadTree(dumpString);
        } catch (IOException e) {
            assertTrue(false, "Failed to parse dumped string: " + e.getMessage());
        }
        assertNotNull(dumpedNode);
        assertEquals(
                teachesEdgeInfoRootNode.get("edge_type").asText(),
                dumpedNode.get("edge_type").asText());

        // Test behavior for non-existent property
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
