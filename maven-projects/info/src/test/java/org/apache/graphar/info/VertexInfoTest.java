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
import org.apache.graphar.graphinfo.Property;
import org.apache.graphar.graphinfo.PropertyGroup;
import org.apache.graphar.graphinfo.VertexInfo;
import org.apache.graphar.types.DataType;
import org.apache.graphar.util.GrapharStaticFunctions;
import org.apache.graphar.util.YamlUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class VertexInfoTest {
    private static VertexInfo studentVertexInfo;
    private static VertexInfo lecturerVertexInfo;
    private static JsonNode studentVertexInfoRootNode;
    private static JsonNode lecturerVertexInfoRootNode;

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
        String studentVertexInfoPath = Paths.get(basePath, "student.vertex.yml").toString();
        String lecturerVertexInfoPath = Paths.get(basePath, "lecturer.vertex.yml").toString();

        studentVertexInfo = GrapharStaticFunctions.INSTANCE.loadVertexInfo(studentVertexInfoPath);
        lecturerVertexInfo =
                GrapharStaticFunctions.INSTANCE.loadVertexInfo(lecturerVertexInfoPath);
        studentVertexInfoRootNode =
                YamlUtil.INSTANCE.loadTree(new FileInputStream(studentVertexInfoPath));
        lecturerVertexInfoRootNode =
                YamlUtil.INSTANCE.loadTree(new FileInputStream(lecturerVertexInfoPath));
    }

    @Test
    public void testLoadStudentVertexInfo() {
        assertNotNull(studentVertexInfo);
        assertEquals("student", studentVertexInfo.getType());
        assertEquals("gar/v1", studentVertexInfo.getVersion().toString());
        assertEquals("student_vertex_data/", studentVertexInfo.getPrefix());
        assertEquals(100, studentVertexInfo.getChunkSize());
        assertEquals("student_id", studentVertexInfo.getPrimaryKey());

        // Verify property groups & specific properties
        List<PropertyGroup> propertyGroups = studentVertexInfo.getPropertyGroups();
        assertNotNull(propertyGroups);
        assertEquals(2, propertyGroups.size());

        // Property: student_id (in first property group)
        PropertyGroup pg1 = studentVertexInfo.getPropertyGroup("student_id");
        assertNotNull(pg1);
        Property studentIdProp = pg1.getProperties().stream()
                .filter(p -> p.getName().equals("student_id"))
                .findFirst()
                .orElse(null);
        assertNotNull(studentIdProp);
        assertEquals("student_id", studentIdProp.getName());
        assertEquals(DataType.Type.INT64, studentIdProp.getDataType().getType());
        assertTrue(studentIdProp.isIsPrimary());
        assertFalse(studentIdProp.isIsNullable());
        assertEquals(
                DataType.Type.INT64, studentVertexInfo.getPropertyType("student_id").getType());
        assertTrue(studentVertexInfo.isPrimaryKey("student_id"));
        assertFalse(studentVertexInfo.isNullableKey("student_id"));

        // Property: name (in second property group)
        PropertyGroup pg2 = studentVertexInfo.getPropertyGroup("name");
        assertNotNull(pg2);
        Property nameProp = pg2.getProperties().stream()
                .filter(p -> p.getName().equals("name"))
                .findFirst()
                .orElse(null);
        assertNotNull(nameProp);
        assertEquals("name", nameProp.getName());
        assertEquals(DataType.Type.STRING, nameProp.getDataType().getType());
        assertFalse(nameProp.isIsPrimary());
        assertFalse(nameProp.isIsNullable());
        assertEquals(DataType.Type.STRING, studentVertexInfo.getPropertyType("name").getType());
        assertFalse(studentVertexInfo.isPrimaryKey("name"));
        assertFalse(studentVertexInfo.isNullableKey("name"));

        // Property: major (in second property group)
        Property majorProp = pg2.getProperties().stream()
                .filter(p -> p.getName().equals("major"))
                .findFirst()
                .orElse(null);
        assertNotNull(majorProp);
        assertEquals("major", majorProp.getName());
        assertEquals(DataType.Type.STRING, majorProp.getDataType().getType());
        assertFalse(majorProp.isIsPrimary());
        assertTrue(majorProp.isIsNullable()); // Nullable as per YAML
        assertEquals(DataType.Type.STRING, studentVertexInfo.getPropertyType("major").getType());
        assertFalse(studentVertexInfo.isPrimaryKey("major"));
        assertTrue(studentVertexInfo.isNullableKey("major"));

        // Property: enrollment_year (in second property group)
        Property enrollmentYearProp = pg2.getProperties().stream()
                .filter(p -> p.getName().equals("enrollment_year"))
                .findFirst()
                .orElse(null);
        assertNotNull(enrollmentYearProp);
        assertEquals("enrollment_year", enrollmentYearProp.getName());
        assertEquals(DataType.Type.INT32, enrollmentYearProp.getDataType().getType());
        assertFalse(enrollmentYearProp.isIsPrimary());
        assertFalse(enrollmentYearProp.isIsNullable());
        assertEquals(
                DataType.Type.INT32,
                studentVertexInfo.getPropertyType("enrollment_year").getType());
        assertFalse(studentVertexInfo.isPrimaryKey("enrollment_year"));
        assertFalse(studentVertexInfo.isNullableKey("enrollment_year"));


        // Test dump
        String dumpString = studentVertexInfo.dump();
        JsonNode dumpedNode = null;
        try {
            dumpedNode = YamlUtil.INSTANCE.loadTree(dumpString);
        } catch (IOException e) {
            assertTrue(false, "Failed to parse dumped string: " + e.getMessage());
        }
        assertNotNull(dumpedNode);
        assertEquals(
                studentVertexInfoRootNode.get("type").asText(), dumpedNode.get("type").asText());

        // Test behavior for non-existent property
        assertFalse(studentVertexInfo.hasProperty("non_existent_property"));
        assertNull(studentVertexInfo.getPropertyGroup("non_existent_property"));
        assertThrows(
                IllegalArgumentException.class,
                () -> studentVertexInfo.getPropertyType("non_existent_property"));
        assertThrows(
                IllegalArgumentException.class,
                () -> studentVertexInfo.isPrimaryKey("non_existent_property"));
        assertThrows(
                IllegalArgumentException.class,
                () -> studentVertexInfo.isNullableKey("non_existent_property"));
    }

    @Test
    public void testLoadLecturerVertexInfo() {
        assertNotNull(lecturerVertexInfo);
        assertEquals("lecturer", lecturerVertexInfo.getType());
        assertEquals("gar/v1", lecturerVertexInfo.getVersion().toString());
        assertEquals("lecturer_vertex_data/", lecturerVertexInfo.getPrefix());
        assertEquals(50, lecturerVertexInfo.getChunkSize());
        assertEquals("lecturer_id", lecturerVertexInfo.getPrimaryKey());

        // Verify property groups & specific properties
        List<PropertyGroup> propertyGroups = lecturerVertexInfo.getPropertyGroups();
        assertNotNull(propertyGroups);
        assertEquals(2, propertyGroups.size());

        // Property: lecturer_id
        PropertyGroup pg1Lecturer = lecturerVertexInfo.getPropertyGroup("lecturer_id");
        assertNotNull(pg1Lecturer);
        Property lecturerIdProp = pg1Lecturer.getProperties().stream()
                .filter(p -> p.getName().equals("lecturer_id"))
                .findFirst()
                .orElse(null);
        assertNotNull(lecturerIdProp);
        assertEquals("lecturer_id", lecturerIdProp.getName());
        assertEquals(DataType.Type.INT64, lecturerIdProp.getDataType().getType());
        assertTrue(lecturerIdProp.isIsPrimary());
        assertFalse(lecturerIdProp.isIsNullable());
        assertEquals(
                DataType.Type.INT64,
                lecturerVertexInfo.getPropertyType("lecturer_id").getType());
        assertTrue(lecturerVertexInfo.isPrimaryKey("lecturer_id"));
        assertFalse(lecturerVertexInfo.isNullableKey("lecturer_id"));

        // Property: name
        PropertyGroup pg2Lecturer = lecturerVertexInfo.getPropertyGroup("name");
        assertNotNull(pg2Lecturer);
        Property nameLecturerProp = pg2Lecturer.getProperties().stream()
                .filter(p -> p.getName().equals("name"))
                .findFirst()
                .orElse(null);
        assertNotNull(nameLecturerProp);
        assertEquals("name", nameLecturerProp.getName());
        assertEquals(DataType.Type.STRING, nameLecturerProp.getDataType().getType());
        assertFalse(nameLecturerProp.isIsPrimary());
        assertFalse(nameLecturerProp.isIsNullable());
        assertEquals(
                DataType.Type.STRING, lecturerVertexInfo.getPropertyType("name").getType());
        assertFalse(lecturerVertexInfo.isPrimaryKey("name"));
        assertFalse(lecturerVertexInfo.isNullableKey("name"));

        // Property: email
        Property emailLecturerProp = pg2Lecturer.getProperties().stream()
                .filter(p -> p.getName().equals("email"))
                .findFirst()
                .orElse(null);
        assertNotNull(emailLecturerProp);
        assertEquals("email", emailLecturerProp.getName());
        assertEquals(DataType.Type.STRING, emailLecturerProp.getDataType().getType());
        assertFalse(emailLecturerProp.isIsPrimary());
        assertTrue(emailLecturerProp.isIsNullable()); // Nullable as per YAML
        assertEquals(
                DataType.Type.STRING, lecturerVertexInfo.getPropertyType("email").getType());
        assertFalse(lecturerVertexInfo.isPrimaryKey("email"));
        assertTrue(lecturerVertexInfo.isNullableKey("email"));

        // Property: department
        Property departmentLecturerProp = pg2Lecturer.getProperties().stream()
                .filter(p -> p.getName().equals("department"))
                .findFirst()
                .orElse(null);
        assertNotNull(departmentLecturerProp);
        assertEquals("department", departmentLecturerProp.getName());
        assertEquals(DataType.Type.STRING, departmentLecturerProp.getDataType().getType());
        assertFalse(departmentLecturerProp.isIsPrimary());
        assertFalse(departmentLecturerProp.isIsNullable());
        assertEquals(
                DataType.Type.STRING,
                lecturerVertexInfo.getPropertyType("department").getType());
        assertFalse(lecturerVertexInfo.isPrimaryKey("department"));
        assertFalse(lecturerVertexInfo.isNullableKey("department"));

        // Test dump
        String dumpString = lecturerVertexInfo.dump();
        JsonNode dumpedNode = null;
        try {
            dumpedNode = YamlUtil.INSTANCE.loadTree(dumpString);
        } catch (IOException e) {
            assertTrue(false, "Failed to parse dumped string: " + e.getMessage());
        }
        assertNotNull(dumpedNode);
        assertEquals(
                lecturerVertexInfoRootNode.get("type").asText(), dumpedNode.get("type").asText());

        // Test behavior for non-existent property
        assertFalse(lecturerVertexInfo.hasProperty("non_existent_property"));
        assertNull(lecturerVertexInfo.getPropertyGroup("non_existent_property"));
        assertThrows(
                IllegalArgumentException.class,
                () -> lecturerVertexInfo.getPropertyType("non_existent_property"));
        assertThrows(
                IllegalArgumentException.class,
                () -> lecturerVertexInfo.isPrimaryKey("non_existent_property"));
        assertThrows(
                IllegalArgumentException.class,
                () -> lecturerVertexInfo.isNullableKey("non_existent_property"));
    }
}
