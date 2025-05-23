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
import java.util.List;
import org.apache.graphar.info.loader.LocalYamlGraphLoader;
import org.apache.graphar.proto.AdjListType;
import org.apache.graphar.proto.DataType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class InfoTest {

    @BeforeClass
    public static void init() {
        TestUtil.checkTestData();
    }

    @Test
    public void testLoadGraphInfo() {
        String graphInfoPath = TestUtil.getLdbcSampleGraphPath();
        GraphInfo graphInfo = null;
        try {
            graphInfo = new LocalYamlGraphLoader().load(graphInfoPath);
        } catch (IOException e) {
            Assert.fail("Failed to load GraphInfo: " + e.getMessage());
        }

        Assert.assertNotNull(graphInfo);
        Assert.assertEquals("ldbc_sample", graphInfo.getName());
        // The prefix field is absent in ldbc_sample.graph.yml.
        // graphInfoPath is /path/to/GAR_TEST_DATA/ldbc_sample/csv/ldbc_sample.graph.yml
        // So the absolute prefix for GraphInfo should be /path/to/GAR_TEST_DATA/ldbc_sample/csv/
        String expectedPrefix = graphInfoPath.substring(0, graphInfoPath.lastIndexOf("/") + 1);
        Assert.assertEquals(expectedPrefix, graphInfo.getPrefix());
        // Assert.assertEquals("gar/v1", graphInfo.getVersion()); // No getVersion() method in
        // GraphInfo

        Assert.assertNotNull(graphInfo.getVertexInfos());
        Assert.assertEquals(1, graphInfo.getVertexInfos().size());
        Assert.assertNotNull(graphInfo.getEdgeInfos());
        Assert.assertEquals(1, graphInfo.getEdgeInfos().size());

        VertexInfo vertexInfo = graphInfo.getVertexInfos().get(0);
        Assert.assertSame(vertexInfo, graphInfo.getVertexInfo("person"));
        Assert.assertEquals("person", vertexInfo.getType());

        EdgeInfo edgeInfo = graphInfo.getEdgeInfos().get(0);
        Assert.assertSame(edgeInfo, graphInfo.getEdgeInfo("person", "knows", "person"));
        Assert.assertEquals("person_knows_person", edgeInfo.getConcat());
    }

    @Test
    public void testLoadVertexInfo() throws IOException {
        String vertexYamlPath = TestUtil.getTestData() + "/ldbc_sample/csv/person.vertex.yml";
        // Load VertexInfo using the new static method
        VertexInfo vertexInfo = VertexInfo.loadVertexInfo(vertexYamlPath);

        Assert.assertNotNull(vertexInfo);
        Assert.assertEquals("person", vertexInfo.getType());
        Assert.assertEquals(100, vertexInfo.getChunkSize());
        Assert.assertEquals("vertex/person/", vertexInfo.getPrefix());
        Assert.assertEquals("gar/v1", vertexInfo.getVersion()); // Assert version

        // Property "id" is the primary key
        // Let's find the primary key by iterating through properties, as getPrimaryKey() is not
        // directly on VertexInfo
        String primaryKey = "";
        for (PropertyGroup pg : vertexInfo.getPropertyGroups()) {
            for (org.apache.graphar.info.Property p : pg.getPropertyList()) { // Changed
                if (p.isPrimary()) { // Changed
                    primaryKey = p.getName();
                    break;
                }
            }
            if (!primaryKey.isEmpty()) {
                break;
            }
        }
        Assert.assertEquals("id", primaryKey);

        List<PropertyGroup> propertyGroups =
                vertexInfo.getPropertyGroups(); // Still useful for iterating if needed or checking
        // size
        Assert.assertNotNull(propertyGroups);
        Assert.assertEquals(2, propertyGroups.size());

        // Use the new getPropertyGroup method
        PropertyGroup pg1 = vertexInfo.getPropertyGroup("id");
        Assert.assertNotNull(pg1);
        Assert.assertEquals(1, pg1.getPropertyList().size()); // Changed
        Assert.assertEquals("id", pg1.getPropertyList().get(0).getName()); // Changed
        Assert.assertEquals(org.apache.graphar.proto.FileType.CSV, pg1.getFileType()); // Changed
        Assert.assertEquals("id/", pg1.getPrefix()); // Prefix of the property group itself
        Assert.assertTrue(vertexInfo.hasProperty("id"));

        PropertyGroup pg2 =
                vertexInfo.getPropertyGroup("firstName"); // Can get by any property in the group
        Assert.assertNotNull(pg2);
        // Also test getting by another property in the same group
        Assert.assertSame(pg2, vertexInfo.getPropertyGroup("lastName"));
        Assert.assertSame(pg2, vertexInfo.getPropertyGroup("gender"));

        Assert.assertEquals(3, pg2.getPropertyList().size()); // Changed
        Assert.assertEquals(org.apache.graphar.proto.FileType.CSV, pg2.getFileType()); // Changed
        Assert.assertEquals("firstName_lastName_gender/", pg2.getPrefix());
        Assert.assertTrue(vertexInfo.hasProperty("firstName"));
        Assert.assertTrue(vertexInfo.hasProperty("lastName"));
        Assert.assertTrue(vertexInfo.hasProperty("gender"));

        // Test property attributes using vertexInfo (remains the same)
        Assert.assertEquals(DataType.INT64, vertexInfo.getPropertyType("id"));
        Assert.assertTrue(vertexInfo.isPrimaryKey("id"));
        Assert.assertFalse(vertexInfo.isNullableKey("id"));

        Assert.assertEquals(DataType.STRING, vertexInfo.getPropertyType("firstName"));
        Assert.assertFalse(vertexInfo.isPrimaryKey("firstName"));
        Assert.assertFalse(
                vertexInfo.isNullableKey("firstName")); // As per TestUtil programmatic definition

        Assert.assertEquals(DataType.STRING, vertexInfo.getPropertyType("gender"));
        Assert.assertFalse(vertexInfo.isPrimaryKey("gender"));
        Assert.assertTrue(
                vertexInfo.isNullableKey("gender")); // As per TestUtil programmatic definition

        Assert.assertEquals("vertex/person/vertex_count", vertexInfo.getVerticesNumFilePath());

        // For the first property group ("id")
        Assert.assertEquals(
                "vertex/person/id/chunk0", vertexInfo.getPropertyGroupChunkPath(pg1, 0));
        Assert.assertEquals("vertex/person/id/", vertexInfo.getPropertyGroupPrefix(pg1));

        // For the second property group ("firstName_lastName_gender")
        Assert.assertEquals(
                "vertex/person/firstName_lastName_gender/chunk0",
                vertexInfo.getPropertyGroupChunkPath(pg2, 0));
        Assert.assertEquals(
                "vertex/person/firstName_lastName_gender/", vertexInfo.getPropertyGroupPrefix(pg2));
    }

    @Test
    public void testLoadEdgeInfo() throws IOException {
        String edgeYamlPath =
                TestUtil.getTestData() + "/ldbc_sample/csv/person_knows_person.edge.yml";
        EdgeInfo edgeInfo = EdgeInfo.loadEdgeInfo(edgeYamlPath);

        Assert.assertNotNull(edgeInfo);
        Assert.assertEquals("person", edgeInfo.getSrcLabel());
        Assert.assertEquals("knows", edgeInfo.getEdgeLabel());
        Assert.assertEquals("person", edgeInfo.getDstLabel());
        Assert.assertEquals(1024, edgeInfo.getChunkSize());
        Assert.assertEquals(100, edgeInfo.getSrcChunkSize());
        Assert.assertEquals(100, edgeInfo.getDstChunkSize());
        Assert.assertFalse(edgeInfo.isDirected());
        Assert.assertEquals("edge/person_knows_person/", edgeInfo.getPrefix());
        Assert.assertEquals("gar/v1", edgeInfo.getVersion());

        Assert.assertNotNull(edgeInfo.getAdjacentLists());
        Assert.assertEquals(2, edgeInfo.getAdjacentLists().size());

        // Ordered By Source
        AdjacentList adjListSrc = edgeInfo.getAdjacentList(AdjListType.ORDERED_BY_SOURCE);
        Assert.assertNotNull(adjListSrc);
        Assert.assertEquals(
                org.apache.graphar.proto.AdjListType.ORDERED_BY_SOURCE, adjListSrc.getType());
        Assert.assertEquals(
                org.apache.graphar.proto.FileType.CSV, adjListSrc.getFileType()); // Changed
        Assert.assertEquals("ordered_by_source/", adjListSrc.getPrefix());
        Assert.assertTrue(
                edgeInfo.hasAdjListType(org.apache.graphar.proto.AdjListType.ORDERED_BY_SOURCE));

        // Ordered By Destination
        AdjacentList adjListDst =
                edgeInfo.getAdjacentList(
                        org.apache.graphar.proto.AdjListType.ORDERED_BY_DESTINATION);
        Assert.assertNotNull(adjListDst);
        Assert.assertEquals(
                org.apache.graphar.proto.AdjListType.ORDERED_BY_DESTINATION, adjListDst.getType());
        Assert.assertEquals(
                org.apache.graphar.proto.FileType.CSV, adjListDst.getFileType()); // Changed
        Assert.assertEquals("ordered_by_dest/", adjListDst.getPrefix());
        Assert.assertTrue(
                edgeInfo.hasAdjListType(
                        org.apache.graphar.proto.AdjListType.ORDERED_BY_DESTINATION));

        Assert.assertNotNull(edgeInfo.getPropertyGroups());
        Assert.assertEquals(1, edgeInfo.getPropertyGroups().size());

        PropertyGroup pg = edgeInfo.getPropertyGroup("creationDate");
        Assert.assertNotNull(pg);
        Assert.assertEquals(1, pg.getPropertyList().size()); // Changed
        Assert.assertEquals("creationDate", pg.getPropertyList().get(0).getName()); // Changed
        Assert.assertEquals(org.apache.graphar.proto.FileType.CSV, pg.getFileType()); // Changed
        Assert.assertEquals("creationDate/", pg.getPrefix());
        Assert.assertTrue(edgeInfo.hasProperty("creationDate"));

        Assert.assertEquals(DataType.STRING, edgeInfo.getPropertyType("creationDate"));
        Assert.assertFalse(edgeInfo.isPrimaryKey("creationDate"));
        Assert.assertFalse(
                edgeInfo.isNullableKey("creationDate")); // As per TestUtil programmatic definition

        // Test path generation methods (using AdjListType.ORDERED_BY_SOURCE for example)
        AdjListType adjType = AdjListType.ORDERED_BY_SOURCE;
        String expectedAdjListPrefix = edgeInfo.getPrefix() + adjListSrc.getPrefix() + "adj_list/";
        Assert.assertEquals(expectedAdjListPrefix, edgeInfo.getAdjacentListPrefix(adjType));

        // getAdjListFilePath(0, 0, adjListType) -> getAdjacentListChunkPath(adjListType,
        // vertexChunkIndex, chunkFileIndex)
        Assert.assertEquals(
                expectedAdjListPrefix + "part0/chunk0",
                edgeInfo.getAdjacentListChunkPath(adjType, 0, 0));

        // getOffsetPathPrefix -> no direct method, but it's part of getOffsetChunkPath
        String expectedOffsetPrefix = expectedAdjListPrefix + "offset/";
        // Assert.assertEquals(expectedOffsetPrefix, edgeInfo.getOffsetPrefix(adjType)); // No
        // direct getOffsetPrefix method

        // getAdjListOffsetFilePath(0, adjListType) -> getOffsetChunkPath(adjListType,
        // vertexChunkIndex)
        Assert.assertEquals(
                expectedOffsetPrefix + "chunk0", edgeInfo.getOffsetChunkPath(adjType, 0));

        // getEdgesNumFilePath(0, adjListType) -> getAdjListEdgesNumFilePath(adjListType,
        // vertexChunkIndex)
        Assert.assertEquals(
                expectedAdjListPrefix + "edge_count0",
                edgeInfo.getAdjListEdgesNumFilePath(adjType, 0));

        // getVerticesNumFilePath(adjListType) -> getAdjListVerticesNumFilePath(adjListType)
        Assert.assertEquals(
                expectedAdjListPrefix + "vertex_count",
                edgeInfo.getAdjListVerticesNumFilePath(adjType));

        // getPropertyGroupPathPrefix(pg, adjListType) -> this is not a direct method.
        // The path is constructed using edgeInfo.getAdjacentListPrefix + "property_groups/" +
        // pg.getPrefix()
        String expectedPropertyGroupBasePath =
                expectedAdjListPrefix + "property_groups/" + pg.getPrefix();
        // Assert.assertEquals(expectedPropertyGroupBasePath,
        // edgeInfo.getPropertyGroupPathPrefix(pg, adjType)); // No direct method

        // getPropertyFilePath(pg, adjListType, 0, 0) -> getPropertyGroupChunkPath(pg, adjListType,
        // vertexChunkIndex, chunkFileIndex)
        Assert.assertEquals(
                expectedPropertyGroupBasePath + "part0/chunk0",
                edgeInfo.getPropertyGroupChunkPath(pg, adjType, 0, 0));
    }

    @Test
    public void testPropertyGroup() throws IOException {
        // 1. Obtain PropertyGroup instances from loaded VertexInfo
        String vertexYamlPath = TestUtil.getTestData() + "/ldbc_sample/csv/person.vertex.yml";
        VertexInfo vertexInfo = VertexInfo.loadVertexInfo(vertexYamlPath);

        PropertyGroup pg_id_loaded = vertexInfo.getPropertyGroup("id");
        Assert.assertNotNull(pg_id_loaded);
        PropertyGroup pg_details_loaded =
                vertexInfo.getPropertyGroup("firstName"); // Can get by any prop in the group
        Assert.assertNotNull(pg_details_loaded);

        // 2. Verify attributes of the "id" property group (pg_id_loaded)
        Assert.assertNotNull(pg_id_loaded.getPropertyList()); // Changed
        Assert.assertEquals(1, pg_id_loaded.getPropertyList().size()); // Changed
        Property idPropLoaded = pg_id_loaded.getProperty("id");
        Assert.assertNotNull(idPropLoaded);
        Assert.assertEquals("id", idPropLoaded.getName());
        Assert.assertEquals(org.apache.graphar.proto.DataType.INT64, idPropLoaded.getDataType());
        Assert.assertTrue(idPropLoaded.isPrimary());
        Assert.assertFalse(idPropLoaded.isNullable());
        Assert.assertEquals(org.apache.graphar.proto.FileType.CSV, pg_id_loaded.getFileType());
        Assert.assertEquals("id/", pg_id_loaded.getPrefix());

        // 3. Verify attributes of the "firstName_lastName_gender" property group
        // (pg_details_loaded)
        Assert.assertNotNull(pg_details_loaded.getPropertyList()); // Changed
        Assert.assertEquals(3, pg_details_loaded.getPropertyList().size()); // Changed

        Property firstNamePropLoaded = pg_details_loaded.getProperty("firstName");
        Assert.assertNotNull(firstNamePropLoaded);
        Assert.assertEquals("firstName", firstNamePropLoaded.getName());
        Assert.assertEquals(
                org.apache.graphar.proto.DataType.STRING, firstNamePropLoaded.getDataType());
        Assert.assertFalse(firstNamePropLoaded.isPrimary());
        Assert.assertFalse(firstNamePropLoaded.isNullable());

        Property lastNamePropLoaded = pg_details_loaded.getProperty("lastName");
        Assert.assertNotNull(lastNamePropLoaded);
        Assert.assertEquals("lastName", lastNamePropLoaded.getName());
        Assert.assertEquals(
                org.apache.graphar.proto.DataType.STRING, lastNamePropLoaded.getDataType());
        Assert.assertFalse(lastNamePropLoaded.isPrimary());
        Assert.assertFalse(lastNamePropLoaded.isNullable());

        Property genderPropLoaded = pg_details_loaded.getProperty("gender");
        Assert.assertNotNull(genderPropLoaded);
        Assert.assertEquals("gender", genderPropLoaded.getName());
        Assert.assertEquals(
                org.apache.graphar.proto.DataType.STRING, genderPropLoaded.getDataType());
        Assert.assertFalse(genderPropLoaded.isPrimary());
        Assert.assertTrue(genderPropLoaded.isNullable()); // Gender is nullable

        Assert.assertEquals(org.apache.graphar.proto.FileType.CSV, pg_details_loaded.getFileType());
        Assert.assertEquals("firstName_lastName_gender/", pg_details_loaded.getPrefix());

        // 4. Test PropertyGroup.hasProperty(String name)
        Assert.assertTrue(pg_id_loaded.hasProperty("id"));
        Assert.assertFalse(pg_id_loaded.hasProperty("firstName"));
        Assert.assertTrue(pg_details_loaded.hasProperty("firstName"));
        Assert.assertTrue(pg_details_loaded.hasProperty("lastName"));
        Assert.assertTrue(pg_details_loaded.hasProperty("gender"));
        Assert.assertFalse(pg_details_loaded.hasProperty("id"));

        // 5. Test PropertyGroup.getProperty(String name)
        Assert.assertSame(idPropLoaded, pg_id_loaded.getProperty("id"));
        Assert.assertNull(pg_id_loaded.getProperty("nonExistentProperty"));

        // 6. Test equality (equals() and hashCode())
        // Manually create Property and PropertyGroup instances matching TestUtil definitions
        Property id_manual_prop =
                new Property("id", org.apache.graphar.proto.DataType.INT64, true, false);
        PropertyGroup pg1_manual =
                new PropertyGroup(
                        List.of(id_manual_prop), org.apache.graphar.proto.FileType.CSV, "id/");
        PropertyGroup pg2_manual =
                new PropertyGroup(
                        List.of(id_manual_prop),
                        org.apache.graphar.proto.FileType.CSV,
                        "id/"); // Identical to pg1_manual

        Property firstName_manual_prop =
                new Property("firstName", org.apache.graphar.proto.DataType.STRING, false, false);
        Property lastName_manual_prop =
                new Property("lastName", org.apache.graphar.proto.DataType.STRING, false, false);
        Property gender_manual_prop =
                new Property("gender", org.apache.graphar.proto.DataType.STRING, false, true);
        PropertyGroup pg_details_manual =
                new PropertyGroup(
                        List.of(firstName_manual_prop, lastName_manual_prop, gender_manual_prop),
                        org.apache.graphar.proto.FileType.CSV,
                        "firstName_lastName_gender/");

        // Test equals and hashCode basic contracts
        Assert.assertEquals(pg1_manual, pg2_manual);
        Assert.assertEquals(pg1_manual.hashCode(), pg2_manual.hashCode());

        // Compare manually created with loaded ones
        Assert.assertEquals(pg1_manual, pg_id_loaded);
        Assert.assertEquals(pg1_manual.hashCode(), pg_id_loaded.hashCode());
        Assert.assertEquals(pg_details_manual, pg_details_loaded);
        Assert.assertEquals(pg_details_manual.hashCode(), pg_details_loaded.hashCode());

        // Test inequality for different PropertyGroups
        Assert.assertNotEquals(pg1_manual, pg_details_manual);

        // Test inequality if a property within a list differs
        Property id_manual_prop_changed_nullability =
                new Property(
                        "id",
                        org.apache.graphar.proto.DataType.INT64,
                        true,
                        true); // nullable changed
        PropertyGroup pg_id_prop_changed =
                new PropertyGroup(
                        List.of(id_manual_prop_changed_nullability),
                        org.apache.graphar.proto.FileType.CSV,
                        "id/");
        Assert.assertNotEquals(pg1_manual, pg_id_prop_changed);

        // Test inequality if file type differs
        PropertyGroup pg_filetype_changed =
                new PropertyGroup(
                        List.of(id_manual_prop), org.apache.graphar.proto.FileType.ORC, "id/");
        Assert.assertNotEquals(pg1_manual, pg_filetype_changed);

        // Test inequality if prefix differs
        PropertyGroup pg_prefix_changed =
                new PropertyGroup(
                        List.of(id_manual_prop),
                        org.apache.graphar.proto.FileType.CSV,
                        "id_changed/");
        Assert.assertNotEquals(pg1_manual, pg_prefix_changed);

        // Test inequality if property list order differs (if PropertyGroup.equals considers order,
        // which it should for list equality)
        // For a single property group, order doesn't make sense to test this way.
        // For a group with multiple properties, if PropertyGroup's
        // cachedPropertyList.equals(that.cachedPropertyList) is used, order matters.
        Property p1 = new Property("p1", org.apache.graphar.proto.DataType.INT32, false, false);
        Property p2 = new Property("p2", org.apache.graphar.proto.DataType.STRING, false, false);
        PropertyGroup pg_multi1 =
                new PropertyGroup(List.of(p1, p2), org.apache.graphar.proto.FileType.CSV, "multi/");
        PropertyGroup pg_multi2 =
                new PropertyGroup(
                        List.of(p2, p1),
                        org.apache.graphar.proto.FileType.CSV,
                        "multi/"); // Same properties, different order
        Assert.assertNotEquals(pg_multi1, pg_multi2); // Because List.equals is order-sensitive
    }

    @Test
    public void testAdjacentList() throws IOException {
        // 1. Obtain AdjacentList instances from loaded EdgeInfo
        String edgeYamlPath =
                TestUtil.getTestData() + "/ldbc_sample/csv/person_knows_person.edge.yml";
        EdgeInfo edgeInfo = EdgeInfo.loadEdgeInfo(edgeYamlPath);

        AdjacentList adjList_obs_loaded =
                edgeInfo.getAdjacentList(org.apache.graphar.proto.AdjListType.ORDERED_BY_SOURCE);
        Assert.assertNotNull(adjList_obs_loaded);
        AdjacentList adjList_obd_loaded =
                edgeInfo.getAdjacentList(
                        org.apache.graphar.proto.AdjListType.ORDERED_BY_DESTINATION);
        Assert.assertNotNull(adjList_obd_loaded);

        // 2. Verify attributes of the "ORDERED_BY_SOURCE" AdjacentList
        Assert.assertEquals(
                org.apache.graphar.proto.AdjListType.ORDERED_BY_SOURCE,
                adjList_obs_loaded.getType());
        Assert.assertEquals(
                org.apache.graphar.proto.FileType.CSV, adjList_obs_loaded.getFileType());
        Assert.assertEquals("ordered_by_source/", adjList_obs_loaded.getPrefix());

        // 3. Verify attributes of the "ORDERED_BY_DESTINATION" AdjacentList
        Assert.assertEquals(
                org.apache.graphar.proto.AdjListType.ORDERED_BY_DESTINATION,
                adjList_obd_loaded.getType());
        Assert.assertEquals(
                org.apache.graphar.proto.FileType.CSV, adjList_obd_loaded.getFileType());
        Assert.assertEquals("ordered_by_dest/", adjList_obd_loaded.getPrefix());

        // 4. Test equality (equals() and hashCode())
        // Manually create AdjacentList instances
        AdjacentList al_obs_manual1 =
                new AdjacentList(
                        org.apache.graphar.proto.AdjListType.ORDERED_BY_SOURCE,
                        org.apache.graphar.proto.FileType.CSV,
                        "ordered_by_source/");
        AdjacentList al_obs_manual2 =
                new AdjacentList(
                        org.apache.graphar.proto.AdjListType.ORDERED_BY_SOURCE,
                        org.apache.graphar.proto.FileType.CSV,
                        "ordered_by_source/");
        AdjacentList al_obd_manual =
                new AdjacentList(
                        org.apache.graphar.proto.AdjListType.ORDERED_BY_DESTINATION,
                        org.apache.graphar.proto.FileType.CSV,
                        "ordered_by_dest/");

        // Test equals and hashCode basic contracts
        Assert.assertEquals(al_obs_manual1, al_obs_manual2);
        Assert.assertEquals(al_obs_manual1.hashCode(), al_obs_manual2.hashCode());

        // Compare manually created with loaded ones
        Assert.assertEquals(al_obs_manual1, adjList_obs_loaded);
        Assert.assertEquals(al_obs_manual1.hashCode(), adjList_obs_loaded.hashCode());
        Assert.assertEquals(al_obd_manual, adjList_obd_loaded);
        Assert.assertEquals(al_obd_manual.hashCode(), adjList_obd_loaded.hashCode());

        // Test inequality for different AdjacentList instances
        Assert.assertNotEquals(al_obs_manual1, al_obd_manual);

        // Test inequality with different AdjListType
        AdjacentList al_ubs_manual =
                new AdjacentList(
                        org.apache.graphar.proto.AdjListType.UNORDERED_BY_SOURCE,
                        org.apache.graphar.proto.FileType.CSV,
                        "ordered_by_source/");
        Assert.assertNotEquals(al_obs_manual1, al_ubs_manual);

        // Test inequality with different FileType
        AdjacentList al_obs_orc_manual =
                new AdjacentList(
                        org.apache.graphar.proto.AdjListType.ORDERED_BY_SOURCE,
                        org.apache.graphar.proto.FileType.ORC,
                        "ordered_by_source/");
        Assert.assertNotEquals(al_obs_manual1, al_obs_orc_manual);

        // Test inequality with different prefix
        AdjacentList al_obs_prefix_manual =
                new AdjacentList(
                        org.apache.graphar.proto.AdjListType.ORDERED_BY_SOURCE,
                        org.apache.graphar.proto.FileType.CSV,
                        "prefix_changed/");
        Assert.assertNotEquals(al_obs_manual1, al_obs_prefix_manual);
    }

    @Test
    public void testProperty() throws IOException {
        // 1. Obtain Property instances from loaded PropertyGroup
        String vertexYamlPath = TestUtil.getTestData() + "/ldbc_sample/csv/person.vertex.yml";
        VertexInfo vertexInfo = VertexInfo.loadVertexInfo(vertexYamlPath);

        PropertyGroup pg_id = vertexInfo.getPropertyGroup("id");
        Assert.assertNotNull(pg_id);
        Property idPropLoaded = pg_id.getProperty("id");
        Assert.assertNotNull(idPropLoaded);

        PropertyGroup pg_details =
                vertexInfo.getPropertyGroup("firstName"); // or "lastName", "gender"
        Assert.assertNotNull(pg_details);
        Property firstNamePropLoaded = pg_details.getProperty("firstName");
        Assert.assertNotNull(firstNamePropLoaded);

        // 2. Verify attributes of the "id" Property (loaded)
        Assert.assertEquals("id", idPropLoaded.getName());
        Assert.assertEquals(org.apache.graphar.proto.DataType.INT64, idPropLoaded.getDataType());
        Assert.assertTrue(idPropLoaded.isPrimary());
        Assert.assertFalse(idPropLoaded.isNullable());

        // 3. Verify attributes of the "firstName" Property (loaded)
        Assert.assertEquals("firstName", firstNamePropLoaded.getName());
        Assert.assertEquals(
                org.apache.graphar.proto.DataType.STRING, firstNamePropLoaded.getDataType());
        Assert.assertFalse(firstNamePropLoaded.isPrimary());
        Assert.assertFalse(firstNamePropLoaded.isNullable());

        // 4. Test equality (equals() and hashCode())
        // Create Property objects programmatically
        Property p1_manual =
                new Property("id", org.apache.graphar.proto.DataType.INT64, true, false);
        Property p2_manual =
                new Property(
                        "id",
                        org.apache.graphar.proto.DataType.INT64,
                        true,
                        false); // Identical to p1_manual

        // Assert p1_manual.equals(p2_manual) is true and hashCodes are equal
        Assert.assertEquals(p1_manual, p2_manual);
        Assert.assertEquals(p1_manual.hashCode(), p2_manual.hashCode());

        // Assert programmatically created "id" Property is equal to loaded "id" Property
        Assert.assertEquals(p1_manual, idPropLoaded);
        Assert.assertEquals(p1_manual.hashCode(), idPropLoaded.hashCode());

        // Create a different Property object ("firstName")
        Property p3_manual_firstName =
                new Property("firstName", org.apache.graphar.proto.DataType.STRING, false, false);
        Assert.assertNotEquals(p1_manual, p3_manual_firstName);

        // Compare loaded firstName with manually created firstName
        Assert.assertEquals(p3_manual_firstName, firstNamePropLoaded);
        Assert.assertEquals(p3_manual_firstName.hashCode(), firstNamePropLoaded.hashCode());

        // Modify attributes and assert inequality
        Property p_name_changed =
                new Property("id_changed", org.apache.graphar.proto.DataType.INT64, true, false);
        Assert.assertNotEquals(p1_manual, p_name_changed);

        Property p_type_changed =
                new Property("id", org.apache.graphar.proto.DataType.STRING, true, false);
        Assert.assertNotEquals(p1_manual, p_type_changed);

        Property p_primary_changed =
                new Property("id", org.apache.graphar.proto.DataType.INT64, false, false);
        Assert.assertNotEquals(p1_manual, p_primary_changed);

        Property p_nullable_changed =
                new Property("id", org.apache.graphar.proto.DataType.INT64, true, true);
        Assert.assertNotEquals(p1_manual, p_nullable_changed);
    }
}
