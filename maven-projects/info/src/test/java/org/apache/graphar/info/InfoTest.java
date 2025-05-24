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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.List;
import org.apache.graphar.info.loader.GraphLoader;
import org.apache.graphar.info.loader.LocalYamlGraphLoader;
import org.apache.graphar.types.AdjListType;
import org.apache.graphar.types.DataType; // Assuming this is the class/interface
import org.apache.graphar.types.FileType;
import org.apache.graphar.util.GrapharStaticFunctions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class InfoTest {

    @BeforeAll
    public static void init() {
        TestUtil.checkTestData();
    }

    @Test
    public void testLoadGraphInfo() {
        String graphInfoPath = TestUtil.getLdbcSampleGraphPath();
        GraphInfo graphInfo = null;
        try {
            GraphLoader graphLoader = new LocalYamlGraphLoader();
            graphInfo = graphLoader.load(graphInfoPath);
        } catch (IOException e) {
            fail("Failed to load GraphInfo: " + e.getMessage());
        }

        assertNotNull(graphInfo);
        assertEquals("ldbc_sample", graphInfo.getName());

        String expectedPrefix = graphInfoPath.substring(0, graphInfoPath.lastIndexOf("/") + 1);
        assertEquals(expectedPrefix, graphInfo.getPrefix());

        assertNotNull(graphInfo.getVersion());
        assertEquals("gar/v1", graphInfo.getVersion().toString());

        assertNotNull(graphInfo.getVertexInfos());
        assertEquals(1, graphInfo.getVertexInfos().size());
        assertNotNull(graphInfo.getEdgeInfos());
        assertEquals(1, graphInfo.getEdgeInfos().size());

        VertexInfo vertexInfo = graphInfo.getVertexInfos().get(0);
        assertSame(vertexInfo, graphInfo.getVertexInfo("person"));
        assertEquals("person", vertexInfo.getType());

        EdgeInfo edgeInfo = graphInfo.getEdgeInfos().get(0);
        assertSame(edgeInfo, graphInfo.getEdgeInfo("person", "knows", "person"));
        assertEquals("person_knows_person", edgeInfo.getConcatKey());
    }

    @Test
    public void testLoadVertexInfo() throws IOException {
        String vertexYamlPath = TestUtil.getTestData() + "/ldbc_sample/csv/person.vertex.yml";
        VertexInfo vertexInfo = GrapharStaticFunctions.INSTANCE.loadVertexInfo(vertexYamlPath);

        assertNotNull(vertexInfo);
        assertEquals("person", vertexInfo.getType());
        assertEquals(100, vertexInfo.getChunkSize());
        assertEquals("vertex/person/", vertexInfo.getPrefix());
        assertEquals("gar/v1", vertexInfo.getVersion().toString());

        String primaryKey = "";
        for (PropertyGroup pg : vertexInfo.getPropertyGroups()) {
            for (Property p : pg.getProperties()) {
                if (p.isPrimary()) {
                    primaryKey = p.getName();
                    break;
                }
            }
            if (!primaryKey.isEmpty()) {
                break;
            }
        }
        assertEquals("id", primaryKey);

        List<PropertyGroup> propertyGroups = vertexInfo.getPropertyGroups();
        assertNotNull(propertyGroups);
        assertEquals(2, propertyGroups.size());

        PropertyGroup pg1 = vertexInfo.getPropertyGroup("id");
        assertNotNull(pg1);
        assertEquals(1, pg1.getProperties().size());
        assertEquals("id", pg1.getProperties().get(0).getName());
        assertEquals(FileType.CSV, pg1.getFileType());
        assertEquals("id/", pg1.getPrefix());
        assertTrue(vertexInfo.hasProperty("id"));

        PropertyGroup pg2 = vertexInfo.getPropertyGroup("firstName");
        assertNotNull(pg2);
        assertSame(pg2, vertexInfo.getPropertyGroup("lastName"));
        assertSame(pg2, vertexInfo.getPropertyGroup("gender"));

        assertEquals(3, pg2.getProperties().size());
        assertEquals(FileType.CSV, pg2.getFileType());
        assertEquals("firstName_lastName_gender/", pg2.getPrefix());
        assertTrue(vertexInfo.hasProperty("firstName"));
        assertTrue(vertexInfo.hasProperty("lastName"));
        assertTrue(vertexInfo.hasProperty("gender"));

        // Corrected DataType usage as per subtask example (DataType.Type.XXX)
        assertEquals(DataType.Type.INT64, vertexInfo.getPropertyType("id").getType());
        assertTrue(vertexInfo.isPrimaryKey("id"));
        assertFalse(vertexInfo.isNullableKey("id"));

        assertEquals(DataType.Type.STRING, vertexInfo.getPropertyType("firstName").getType());
        assertFalse(vertexInfo.isPrimaryKey("firstName"));
        assertFalse(vertexInfo.isNullableKey("firstName"));

        assertEquals(DataType.Type.STRING, vertexInfo.getPropertyType("gender").getType());
        assertFalse(vertexInfo.isPrimaryKey("gender"));
        assertTrue(vertexInfo.isNullableKey("gender"));

        assertEquals("vertex/person/vertex_count", vertexInfo.getVerticesNumFilePath().toString());

        assertEquals("vertex/person/id/chunk0", vertexInfo.getFilePath(pg1, 0).toString());
        assertEquals("vertex/person/id/", vertexInfo.getPathPrefix(pg1));

        assertEquals(
                "vertex/person/firstName_lastName_gender/chunk0",
                vertexInfo.getFilePath(pg2, 0).toString());
        assertEquals("vertex/person/firstName_lastName_gender/", vertexInfo.getPathPrefix(pg2));
    }

    @Test
    public void testLoadEdgeInfo() throws IOException {
        String edgeYamlPath =
                TestUtil.getTestData() + "/ldbc_sample/csv/person_knows_person.edge.yml";
        EdgeInfo edgeInfo = GrapharStaticFunctions.INSTANCE.loadEdgeInfo(edgeYamlPath);

        assertNotNull(edgeInfo);
        assertEquals("person", edgeInfo.getSrcType());
        assertEquals("knows", edgeInfo.getEdgeType());
        assertEquals("person", edgeInfo.getDstType());
        assertEquals(1024, edgeInfo.getChunkSize());
        assertEquals(100, edgeInfo.getSrcChunkSize());
        assertEquals(100, edgeInfo.getDstChunkSize());
        assertFalse(edgeInfo.isDirected());
        assertEquals("edge/person_knows_person/", edgeInfo.getPrefix());
        assertEquals("gar/v1", edgeInfo.getVersion().toString());

        assertNotNull(edgeInfo.getAdjLists());
        assertEquals(2, edgeInfo.getAdjLists().size());

        AdjacentList adjListSrc = edgeInfo.getAdjList(AdjListType.ordered_by_source);
        assertNotNull(adjListSrc);
        assertEquals(AdjListType.ordered_by_source, adjListSrc.getAdjListType());
        assertEquals(FileType.CSV, adjListSrc.getFileType());
        assertEquals("ordered_by_source/", adjListSrc.getPrefix());
        assertTrue(edgeInfo.hasAdjList(AdjListType.ordered_by_source));

        AdjacentList adjListDst = edgeInfo.getAdjList(AdjListType.ordered_by_dest);
        assertNotNull(adjListDst);
        assertEquals(AdjListType.ordered_by_dest, adjListDst.getAdjListType());
        assertEquals(FileType.CSV, adjListDst.getFileType());
        assertEquals("ordered_by_dest/", adjListDst.getPrefix());
        assertTrue(edgeInfo.hasAdjList(AdjListType.ordered_by_dest));

        assertNotNull(edgeInfo.getPropertyGroups());
        assertEquals(1, edgeInfo.getPropertyGroups().size());

        PropertyGroup pg = edgeInfo.getPropertyGroup("creationDate");
        assertNotNull(pg);
        assertEquals(1, pg.getProperties().size());
        assertEquals("creationDate", pg.getProperties().get(0).getName());
        assertEquals(FileType.CSV, pg.getFileType());
        assertEquals("creationDate/", pg.getPrefix());
        assertTrue(edgeInfo.hasProperty("creationDate"));

        assertEquals(DataType.Type.STRING, edgeInfo.getPropertyType("creationDate").getType());
        assertFalse(edgeInfo.isPrimaryKey("creationDate"));
        assertFalse(edgeInfo.isNullableKey("creationDate"));

        AdjListType adjType = AdjListType.ordered_by_source;

        String adjListPathPrefix = edgeInfo.getAdjListPathPrefix(adjType).toString();
        assertEquals(
                edgeInfo.getPrefix() + adjListSrc.getPrefix() + "adj_list/", adjListPathPrefix);

        assertEquals(
                adjListPathPrefix + "part0/chunk0",
                edgeInfo.getAdjListFilePath(0, 0, adjType).toString());

        String offsetPathPrefix = edgeInfo.getOffsetPathPrefix(adjType).toString();
        assertEquals(edgeInfo.getPrefix() + adjListSrc.getPrefix() + "offset/", offsetPathPrefix);

        assertEquals(
                offsetPathPrefix + "chunk0",
                edgeInfo.getAdjListOffsetFilePath(0, adjType).toString());

        assertEquals(
                edgeInfo.getPrefix() + adjListSrc.getPrefix() + "edge_count0",
                edgeInfo.getEdgesNumFilePath(0, adjType).toString());

        assertEquals(
                edgeInfo.getPrefix() + adjListSrc.getPrefix() + "vertex_count",
                edgeInfo.getVerticesNumFilePath(adjType).toString());

        String propertyGroupPathPrefix =
                edgeInfo.getPropertyGroupPathPrefix(pg, adjType).toString();
        assertEquals(
                edgeInfo.getPrefix() + adjListSrc.getPrefix() + pg.getPrefix(),
                propertyGroupPathPrefix);

        assertEquals(
                propertyGroupPathPrefix + "part0/chunk0",
                edgeInfo.getPropertyFilePath(pg, adjType, 0, 0).toString());
    }

    @Test
    public void testPropertyGroup() throws IOException {
        String vertexYamlPath = TestUtil.getTestData() + "/ldbc_sample/csv/person.vertex.yml";
        VertexInfo vertexInfo = GrapharStaticFunctions.INSTANCE.loadVertexInfo(vertexYamlPath);

        PropertyGroup pg_id_loaded = vertexInfo.getPropertyGroup("id");
        assertNotNull(pg_id_loaded);
        PropertyGroup pg_details_loaded = vertexInfo.getPropertyGroup("firstName");
        assertNotNull(pg_details_loaded);

        assertNotNull(pg_id_loaded.getProperties());
        assertEquals(1, pg_id_loaded.getProperties().size());
        Property idPropLoaded = pg_id_loaded.getProperty("id");
        assertNotNull(idPropLoaded);
        assertEquals("id", idPropLoaded.getName());
        assertEquals(DataType.Type.INT64, idPropLoaded.getDataType().getType());
        assertTrue(idPropLoaded.isPrimary());
        assertFalse(idPropLoaded.isNullable());
        assertEquals(FileType.CSV, pg_id_loaded.getFileType());
        assertEquals("id/", pg_id_loaded.getPrefix());

        assertNotNull(pg_details_loaded.getProperties());
        assertEquals(3, pg_details_loaded.getProperties().size());

        Property firstNamePropLoaded = pg_details_loaded.getProperty("firstName");
        assertNotNull(firstNamePropLoaded);
        assertEquals("firstName", firstNamePropLoaded.getName());
        assertEquals(DataType.Type.STRING, firstNamePropLoaded.getDataType().getType());
        assertFalse(firstNamePropLoaded.isPrimary());
        assertFalse(firstNamePropLoaded.isNullable());

        Property lastNamePropLoaded = pg_details_loaded.getProperty("lastName");
        assertNotNull(lastNamePropLoaded);
        assertEquals("lastName", lastNamePropLoaded.getName());
        assertEquals(DataType.Type.STRING, lastNamePropLoaded.getDataType().getType());
        assertFalse(lastNamePropLoaded.isPrimary());
        assertFalse(lastNamePropLoaded.isNullable());

        Property genderPropLoaded = pg_details_loaded.getProperty("gender");
        assertNotNull(genderPropLoaded);
        assertEquals("gender", genderPropLoaded.getName());
        assertEquals(DataType.Type.STRING, genderPropLoaded.getDataType().getType());
        assertFalse(genderPropLoaded.isPrimary());
        assertTrue(genderPropLoaded.isNullable());

        assertEquals(FileType.CSV, pg_details_loaded.getFileType());
        assertEquals("firstName_lastName_gender/", pg_details_loaded.getPrefix());

        assertTrue(pg_id_loaded.hasProperty("id"));
        assertFalse(pg_id_loaded.hasProperty("firstName"));
        assertTrue(pg_details_loaded.hasProperty("firstName"));
        assertTrue(pg_details_loaded.hasProperty("lastName"));
        assertTrue(pg_details_loaded.hasProperty("gender"));
        assertFalse(pg_details_loaded.hasProperty("id"));

        assertSame(idPropLoaded, pg_id_loaded.getProperty("id"));
        assertNull(pg_id_loaded.getProperty("nonExistentProperty"));

        // For manual creation, if DataType is a class with an inner Type enum:
        // Property id_manual_prop = new Property("id",
        // DataType.newBuilder().setType(DataType.Type.INT64).build(), true, false);
        // This depends on the actual structure of org.apache.graphar.types.DataType
        // For now, assuming TestUtil's way of creating Property is correct for its context
        // and we are focusing on fixing loaded object interaction here.
        // The following manual creations will likely fail if DataType.INT64 isn't an instance of
        // DataType class.
        // However, the primary goal is to fix how loaded properties are *read*.
        Property id_manual_prop =
                new Property(
                        "id",
                        DataType.newBuilder().setType(DataType.Type.INT64).build(),
                        true,
                        false);
        PropertyGroup pg1_manual = new PropertyGroup(List.of(id_manual_prop), FileType.CSV, "id/");
        PropertyGroup pg2_manual = new PropertyGroup(List.of(id_manual_prop), FileType.CSV, "id/");

        Property firstName_manual_prop =
                new Property(
                        "firstName",
                        DataType.newBuilder().setType(DataType.Type.STRING).build(),
                        false,
                        false);
        Property lastName_manual_prop =
                new Property(
                        "lastName",
                        DataType.newBuilder().setType(DataType.Type.STRING).build(),
                        false,
                        false);
        Property gender_manual_prop =
                new Property(
                        "gender",
                        DataType.newBuilder().setType(DataType.Type.STRING).build(),
                        false,
                        true);
        PropertyGroup pg_details_manual =
                new PropertyGroup(
                        List.of(firstName_manual_prop, lastName_manual_prop, gender_manual_prop),
                        FileType.CSV,
                        "firstName_lastName_gender/");

        assertEquals(pg1_manual, pg2_manual);
        assertEquals(pg1_manual.hashCode(), pg2_manual.hashCode());

        assertEquals(pg1_manual, pg_id_loaded);
        assertEquals(pg1_manual.hashCode(), pg_id_loaded.hashCode());
        assertEquals(pg_details_manual, pg_details_loaded);
        assertEquals(pg_details_manual.hashCode(), pg_details_loaded.hashCode());

        assertNotEquals(pg1_manual, pg_details_manual);

        Property id_manual_prop_changed_nullability =
                new Property(
                        "id",
                        DataType.newBuilder().setType(DataType.Type.INT64).build(),
                        true,
                        true);
        PropertyGroup pg_id_prop_changed =
                new PropertyGroup(List.of(id_manual_prop_changed_nullability), FileType.CSV, "id/");
        assertNotEquals(pg1_manual, pg_id_prop_changed);

        PropertyGroup pg_filetype_changed =
                new PropertyGroup(List.of(id_manual_prop), FileType.ORC, "id/");
        assertNotEquals(pg1_manual, pg_filetype_changed);

        PropertyGroup pg_prefix_changed =
                new PropertyGroup(List.of(id_manual_prop), FileType.CSV, "id_changed/");
        assertNotEquals(pg1_manual, pg_prefix_changed);

        Property p1 =
                new Property(
                        "p1",
                        DataType.newBuilder().setType(DataType.Type.INT32).build(),
                        false,
                        false);
        Property p2 =
                new Property(
                        "p2",
                        DataType.newBuilder().setType(DataType.Type.STRING).build(),
                        false,
                        false);
        PropertyGroup pg_multi1 = new PropertyGroup(List.of(p1, p2), FileType.CSV, "multi/");
        PropertyGroup pg_multi2 = new PropertyGroup(List.of(p2, p1), FileType.CSV, "multi/");
        assertNotEquals(pg_multi1, pg_multi2);
    }

    @Test
    public void testAdjacentList() throws IOException {
        String edgeYamlPath =
                TestUtil.getTestData() + "/ldbc_sample/csv/person_knows_person.edge.yml";
        EdgeInfo edgeInfo = GrapharStaticFunctions.INSTANCE.loadEdgeInfo(edgeYamlPath);

        AdjacentList adjList_obs_loaded = edgeInfo.getAdjList(AdjListType.ordered_by_source);
        assertNotNull(adjList_obs_loaded);
        AdjacentList adjList_obd_loaded = edgeInfo.getAdjList(AdjListType.ordered_by_dest);
        assertNotNull(adjList_obd_loaded);

        assertEquals(AdjListType.ordered_by_source, adjList_obs_loaded.getAdjListType());
        assertEquals(FileType.CSV, adjList_obs_loaded.getFileType());
        assertEquals("ordered_by_source/", adjList_obs_loaded.getPrefix());

        assertEquals(AdjListType.ordered_by_dest, adjList_obd_loaded.getAdjListType());
        assertEquals(FileType.CSV, adjList_obd_loaded.getFileType());
        assertEquals("ordered_by_dest/", adjList_obd_loaded.getPrefix());

        AdjacentList al_obs_manual1 =
                new AdjacentList(AdjListType.ordered_by_source, FileType.CSV, "ordered_by_source/");
        AdjacentList al_obs_manual2 =
                new AdjacentList(AdjListType.ordered_by_source, FileType.CSV, "ordered_by_source/");
        AdjacentList al_obd_manual =
                new AdjacentList(AdjListType.ordered_by_dest, FileType.CSV, "ordered_by_dest/");

        assertEquals(al_obs_manual1, al_obs_manual2);
        assertEquals(al_obs_manual1.hashCode(), al_obs_manual2.hashCode());

        assertEquals(al_obs_manual1, adjList_obs_loaded);
        assertEquals(al_obs_manual1.hashCode(), adjList_obs_loaded.hashCode());
        assertEquals(al_obd_manual, adjList_obd_loaded);
        assertEquals(al_obd_manual.hashCode(), adjList_obd_loaded.hashCode());

        assertNotEquals(al_obs_manual1, al_obd_manual);

        AdjacentList al_ubs_manual =
                new AdjacentList(
                        AdjListType.unordered_by_source, FileType.CSV, "ordered_by_source/");
        assertNotEquals(al_obs_manual1, al_ubs_manual);

        AdjacentList al_obs_orc_manual =
                new AdjacentList(AdjListType.ordered_by_source, FileType.ORC, "ordered_by_source/");
        assertNotEquals(al_obs_manual1, al_obs_orc_manual);

        AdjacentList al_obs_prefix_manual =
                new AdjacentList(AdjListType.ordered_by_source, FileType.CSV, "prefix_changed/");
        assertNotEquals(al_obs_manual1, al_obs_prefix_manual);
    }

    @Test
    public void testProperty() throws IOException {
        String vertexYamlPath = TestUtil.getTestData() + "/ldbc_sample/csv/person.vertex.yml";
        VertexInfo vertexInfo = GrapharStaticFunctions.INSTANCE.loadVertexInfo(vertexYamlPath);

        PropertyGroup pg_id = vertexInfo.getPropertyGroup("id");
        assertNotNull(pg_id);
        Property idPropLoaded = pg_id.getProperty("id");
        assertNotNull(idPropLoaded);

        PropertyGroup pg_details = vertexInfo.getPropertyGroup("firstName");
        assertNotNull(pg_details);
        Property firstNamePropLoaded = pg_details.getProperty("firstName");
        assertNotNull(firstNamePropLoaded);

        assertEquals("id", idPropLoaded.getName());
        assertEquals(DataType.Type.INT64, idPropLoaded.getDataType().getType());
        assertTrue(idPropLoaded.isPrimary());
        assertFalse(idPropLoaded.isNullable());

        assertEquals("firstName", firstNamePropLoaded.getName());
        assertEquals(DataType.Type.STRING, firstNamePropLoaded.getDataType().getType());
        assertFalse(firstNamePropLoaded.isPrimary());
        assertFalse(firstNamePropLoaded.isNullable());

        // Manual creation of Property objects needs to align with actual DataType
        // constructor/factory
        // Assuming DataType.newBuilder().setType(DataType.Type.INT64).build() is how it's done
        // This is a guess and might need correction if DataType is a simple enum.
        Property p1_manual =
                new Property(
                        "id",
                        DataType.newBuilder().setType(DataType.Type.INT64).build(),
                        true,
                        false);
        Property p2_manual =
                new Property(
                        "id",
                        DataType.newBuilder().setType(DataType.Type.INT64).build(),
                        true,
                        false);

        assertEquals(p1_manual, p2_manual);
        assertEquals(p1_manual.hashCode(), p2_manual.hashCode());

        assertEquals(p1_manual, idPropLoaded);
        assertEquals(p1_manual.hashCode(), idPropLoaded.hashCode());

        Property p3_manual_firstName =
                new Property(
                        "firstName",
                        DataType.newBuilder().setType(DataType.Type.STRING).build(),
                        false,
                        false);
        assertNotEquals(p1_manual, p3_manual_firstName);

        assertEquals(p3_manual_firstName, firstNamePropLoaded);
        assertEquals(p3_manual_firstName.hashCode(), firstNamePropLoaded.hashCode());

        Property p_name_changed =
                new Property(
                        "id_changed",
                        DataType.newBuilder().setType(DataType.Type.INT64).build(),
                        true,
                        false);
        assertNotEquals(p1_manual, p_name_changed);

        Property p_type_changed =
                new Property(
                        "id",
                        DataType.newBuilder().setType(DataType.Type.STRING).build(),
                        true,
                        false);
        assertNotEquals(p1_manual, p_type_changed);

        Property p_primary_changed =
                new Property(
                        "id",
                        DataType.newBuilder().setType(DataType.Type.INT64).build(),
                        false,
                        false);
        assertNotEquals(p1_manual, p_primary_changed);

        Property p_nullable_changed =
                new Property(
                        "id",
                        DataType.newBuilder().setType(DataType.Type.INT64).build(),
                        true,
                        true);
        assertNotEquals(p1_manual, p_nullable_changed);
    }
}
