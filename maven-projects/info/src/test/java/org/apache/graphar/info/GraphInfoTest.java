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
import java.util.ArrayList;
import java.util.List;
import org.apache.graphar.info.loader.GraphInfoLoader;
import org.apache.graphar.info.loader.impl.LocalFileSystemStreamGraphInfoLoader;
import org.apache.graphar.info.type.AdjListType;
import org.apache.graphar.info.type.DataType;
import org.apache.graphar.info.type.FileType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class GraphInfoTest {

    private static GraphInfo graphInfo;
    private static VertexInfo personVertexInfo;
    private static EdgeInfo knowsEdgeInfo;
    private static URI GRAPH_PATH_URI;

    @BeforeClass
    public static void setUp() {
        TestUtil.checkTestData();

        // Always use real test data - fail if not available
        GRAPH_PATH_URI = TestUtil.getLdbcSampleGraphURI();
        GraphInfoLoader loader = new LocalFileSystemStreamGraphInfoLoader();
        try {
            graphInfo = loader.loadGraphInfo(GRAPH_PATH_URI);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to load real test data from " + GRAPH_PATH_URI + ": " + e.getMessage(),
                    e);
        }

        personVertexInfo = graphInfo.getVertexInfos().get(0);
        knowsEdgeInfo = graphInfo.getEdgeInfos().get(0);
    }

    @AfterClass
    public static void clean() {}

    @Test
    public void test() {
        System.out.println(
                graphInfo
                        .getBaseUri()
                        .resolve(
                                knowsEdgeInfo.getAdjacentListPrefix(
                                        AdjListType.ordered_by_source)));
    }

    @Test
    public void testGraphInfoBasics() {
        Assert.assertNotNull(graphInfo);
        Assert.assertEquals("ldbc_sample", graphInfo.getName());

        // For real test data
        Assert.assertEquals(GRAPH_PATH_URI.resolve(".").toString(), graphInfo.getPrefix());
        Assert.assertEquals(GRAPH_PATH_URI.resolve("."), graphInfo.getBaseUri());

        Assert.assertNotNull(graphInfo.getEdgeInfos());
        Assert.assertEquals(1, graphInfo.getEdgeInfos().size());
        Assert.assertNotNull(graphInfo.getVertexInfos());
        Assert.assertEquals(1, graphInfo.getVertexInfos().size());
        // test version gar/v1
        Assert.assertEquals(1, graphInfo.getVersion().getVersion());
    }

    @Test
    public void testPersonVertexInfoBasics() {
        VertexInfo personVertexInfo = graphInfo.getVertexInfos().get(0);
        Assert.assertEquals("person", personVertexInfo.getType());
        Assert.assertEquals(100, personVertexInfo.getChunkSize());
        Assert.assertEquals("vertex/person/", personVertexInfo.getPrefix());
        Assert.assertEquals(URI.create("vertex/person/"), personVertexInfo.getBaseUri());
        Assert.assertNotNull(personVertexInfo.getPropertyGroups());
        Assert.assertEquals(2, personVertexInfo.getPropertyGroups().size());
        Assert.assertEquals(1, personVertexInfo.getVersion().getVersion());
    }

    @Test
    public void testPersonVertexPropertyGroup() {
        // group1 id
        PropertyGroup idPropertyGroup = personVertexInfo.getPropertyGroups().get(0);
        Assert.assertEquals("id/", idPropertyGroup.getPrefix());
        Assert.assertEquals(URI.create("id/"), idPropertyGroup.getBaseUri());
        Assert.assertEquals(FileType.CSV, idPropertyGroup.getFileType());
        Assert.assertEquals(
                URI.create("vertex/person/id/"),
                personVertexInfo.getPropertyGroupPrefix(idPropertyGroup));
        Assert.assertEquals(
                URI.create("vertex/person/id/chunk0"),
                personVertexInfo.getPropertyGroupChunkPath(idPropertyGroup, 0));
        Assert.assertEquals(
                URI.create("vertex/person/id/chunk4"),
                personVertexInfo.getPropertyGroupChunkPath(idPropertyGroup, 4));
        Assert.assertNotNull(idPropertyGroup.getPropertyList());
        Assert.assertEquals(1, idPropertyGroup.getPropertyList().size());
        Property idProperty = idPropertyGroup.getPropertyList().get(0);
        Assert.assertTrue(personVertexInfo.hasProperty("id"));
        Assert.assertEquals("id", idProperty.getName());
        Assert.assertEquals(DataType.INT64, idProperty.getDataType());
        Assert.assertTrue(idProperty.isPrimary());
        Assert.assertFalse(idProperty.isNullable());
        // group2 firstName_lastName_gender
        PropertyGroup firstName_lastName_gender = personVertexInfo.getPropertyGroups().get(1);
        Assert.assertEquals("firstName_lastName_gender/", firstName_lastName_gender.getPrefix());
        Assert.assertEquals(
                URI.create("firstName_lastName_gender/"), firstName_lastName_gender.getBaseUri());
        Assert.assertEquals(FileType.CSV, firstName_lastName_gender.getFileType());
        Assert.assertEquals(
                URI.create("vertex/person/firstName_lastName_gender/"),
                personVertexInfo.getPropertyGroupPrefix(firstName_lastName_gender));
        Assert.assertEquals(
                URI.create("vertex/person/firstName_lastName_gender/chunk0"),
                personVertexInfo.getPropertyGroupChunkPath(firstName_lastName_gender, 0));
        Assert.assertEquals(
                URI.create("vertex/person/firstName_lastName_gender/chunk4"),
                personVertexInfo.getPropertyGroupChunkPath(firstName_lastName_gender, 4));
        Assert.assertNotNull(firstName_lastName_gender.getPropertyList());
        Assert.assertEquals(3, firstName_lastName_gender.getPropertyList().size());
        Property firstNameProperty = firstName_lastName_gender.getPropertyList().get(0);
        Assert.assertTrue(personVertexInfo.hasProperty("firstName"));
        Assert.assertEquals("firstName", firstNameProperty.getName());
        Assert.assertEquals(DataType.STRING, firstNameProperty.getDataType());
        Assert.assertFalse(firstNameProperty.isPrimary());
        Assert.assertTrue(firstNameProperty.isNullable());
        Property lastNameProperty = firstName_lastName_gender.getPropertyList().get(1);
        Assert.assertTrue(personVertexInfo.hasProperty("lastName"));
        Assert.assertEquals("lastName", lastNameProperty.getName());
        Assert.assertEquals(DataType.STRING, lastNameProperty.getDataType());
        Assert.assertFalse(lastNameProperty.isPrimary());
        Assert.assertTrue(lastNameProperty.isNullable());
        Property genderProperty = firstName_lastName_gender.getPropertyList().get(2);
        Assert.assertTrue(personVertexInfo.hasProperty("gender"));
        Assert.assertEquals("gender", genderProperty.getName());
        Assert.assertEquals(DataType.STRING, genderProperty.getDataType());
        Assert.assertFalse(genderProperty.isPrimary());
        Assert.assertTrue(genderProperty.isNullable());
    }

    @Test
    public void testKnowEdgeInfoBasic() {
        Assert.assertEquals("knows", knowsEdgeInfo.getEdgeType());
        Assert.assertEquals(1024, knowsEdgeInfo.getChunkSize());
        Assert.assertEquals("person", knowsEdgeInfo.getSrcType());
        Assert.assertEquals(100, knowsEdgeInfo.getSrcChunkSize());
        Assert.assertEquals("person", knowsEdgeInfo.getDstType());
        Assert.assertEquals(100, knowsEdgeInfo.getDstChunkSize());
        Assert.assertFalse(knowsEdgeInfo.isDirected());
        Assert.assertEquals("person_knows_person", knowsEdgeInfo.getConcat());
        Assert.assertEquals("edge/person_knows_person/", knowsEdgeInfo.getPrefix());
        Assert.assertEquals(URI.create("edge/person_knows_person/"), knowsEdgeInfo.getBaseUri());
        Assert.assertEquals(1, knowsEdgeInfo.getVersion().getVersion());
    }

    @Test
    public void testKnowsEdgeAdjacencyLists() {
        Assert.assertEquals(2, knowsEdgeInfo.getAdjacentLists().size());
        // test ordered by source adjacency list
        AdjacentList adjOrderBySource =
                knowsEdgeInfo.getAdjacentList(AdjListType.ordered_by_source);
        Assert.assertEquals(FileType.CSV, adjOrderBySource.getFileType());
        Assert.assertEquals(AdjListType.ordered_by_source, adjOrderBySource.getType());
        Assert.assertEquals("ordered_by_source/", adjOrderBySource.getPrefix());
        Assert.assertEquals(URI.create("ordered_by_source/"), adjOrderBySource.getBaseUri());
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/adj_list/vertex_count"),
                knowsEdgeInfo.getVerticesNumFilePath(AdjListType.ordered_by_source));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/adj_list/edge_count0"),
                knowsEdgeInfo.getEdgesNumFilePath(AdjListType.ordered_by_source, 0));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/adj_list/edge_count4"),
                knowsEdgeInfo.getEdgesNumFilePath(AdjListType.ordered_by_source, 4));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/adj_list/"),
                knowsEdgeInfo.getAdjacentListPrefix(AdjListType.ordered_by_source));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/adj_list/chunk0"),
                knowsEdgeInfo.getAdjacentListChunkPath(AdjListType.ordered_by_source, 0));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/adj_list/chunk4"),
                knowsEdgeInfo.getAdjacentListChunkPath(AdjListType.ordered_by_source, 4));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/adj_list/offset/"),
                knowsEdgeInfo.getOffsetPrefix(AdjListType.ordered_by_source));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/adj_list/offset/chunk0"),
                knowsEdgeInfo.getOffsetChunkPath(AdjListType.ordered_by_source, 0));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/adj_list/offset/chunk4"),
                knowsEdgeInfo.getOffsetChunkPath(AdjListType.ordered_by_source, 4));

        // test ordered by destination adjacency list
        AdjacentList adjOrderByDestination =
                knowsEdgeInfo.getAdjacentList(AdjListType.ordered_by_dest);
        Assert.assertEquals(FileType.CSV, adjOrderByDestination.getFileType());
        Assert.assertEquals(AdjListType.ordered_by_dest, adjOrderByDestination.getType());
        Assert.assertEquals("ordered_by_dest/", adjOrderByDestination.getPrefix());
        Assert.assertEquals(URI.create("ordered_by_dest/"), adjOrderByDestination.getBaseUri());
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_dest/adj_list/vertex_count"),
                knowsEdgeInfo.getVerticesNumFilePath(AdjListType.ordered_by_dest));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_dest/adj_list/edge_count0"),
                knowsEdgeInfo.getEdgesNumFilePath(AdjListType.ordered_by_dest, 0));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_dest/adj_list/edge_count4"),
                knowsEdgeInfo.getEdgesNumFilePath(AdjListType.ordered_by_dest, 4));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_dest/adj_list/"),
                knowsEdgeInfo.getAdjacentListPrefix(AdjListType.ordered_by_dest));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_dest/adj_list/chunk0"),
                knowsEdgeInfo.getAdjacentListChunkPath(AdjListType.ordered_by_dest, 0));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_dest/adj_list/chunk4"),
                knowsEdgeInfo.getAdjacentListChunkPath(AdjListType.ordered_by_dest, 4));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_dest/adj_list/offset/"),
                knowsEdgeInfo.getOffsetPrefix(AdjListType.ordered_by_dest));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_dest/adj_list/offset/chunk0"),
                knowsEdgeInfo.getOffsetChunkPath(AdjListType.ordered_by_dest, 0));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_dest/adj_list/offset/chunk4"),
                knowsEdgeInfo.getOffsetChunkPath(AdjListType.ordered_by_dest, 4));
    }

    @Test
    public void testKnowsEdgePropertyGroup() {
        Assert.assertEquals(1, knowsEdgeInfo.getPropertyGroupNum());
        // edge properties group 1
        PropertyGroup propertyGroup = knowsEdgeInfo.getPropertyGroups().get(0);
        Assert.assertEquals("creationDate/", propertyGroup.getPrefix());
        Assert.assertEquals(URI.create("creationDate/"), propertyGroup.getBaseUri());
        Assert.assertEquals(FileType.CSV, propertyGroup.getFileType());
        Assert.assertEquals(
                URI.create("edge/person_knows_person/creationDate/"),
                knowsEdgeInfo.getPropertyGroupPrefix(propertyGroup));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/creationDate/chunk0"),
                knowsEdgeInfo.getPropertyGroupChunkPath(propertyGroup, 0));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/creationDate/chunk4"),
                knowsEdgeInfo.getPropertyGroupChunkPath(propertyGroup, 4));
        // edge properties in group 1
        Assert.assertNotNull(propertyGroup.getPropertyList());
        Assert.assertEquals(1, propertyGroup.getPropertyList().size());
        Property property = propertyGroup.getPropertyList().get(0);
        Assert.assertTrue(knowsEdgeInfo.hasProperty("creationDate"));
        Assert.assertEquals("creationDate", property.getName());
        Assert.assertEquals(DataType.STRING, property.getDataType());
        Assert.assertFalse(property.isPrimary());
        Assert.assertTrue(property.isNullable());
    }

    @Test
    public void testVersionParser() {
        // parser
        VersionInfo versionInfo = VersionParser.getVersion("gar/v1");
        Assert.assertEquals(1, versionInfo.getVersion());
        Assert.assertTrue(versionInfo.getUserDefinedTypes().isEmpty());
        Assert.assertTrue((versionInfo.checkType("int32")));
        Assert.assertFalse((versionInfo.checkType("date32")));

        versionInfo = VersionParser.getVersion("gar/v1 (t1,t2)");
        Assert.assertEquals(1, versionInfo.getVersion());
        Assert.assertEquals(List.of("t1", "t2"), versionInfo.getUserDefinedTypes());
        Assert.assertTrue(versionInfo.checkType("t1"));
        Assert.assertTrue(versionInfo.checkType("t2"));
        Assert.assertTrue((versionInfo.checkType("int32")));
        Assert.assertFalse((versionInfo.checkType("date32")));

        // dump
        versionInfo = new VersionInfo(1, new ArrayList<>());
        Assert.assertEquals(1, versionInfo.getVersion());
        Assert.assertTrue(versionInfo.getUserDefinedTypes().isEmpty());
        Assert.assertEquals("gar/v1", versionInfo.toString());

        versionInfo = new VersionInfo(2, List.of("t1", "t2"));
        Assert.assertEquals(2, versionInfo.getVersion());
        Assert.assertEquals(List.of("t1", "t2"), versionInfo.getUserDefinedTypes());
        Assert.assertEquals("gar/v2 (t1,t2)", versionInfo.toString());
    }
}
