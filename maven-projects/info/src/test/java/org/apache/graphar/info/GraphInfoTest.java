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
import java.util.Arrays;
import java.util.List;
import org.apache.graphar.info.loader.GraphInfoLoader;
import org.apache.graphar.info.loader.impl.LocalFileSystemStreamGraphInfoLoader;
import org.apache.graphar.info.type.AdjListType;
import org.apache.graphar.info.type.Cardinality;
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
    // test not exist property group
    private static final PropertyGroup notExistPg =
            new PropertyGroup(
                    List.of(new Property("not_exist", DataType.INT64, true, false)),
                    FileType.CSV,
                    "not_exist/");

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
                        .resolve(knowsEdgeInfo.getAdjacentListUri(AdjListType.ordered_by_source)));
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
        Assert.assertEquals(1, graphInfo.getEdgeInfoNum());
        Assert.assertNotNull(graphInfo.getVertexInfos());
        Assert.assertEquals(1, graphInfo.getVertexInfos().size());
        Assert.assertEquals(1, graphInfo.getVertexInfoNum());
        Assert.assertEquals(personVertexInfo, graphInfo.getVertexInfo("person"));
        IllegalArgumentException illegalArgumentException =
                Assert.assertThrows(
                        IllegalArgumentException.class, () -> graphInfo.getVertexInfo("not_exist"));
        Assert.assertEquals(
                "Vertex type not_exist not exist in graph ldbc_sample",
                illegalArgumentException.getMessage());
        Assert.assertEquals(knowsEdgeInfo, graphInfo.getEdgeInfo("person", "knows", "person"));
        illegalArgumentException =
                Assert.assertThrows(
                        IllegalArgumentException.class,
                        () -> graphInfo.getEdgeInfo("person", "not_knows", "person"));
        Assert.assertEquals(
                "Edge type person_not_knows_person not exist in graph ldbc_sample",
                illegalArgumentException.getMessage());
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
        Assert.assertEquals(2, personVertexInfo.getPropertyGroupNum());
        PropertyGroup idPropertyGroup = personVertexInfo.getPropertyGroups().get(0);
        Assert.assertEquals("id/", idPropertyGroup.getPrefix());
        Assert.assertEquals(URI.create("id/"), idPropertyGroup.getBaseUri());
        Assert.assertEquals(FileType.CSV, idPropertyGroup.getFileType());
        Assert.assertEquals(
                URI.create("vertex/person/id/"),
                personVertexInfo.getPropertyGroupUri(idPropertyGroup));
        Assert.assertEquals(
                URI.create("vertex/person/id/chunk0"),
                personVertexInfo.getPropertyGroupChunkUri(idPropertyGroup, 0));
        Assert.assertEquals(
                URI.create("vertex/person/id/chunk4"),
                personVertexInfo.getPropertyGroupChunkUri(idPropertyGroup, 4));
        IllegalArgumentException illegalArgumentException =
                Assert.assertThrows(
                        IllegalArgumentException.class,
                        () -> personVertexInfo.getPropertyGroupUri(notExistPg));
        Assert.assertEquals(
                "Property group "
                        + notExistPg
                        + " does not exist in the vertex "
                        + personVertexInfo.getType(),
                illegalArgumentException.getMessage());
        Assert.assertEquals(idPropertyGroup, personVertexInfo.getPropertyGroup("id"));
        illegalArgumentException =
                Assert.assertThrows(
                        IllegalArgumentException.class,
                        () -> personVertexInfo.getPropertyGroup("not_exist"));
        Assert.assertEquals(
                "Property not_exist does not exist", illegalArgumentException.getMessage());
        illegalArgumentException =
                Assert.assertThrows(
                        IllegalArgumentException.class,
                        () -> personVertexInfo.getPropertyGroup(null));
        Assert.assertEquals("Property name is null", illegalArgumentException.getMessage());
        Assert.assertNotNull(idPropertyGroup.getPropertyList());
        Assert.assertEquals(1, idPropertyGroup.getPropertyList().size());
        Property idProperty = idPropertyGroup.getPropertyList().get(0);
        Assert.assertTrue(personVertexInfo.hasProperty("id"));
        Assert.assertEquals("id", idProperty.getName());
        Assert.assertEquals(DataType.INT64, idProperty.getDataType());
        Assert.assertEquals(DataType.INT64, personVertexInfo.getPropertyType("id"));
        Assert.assertEquals(Cardinality.SINGLE, idProperty.getCardinality());
        Assert.assertEquals(Cardinality.SINGLE, personVertexInfo.getCardinality("id"));
        Assert.assertTrue(idProperty.isPrimary());
        Assert.assertTrue(personVertexInfo.isPrimaryKey("id"));
        Assert.assertFalse(idProperty.isNullable());
        Assert.assertFalse(personVertexInfo.isNullableKey("id"));
        // group2 firstName_lastName_gender
        PropertyGroup firstName_lastName_gender = personVertexInfo.getPropertyGroups().get(1);
        Assert.assertEquals("firstName_lastName_gender/", firstName_lastName_gender.getPrefix());
        Assert.assertEquals(
                URI.create("firstName_lastName_gender/"), firstName_lastName_gender.getBaseUri());
        Assert.assertEquals(FileType.CSV, firstName_lastName_gender.getFileType());
        Assert.assertEquals(
                URI.create("vertex/person/firstName_lastName_gender/"),
                personVertexInfo.getPropertyGroupUri(firstName_lastName_gender));
        Assert.assertEquals(
                URI.create("vertex/person/firstName_lastName_gender/chunk0"),
                personVertexInfo.getPropertyGroupChunkUri(firstName_lastName_gender, 0));
        Assert.assertEquals(
                URI.create("vertex/person/firstName_lastName_gender/chunk4"),
                personVertexInfo.getPropertyGroupChunkUri(firstName_lastName_gender, 4));
        Assert.assertEquals(
                URI.create("vertex/person/vertex_count"), personVertexInfo.getVerticesNumFileUri());
        Assert.assertNotNull(firstName_lastName_gender.getPropertyList());
        Assert.assertEquals(3, firstName_lastName_gender.getPropertyList().size());
        Property firstNameProperty = firstName_lastName_gender.getPropertyList().get(0);
        Assert.assertTrue(personVertexInfo.hasProperty("firstName"));
        Assert.assertEquals("firstName", firstNameProperty.getName());
        Assert.assertEquals(DataType.STRING, firstNameProperty.getDataType());
        Assert.assertEquals(Cardinality.SINGLE, firstNameProperty.getCardinality());
        Assert.assertFalse(firstNameProperty.isPrimary());
        Assert.assertTrue(firstNameProperty.isNullable());
        Property lastNameProperty = firstName_lastName_gender.getPropertyList().get(1);
        Assert.assertTrue(personVertexInfo.hasProperty("lastName"));
        Assert.assertEquals("lastName", lastNameProperty.getName());
        Assert.assertEquals(DataType.STRING, lastNameProperty.getDataType());
        Assert.assertEquals(Cardinality.SINGLE, lastNameProperty.getCardinality());
        Assert.assertFalse(lastNameProperty.isPrimary());
        Assert.assertTrue(lastNameProperty.isNullable());
        Property genderProperty = firstName_lastName_gender.getPropertyList().get(2);
        Assert.assertTrue(personVertexInfo.hasProperty("gender"));
        Assert.assertEquals("gender", genderProperty.getName());
        Assert.assertEquals(DataType.STRING, genderProperty.getDataType());
        Assert.assertEquals(Cardinality.SINGLE, genderProperty.getCardinality());
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
        Assert.assertTrue(knowsEdgeInfo.hasAdjListType(AdjListType.ordered_by_source));
        Assert.assertTrue(knowsEdgeInfo.hasAdjListType(AdjListType.ordered_by_dest));
        Assert.assertFalse(knowsEdgeInfo.hasAdjListType(AdjListType.unordered_by_source));
        Assert.assertFalse(knowsEdgeInfo.hasAdjListType(AdjListType.unordered_by_dest));
        // test ordered by source adjacency list
        AdjacentList adjOrderBySource =
                knowsEdgeInfo.getAdjacentList(AdjListType.ordered_by_source);
        Assert.assertEquals(FileType.CSV, adjOrderBySource.getFileType());
        Assert.assertEquals(AdjListType.ordered_by_source, adjOrderBySource.getType());
        Assert.assertEquals("ordered_by_source/", adjOrderBySource.getPrefix());
        Assert.assertEquals(URI.create("ordered_by_source/"), adjOrderBySource.getBaseUri());
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/adj_list/vertex_count"),
                knowsEdgeInfo.getVerticesNumFileUri(AdjListType.ordered_by_source));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/adj_list/edge_count0"),
                knowsEdgeInfo.getEdgesNumFileUri(AdjListType.ordered_by_source, 0));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/adj_list/edge_count4"),
                knowsEdgeInfo.getEdgesNumFileUri(AdjListType.ordered_by_source, 4));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/adj_list/"),
                knowsEdgeInfo.getAdjacentListUri(AdjListType.ordered_by_source));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/adj_list/chunk0"),
                knowsEdgeInfo.getAdjacentListChunkUri(AdjListType.ordered_by_source, 0));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/adj_list/chunk4"),
                knowsEdgeInfo.getAdjacentListChunkUri(AdjListType.ordered_by_source, 4));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/adj_list/offset/"),
                knowsEdgeInfo.getOffsetUri(AdjListType.ordered_by_source));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/adj_list/offset/chunk0"),
                knowsEdgeInfo.getOffsetChunkUri(AdjListType.ordered_by_source, 0));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/adj_list/offset/chunk4"),
                knowsEdgeInfo.getOffsetChunkUri(AdjListType.ordered_by_source, 4));

        // test ordered by destination adjacency list
        AdjacentList adjOrderByDestination =
                knowsEdgeInfo.getAdjacentList(AdjListType.ordered_by_dest);
        Assert.assertEquals(FileType.CSV, adjOrderByDestination.getFileType());
        Assert.assertEquals(AdjListType.ordered_by_dest, adjOrderByDestination.getType());
        Assert.assertEquals("ordered_by_dest/", adjOrderByDestination.getPrefix());
        Assert.assertEquals(URI.create("ordered_by_dest/"), adjOrderByDestination.getBaseUri());
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_dest/adj_list/vertex_count"),
                knowsEdgeInfo.getVerticesNumFileUri(AdjListType.ordered_by_dest));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_dest/adj_list/edge_count0"),
                knowsEdgeInfo.getEdgesNumFileUri(AdjListType.ordered_by_dest, 0));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_dest/adj_list/edge_count4"),
                knowsEdgeInfo.getEdgesNumFileUri(AdjListType.ordered_by_dest, 4));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_dest/adj_list/"),
                knowsEdgeInfo.getAdjacentListUri(AdjListType.ordered_by_dest));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_dest/adj_list/chunk0"),
                knowsEdgeInfo.getAdjacentListChunkUri(AdjListType.ordered_by_dest, 0));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_dest/adj_list/chunk4"),
                knowsEdgeInfo.getAdjacentListChunkUri(AdjListType.ordered_by_dest, 4));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_dest/adj_list/offset/"),
                knowsEdgeInfo.getOffsetUri(AdjListType.ordered_by_dest));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_dest/adj_list/offset/chunk0"),
                knowsEdgeInfo.getOffsetChunkUri(AdjListType.ordered_by_dest, 0));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_dest/adj_list/offset/chunk4"),
                knowsEdgeInfo.getOffsetChunkUri(AdjListType.ordered_by_dest, 4));
    }

    @Test
    public void testKnowsEdgePropertyGroup() {
        Assert.assertEquals(1, knowsEdgeInfo.getPropertyGroupNum());
        // edge properties group 1
        PropertyGroup propertyGroup = knowsEdgeInfo.getPropertyGroups().get(0);
        IllegalArgumentException illegalArgumentException =
                Assert.assertThrows(
                        IllegalArgumentException.class,
                        () -> knowsEdgeInfo.getPropertyGroupUri(notExistPg));
        Assert.assertEquals(
                "Property group "
                        + notExistPg
                        + " does not exist in the edge "
                        + knowsEdgeInfo.getConcat(),
                illegalArgumentException.getMessage());
        Assert.assertEquals(propertyGroup, knowsEdgeInfo.getPropertyGroup("creationDate"));
        illegalArgumentException =
                Assert.assertThrows(
                        IllegalArgumentException.class,
                        () -> knowsEdgeInfo.getPropertyGroup("not_exist"));
        Assert.assertEquals(
                "Property not_exist does not exist", illegalArgumentException.getMessage());
        illegalArgumentException =
                Assert.assertThrows(
                        IllegalArgumentException.class, () -> knowsEdgeInfo.getPropertyGroup(null));
        Assert.assertEquals("Property name is null", illegalArgumentException.getMessage());
        Assert.assertEquals("creationDate/", propertyGroup.getPrefix());
        Assert.assertEquals(URI.create("creationDate/"), propertyGroup.getBaseUri());
        Assert.assertEquals(FileType.CSV, propertyGroup.getFileType());
        Assert.assertEquals(
                URI.create("edge/person_knows_person/creationDate/"),
                knowsEdgeInfo.getPropertyGroupUri(propertyGroup));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/creationDate/chunk0"),
                knowsEdgeInfo.getPropertyGroupChunkUri(propertyGroup, 0));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/creationDate/chunk4"),
                knowsEdgeInfo.getPropertyGroupChunkUri(propertyGroup, 4));
        // edge properties in group 1
        Assert.assertNotNull(propertyGroup.getPropertyList());
        Assert.assertEquals(1, propertyGroup.getPropertyList().size());
        Property property = propertyGroup.getPropertyList().get(0);
        Assert.assertTrue(knowsEdgeInfo.hasProperty("creationDate"));
        Assert.assertEquals("creationDate", property.getName());
        Assert.assertEquals(DataType.STRING, property.getDataType());
        Assert.assertEquals(DataType.STRING, knowsEdgeInfo.getPropertyType("creationDate"));
        Assert.assertEquals(Cardinality.SINGLE, property.getCardinality());
        Assert.assertFalse(property.isPrimary());
        Assert.assertFalse(knowsEdgeInfo.isPrimaryKey("creationDate"));
        Assert.assertTrue(property.isNullable());
        Assert.assertTrue(knowsEdgeInfo.isNullableKey("creationDate"));
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

    @Test
    public void testIsValidated() {
        // Test valid graph info from real test data
        Assert.assertTrue(graphInfo.isValidated());

        // Test invalid graph info with empty name
        GraphInfo emptyNameGraphInfo =
                new GraphInfo(
                        "",
                        graphInfo.getVertexInfos(),
                        graphInfo.getEdgeInfos(),
                        graphInfo.getBaseUri(),
                        graphInfo.getVersion().toString());
        Assert.assertFalse(emptyNameGraphInfo.isValidated());

        // Test invalid graph info with null base URI
        GraphInfo nullBaseUriGraphInfo =
                new GraphInfo(
                        "test",
                        graphInfo.getVertexInfos(),
                        graphInfo.getEdgeInfos(),
                        (URI) null,
                        graphInfo.getVersion().toString());
        Assert.assertFalse(nullBaseUriGraphInfo.isValidated());

        // Test invalid graph info with invalid vertex info
        VertexInfo invalidVertexInfo =
                new VertexInfo("", 100, Arrays.asList(TestUtil.pg1), "vertex/person/", "gar/v1");
        GraphInfo invalidVertexGraphInfo =
                new GraphInfo(
                        "test",
                        Arrays.asList(invalidVertexInfo),
                        graphInfo.getEdgeInfos(),
                        graphInfo.getBaseUri(),
                        graphInfo.getVersion().toString());
        Assert.assertFalse(invalidVertexGraphInfo.isValidated());

        // Test invalid graph info with invalid edge info
        EdgeInfo invalidEdgeInfo =
                EdgeInfo.builder()
                        .srcType("")
                        .edgeType("knows")
                        .dstType("person")
                        .propertyGroups(new PropertyGroups(List.of(TestUtil.pg3)))
                        .adjacentLists(List.of(TestUtil.orderedBySource))
                        .chunkSize(1024)
                        .srcChunkSize(100)
                        .dstChunkSize(100)
                        .directed(false)
                        .prefix("edge/person_knows_person/")
                        .version("gar/v1")
                        .build();
        GraphInfo invalidEdgeGraphInfo =
                new GraphInfo(
                        "test",
                        graphInfo.getVertexInfos(),
                        Arrays.asList(invalidEdgeInfo),
                        graphInfo.getBaseUri(),
                        graphInfo.getVersion().toString());
        Assert.assertFalse(invalidEdgeGraphInfo.isValidated());
    }
}
