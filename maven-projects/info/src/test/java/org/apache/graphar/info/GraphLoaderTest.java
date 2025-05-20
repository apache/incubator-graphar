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

import org.apache.graphar.info.loader.GraphLoader;
import org.apache.graphar.info.loader.LocalYamlGraphLoader;
import org.apache.graphar.proto.AdjListType;
import org.apache.graphar.proto.DataType;
import org.apache.graphar.proto.FileType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class GraphLoaderTest {

    @BeforeClass
    public static void init() {
        TestUtil.checkTestData();
    }

    @AfterClass
    public static void clean() {}

    @Test
    public void testLoad() {
        final GraphLoader graphLoader = new LocalYamlGraphLoader();
        final String GRAPH_PATH = TestUtil.getLdbcSampleGraphPath();
        GraphInfo graphInfo = null;
        try {
            graphInfo = graphLoader.load(GRAPH_PATH);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        testGraphInfo(graphInfo);
        testVertexInfo(graphInfo);
        testEdgeInfo(graphInfo);
    }

    private void testGraphInfo(GraphInfo graphInfo) {
        Assert.assertNotNull(graphInfo);
        Assert.assertEquals("ldbc_sample", graphInfo.getName());
        Assert.assertEquals("", graphInfo.getPrefix()); // is empty string?
        Assert.assertNotNull(graphInfo.getEdgeInfos());
        Assert.assertEquals(1, graphInfo.getEdgeInfos().size());
        Assert.assertNotNull(graphInfo.getVertexInfos());
        Assert.assertEquals(1, graphInfo.getVertexInfos().size());
    }

    private void testVertexInfo(GraphInfo graphInfo) {
        VertexInfo personVertexInfo = graphInfo.getVertexInfos().get(0);
        Assert.assertEquals("person", personVertexInfo.getType());
        Assert.assertEquals(100, personVertexInfo.getChunkSize());
        Assert.assertEquals("vertex/person/", personVertexInfo.getPrefix());
        Assert.assertEquals("vertex/person//vertex_count", personVertexInfo.getVerticesNumFilePath()); //one more '/'
        Assert.assertEquals("vertex/person//person.vertex.yaml", personVertexInfo.getVertexPath());
        Assert.assertNotNull(personVertexInfo.getPropertyGroups());
        Assert.assertEquals(2, personVertexInfo.getPropertyGroups().size());

        //vertex properties
        //group1 id
        PropertyGroup idPropertyGroup = personVertexInfo.getPropertyGroups().get(0);
        Assert.assertEquals("id/", idPropertyGroup.getPrefix());
        Assert.assertEquals(FileType.CSV, idPropertyGroup.getFileType());
        Assert.assertEquals("vertex/person//id/", personVertexInfo.getPropertyGroupPrefix(idPropertyGroup));
        Assert.assertEquals("vertex/person//id//chunk0", personVertexInfo.getPropertyGroupChunkPath(idPropertyGroup, 0));
        Assert.assertEquals("vertex/person//id//chunk4", personVertexInfo.getPropertyGroupChunkPath(idPropertyGroup, 4));
        Assert.assertNotNull(idPropertyGroup.getPropertyList());
        Assert.assertEquals(1, idPropertyGroup.getPropertyList().size());
        Property idProperty = idPropertyGroup.getPropertyList().get(0);
        Assert.assertTrue(personVertexInfo.hasProperty("id"));
        Assert.assertEquals("id", idProperty.getName());
        Assert.assertEquals(DataType.INT64, idProperty.getDataType());
        Assert.assertTrue(idProperty.isPrimary());
        Assert.assertFalse(idProperty.isNullable());
        //group2 firstName_lastName_gender
        PropertyGroup firstName_lastName_gender = personVertexInfo.getPropertyGroups().get(1);
        Assert.assertEquals("firstName_lastName_gender/", firstName_lastName_gender.getPrefix());
        Assert.assertEquals(FileType.CSV, firstName_lastName_gender.getFileType());
        Assert.assertEquals("vertex/person//firstName_lastName_gender/", personVertexInfo.getPropertyGroupPrefix(firstName_lastName_gender));
        Assert.assertEquals("vertex/person//firstName_lastName_gender//chunk0", personVertexInfo.getPropertyGroupChunkPath(firstName_lastName_gender, 0));
        Assert.assertEquals("vertex/person//firstName_lastName_gender//chunk4", personVertexInfo.getPropertyGroupChunkPath(firstName_lastName_gender, 4));
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

    private void testEdgeInfo(GraphInfo graphInfo) {
        EdgeInfo knowsEdge = graphInfo.getEdgeInfos().get(0);
        Assert.assertEquals("knows", knowsEdge.getEdgeLabel());
        Assert.assertEquals(1024, knowsEdge.getChunkSize());
        Assert.assertEquals("person", knowsEdge.getSrcLabel());
        Assert.assertEquals(100, knowsEdge.getSrcChunkSize());
        Assert.assertEquals("person", knowsEdge.getDstLabel());
        Assert.assertEquals(100, knowsEdge.getDstChunkSize());
        Assert.assertFalse(knowsEdge.isDirected());
        Assert.assertEquals("person_knows_person", knowsEdge.getConcat());
        Assert.assertEquals("edge/person_knows_person/", knowsEdge.getPrefix());
        Assert.assertEquals("edge/person_knows_person//person_knows_person.edge.yaml", knowsEdge.getEdgePath());

        //edge adjacentList
        Assert.assertEquals(2, knowsEdge.getAdjacentLists().size());
        AdjacentList adjOrderBySource = knowsEdge.getAdjacentList(AdjListType.ORDERED_BY_SOURCE);
        Assert.assertEquals(FileType.CSV, adjOrderBySource.getFileType());
        Assert.assertEquals(AdjListType.ORDERED_BY_SOURCE, adjOrderBySource.getType());
        Assert.assertEquals("ordered_by_source/", adjOrderBySource.getPrefix());
        Assert.assertEquals("edge/person_knows_person//ordered_by_source//adj_list/vertex_count", knowsEdge.getVerticesNumFilePath(AdjListType.ORDERED_BY_SOURCE));
        Assert.assertEquals("edge/person_knows_person//ordered_by_source//adj_list/edge_count0", knowsEdge.getEdgesNumFilePath(AdjListType.ORDERED_BY_SOURCE, 0));
        Assert.assertEquals("edge/person_knows_person//ordered_by_source//adj_list/edge_count4", knowsEdge.getEdgesNumFilePath(AdjListType.ORDERED_BY_SOURCE, 4));
        Assert.assertEquals("edge/person_knows_person//ordered_by_source//adj_list", knowsEdge.getAdjacentListPrefix(AdjListType.ORDERED_BY_SOURCE));
        Assert.assertEquals("edge/person_knows_person//ordered_by_source//adj_list/chunk0", knowsEdge.getAdjacentListChunkPath(AdjListType.ORDERED_BY_SOURCE, 0));
        Assert.assertEquals("edge/person_knows_person//ordered_by_source//adj_list/chunk4", knowsEdge.getAdjacentListChunkPath(AdjListType.ORDERED_BY_SOURCE, 4));
        Assert.assertEquals("edge/person_knows_person//ordered_by_source//adj_list/offset", knowsEdge.getOffsetPrefix(AdjListType.ORDERED_BY_SOURCE));
        Assert.assertEquals("edge/person_knows_person//ordered_by_source//adj_list/offset/chunk0", knowsEdge.getOffsetChunkPath(AdjListType.ORDERED_BY_SOURCE, 0));
        Assert.assertEquals("edge/person_knows_person//ordered_by_source//adj_list/offset/chunk4", knowsEdge.getOffsetChunkPath(AdjListType.ORDERED_BY_SOURCE, 4));
        AdjacentList adjOrderByDestination = knowsEdge.getAdjacentList(AdjListType.ORDERED_BY_DESTINATION);
        Assert.assertEquals(FileType.CSV, adjOrderByDestination.getFileType());
        Assert.assertEquals(AdjListType.ORDERED_BY_DESTINATION, adjOrderByDestination.getType());
        Assert.assertEquals("ordered_by_dest/", adjOrderByDestination.getPrefix());
        Assert.assertEquals("edge/person_knows_person//ordered_by_dest//adj_list/vertex_count", knowsEdge.getVerticesNumFilePath(AdjListType.ORDERED_BY_DESTINATION));
        Assert.assertEquals("edge/person_knows_person//ordered_by_dest//adj_list/edge_count0", knowsEdge.getEdgesNumFilePath(AdjListType.ORDERED_BY_DESTINATION, 0));
        Assert.assertEquals("edge/person_knows_person//ordered_by_dest//adj_list/edge_count4", knowsEdge.getEdgesNumFilePath(AdjListType.ORDERED_BY_DESTINATION, 4));
        Assert.assertEquals("edge/person_knows_person//ordered_by_dest//adj_list", knowsEdge.getAdjacentListPrefix(AdjListType.ORDERED_BY_DESTINATION));
        Assert.assertEquals("edge/person_knows_person//ordered_by_dest//adj_list/chunk0", knowsEdge.getAdjacentListChunkPath(AdjListType.ORDERED_BY_DESTINATION, 0));
        Assert.assertEquals("edge/person_knows_person//ordered_by_dest//adj_list/chunk4", knowsEdge.getAdjacentListChunkPath(AdjListType.ORDERED_BY_DESTINATION, 4));
        Assert.assertEquals("edge/person_knows_person//ordered_by_dest//adj_list/offset", knowsEdge.getOffsetPrefix(AdjListType.ORDERED_BY_DESTINATION));
        Assert.assertEquals("edge/person_knows_person//ordered_by_dest//adj_list/offset/chunk0", knowsEdge.getOffsetChunkPath(AdjListType.ORDERED_BY_DESTINATION, 0));
        Assert.assertEquals("edge/person_knows_person//ordered_by_dest//adj_list/offset/chunk4", knowsEdge.getOffsetChunkPath(AdjListType.ORDERED_BY_DESTINATION, 4));

        //edge properties
        Assert.assertEquals(1, knowsEdge.getPropertyGroupNum());
        PropertyGroup propertyGroup = knowsEdge.getPropertyGroups().get(0);
        Assert.assertEquals("creationDate/", propertyGroup.getPrefix());
        Assert.assertEquals(FileType.CSV, propertyGroup.getFileType());
        Assert.assertEquals("edge/person_knows_person//creationDate/", knowsEdge.getPropertyGroupPrefix(propertyGroup));
        Assert.assertEquals("edge/person_knows_person//creationDate//chunk0", knowsEdge.getPropertyGroupChunkPath(propertyGroup, 0));
        Assert.assertEquals("edge/person_knows_person//creationDate//chunk4", knowsEdge.getPropertyGroupChunkPath(propertyGroup, 4));
        Assert.assertNotNull(propertyGroup.getPropertyList());
        Assert.assertEquals(1, propertyGroup.getPropertyList().size());
        Property property = propertyGroup.getPropertyList().get(0);
        Assert.assertTrue(knowsEdge.hasProperty("creationDate"));
        Assert.assertEquals("creationDate", property.getName());
        Assert.assertEquals(DataType.STRING, property.getDataType());
        Assert.assertFalse(property.isPrimary());
        Assert.assertTrue(property.isNullable());
    }
}
