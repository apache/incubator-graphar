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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.graphar.info.type.AdjListType;
import org.apache.graphar.info.type.DataType;
import org.apache.graphar.info.type.FileType;
import org.junit.Assert;
import org.junit.Test;

public class MultiFormatGraphInfoTest {

    @Test
    public void testGraphInfoWithEmptyVertexAndEdgeInfos() {
        List<VertexInfo> emptyVertices = new ArrayList<>();
        List<EdgeInfo> emptyEdges = new ArrayList<>();
        String version = "gar/v1";

        GraphInfo graphInfo =
                new GraphInfo("emptyGraph", emptyVertices, emptyEdges, "test/", version);

        Assert.assertEquals("emptyGraph", graphInfo.getName());
        Assert.assertEquals(0, graphInfo.getVertexInfos().size());
        Assert.assertEquals(0, graphInfo.getEdgeInfos().size());
        Assert.assertEquals("test/", graphInfo.getPrefix());
        Assert.assertEquals("gar/v1", graphInfo.getVersion().toString());
    }

    @Test
    public void testGraphInfoWithNullPrefix() {
        List<VertexInfo> vertices = new ArrayList<>();
        List<EdgeInfo> edges = new ArrayList<>();
        String version = "gar/v1";

        GraphInfo graphInfo = new GraphInfo("testGraph", vertices, edges, (String) null, version);

        Assert.assertEquals("testGraph", graphInfo.getName());
        Assert.assertNull(graphInfo.getPrefix());
    }

    @Test
    public void testVertexInfoWithEmptyPropertyGroups() {
        PropertyGroup emptyPg = new PropertyGroup(Collections.emptyList(), FileType.CSV, "empty/");
        List<PropertyGroup> propertyGroups = Arrays.asList(emptyPg);
        String version = "gar/v1";

        VertexInfo vertexInfo =
                new VertexInfo("testVertex", 100, propertyGroups, "vertex/", version);

        Assert.assertEquals("testVertex", vertexInfo.getType());
        Assert.assertEquals(100, vertexInfo.getChunkSize());
        Assert.assertEquals(1, vertexInfo.getPropertyGroups().size());
        Assert.assertEquals(0, vertexInfo.getPropertyGroups().get(0).size());
    }

    @Test
    public void testVertexInfoWithZeroChunkSize() {
        Property prop = new Property("id", DataType.INT32, true, false);
        PropertyGroup pg = new PropertyGroup(Arrays.asList(prop), FileType.CSV, "test/");
        List<PropertyGroup> propertyGroups = Arrays.asList(pg);
        String version = "gar/v1";

        VertexInfo vertexInfo = new VertexInfo("testVertex", 0, propertyGroups, "vertex/", version);

        Assert.assertEquals("testVertex", vertexInfo.getType());
        Assert.assertEquals(0, vertexInfo.getChunkSize());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testVertexInfoWithNegativeChunkSize() {
        Property prop = new Property("id", DataType.INT32, true, false);
        PropertyGroup pg = new PropertyGroup(Arrays.asList(prop), FileType.CSV, "test/");
        List<PropertyGroup> propertyGroups = Arrays.asList(pg);
        String version = "gar/v1";

        new VertexInfo("testVertex", -100, propertyGroups, "vertex/", version);
    }

    @Test
    public void testEdgeInfoWithEmptyAdjacentLists() {
        List<AdjacentList> emptyAdjLists = new ArrayList<>();
        List<PropertyGroup> emptyPropertyGroups = new ArrayList<>();
        String version = "gar/v1";

        EdgeInfo edgeInfo =
                new EdgeInfo(
                        "srcType",
                        "edgeType",
                        "dstType",
                        50,
                        100,
                        200,
                        true,
                        "edge/",
                        version,
                        emptyAdjLists,
                        emptyPropertyGroups);

        Assert.assertEquals("srcType", edgeInfo.getSrcType());
        Assert.assertEquals("edgeType", edgeInfo.getEdgeType());
        Assert.assertEquals("dstType", edgeInfo.getDstType());
        Assert.assertEquals(50, edgeInfo.getChunkSize());
        Assert.assertEquals(0, edgeInfo.getAdjacentLists().size());
        Assert.assertEquals(0, edgeInfo.getPropertyGroupNum());
    }

    @Test
    public void testEdgeInfoWithZeroChunkSize() {
        AdjacentList adjList =
                new AdjacentList(AdjListType.unordered_by_source, FileType.CSV, "adj/");
        List<AdjacentList> adjLists = Arrays.asList(adjList);
        List<PropertyGroup> emptyPropertyGroups = new ArrayList<>();
        String version = "gar/v1";

        EdgeInfo edgeInfo =
                new EdgeInfo(
                        "srcType",
                        "edgeType",
                        "dstType",
                        0,
                        100,
                        200,
                        true,
                        "edge/",
                        version,
                        adjLists,
                        emptyPropertyGroups);

        Assert.assertEquals(0, edgeInfo.getChunkSize());
    }

    @Test
    public void testPropertyGroupWithMixedDataTypes() {
        Property boolProp = new Property("isActive", DataType.BOOL, false, false);
        Property int32Prop = new Property("count", DataType.INT32, false, true);
        Property int64Prop = new Property("bigCount", DataType.INT64, true, false);
        Property floatProp = new Property("score", DataType.FLOAT, false, true);
        Property doubleProp = new Property("precision", DataType.DOUBLE, false, true);
        Property stringProp = new Property("name", DataType.STRING, false, true);
        Property listProp = new Property("tags", DataType.LIST, false, true);

        List<Property> mixedProps =
                Arrays.asList(
                        boolProp,
                        int32Prop,
                        int64Prop,
                        floatProp,
                        doubleProp,
                        stringProp,
                        listProp);

        PropertyGroup pg = new PropertyGroup(mixedProps, FileType.PARQUET, "mixed/");

        Assert.assertEquals(7, pg.size());
        Assert.assertEquals(DataType.BOOL, pg.getPropertyMap().get("isActive").getDataType());
        Assert.assertEquals(DataType.INT32, pg.getPropertyMap().get("count").getDataType());
        Assert.assertEquals(DataType.INT64, pg.getPropertyMap().get("bigCount").getDataType());
        Assert.assertEquals(DataType.FLOAT, pg.getPropertyMap().get("score").getDataType());
        Assert.assertEquals(DataType.DOUBLE, pg.getPropertyMap().get("precision").getDataType());
        Assert.assertEquals(DataType.STRING, pg.getPropertyMap().get("name").getDataType());
        Assert.assertEquals(DataType.LIST, pg.getPropertyMap().get("tags").getDataType());
    }

    @Test
    public void testPropertyGroupWithDifferentFileFormats() {
        Property prop = new Property("test", DataType.STRING, false, true);
        List<Property> properties = Arrays.asList(prop);

        // Test all file formats
        PropertyGroup csvPg = new PropertyGroup(properties, FileType.CSV, "csv/");
        PropertyGroup parquetPg = new PropertyGroup(properties, FileType.PARQUET, "parquet/");
        PropertyGroup orcPg = new PropertyGroup(properties, FileType.ORC, "orc/");

        Assert.assertEquals(FileType.CSV, csvPg.getFileType());
        Assert.assertEquals(FileType.PARQUET, parquetPg.getFileType());
        Assert.assertEquals(FileType.ORC, orcPg.getFileType());
    }

    @Test
    public void testAdjacentListWithAllTypesAndFormats() {
        // Test all combinations of AdjListType and FileType
        AdjListType[] adjTypes = {
            AdjListType.unordered_by_source, AdjListType.unordered_by_dest,
            AdjListType.ordered_by_source, AdjListType.ordered_by_dest
        };

        FileType[] fileTypes = {FileType.CSV, FileType.PARQUET, FileType.ORC};

        for (AdjListType adjType : adjTypes) {
            for (FileType fileType : fileTypes) {
                AdjacentList adjList = new AdjacentList(adjType, fileType, "test/");
                Assert.assertEquals(adjType, adjList.getType());
                Assert.assertEquals(fileType, adjList.getFileType());
                Assert.assertEquals("test/", adjList.getPrefix());
            }
        }
    }

    @Test
    public void testVersionInfoWithExtensiveUserDefinedTypes() {
        List<String> manyTypes =
                Arrays.asList(
                        "type1",
                        "type2",
                        "type3",
                        "type4",
                        "type5",
                        "type6",
                        "type7",
                        "type8",
                        "type9",
                        "type10",
                        "customDate",
                        "customTime",
                        "customGeometry",
                        "customJSON",
                        "customXML",
                        "user-defined-1",
                        "user_defined_2",
                        "user.defined.3");

        VersionInfo versionInfo = new VersionInfo(1, manyTypes);

        Assert.assertEquals(1, versionInfo.getVersion());
        Assert.assertEquals(manyTypes, versionInfo.getUserDefinedTypes());

        // Test all user-defined types are supported
        for (String type : manyTypes) {
            Assert.assertTrue("Type " + type + " should be supported", versionInfo.checkType(type));
        }

        // Built-in types should still work
        Assert.assertTrue(versionInfo.checkType("int32"));
        Assert.assertTrue(versionInfo.checkType("string"));

        // Non-existent types should not work
        Assert.assertFalse(versionInfo.checkType("nonExistentType"));
    }

    @Test
    public void testPropertyWithExtremeValues() {
        // Test properties with edge case names
        Property emptyNameProp = new Property("", DataType.STRING, false, true);
        Property longNameProp =
                new Property(
                        "verylongpropertynamethatgoesonyesitdoes", DataType.INT32, false, false);
        Property specialCharsProp =
                new Property("prop-with_special.chars", DataType.DOUBLE, true, false);

        Assert.assertEquals("", emptyNameProp.getName());
        Assert.assertEquals("verylongpropertynamethatgoesonyesitdoes", longNameProp.getName());
        Assert.assertEquals("prop-with_special.chars", specialCharsProp.getName());

        // Test all combinations of primary/nullable flags
        Property primaryNullable = new Property("test1", DataType.INT32, true, true);
        Property primaryNotNullable = new Property("test2", DataType.INT32, true, false);
        Property notPrimaryNullable = new Property("test3", DataType.INT32, false, true);
        Property notPrimaryNotNullable = new Property("test4", DataType.INT32, false, false);

        Assert.assertTrue(primaryNullable.isPrimary() && primaryNullable.isNullable());
        Assert.assertTrue(primaryNotNullable.isPrimary() && !primaryNotNullable.isNullable());
        Assert.assertTrue(!notPrimaryNullable.isPrimary() && notPrimaryNullable.isNullable());
        Assert.assertTrue(
                !notPrimaryNotNullable.isPrimary() && !notPrimaryNotNullable.isNullable());
    }

    @Test
    public void testLargeGraphInfoConstruction() {
        // Create a graph with many vertices and edges
        List<VertexInfo> vertices = new ArrayList<>();
        List<EdgeInfo> edges = new ArrayList<>();
        String version = "gar/v1";

        // Create 10 vertex types
        for (int i = 0; i < 10; i++) {
            Property prop = new Property("id" + i, DataType.INT64, true, false);
            PropertyGroup pg =
                    new PropertyGroup(Arrays.asList(prop), FileType.PARQUET, "vertex" + i + "/");
            VertexInfo vertex =
                    new VertexInfo(
                            "vertex" + i, 1000, Arrays.asList(pg), "vertex" + i + "/", version);
            vertices.add(vertex);
        }

        // Create edges between vertex types
        for (int i = 0; i < 5; i++) {
            AdjacentList adjList =
                    new AdjacentList(
                            AdjListType.ordered_by_source, FileType.PARQUET, "edge" + i + "/");
            Property edgeProp = new Property("weight", DataType.DOUBLE, false, true);
            PropertyGroup edgePg =
                    new PropertyGroup(Arrays.asList(edgeProp), FileType.PARQUET, "edge" + i + "/");
            EdgeInfo edge =
                    new EdgeInfo(
                            "vertex" + i,
                            "edge" + i,
                            "vertex" + (i + 1),
                            500,
                            1000,
                            1000,
                            true,
                            "edge" + i + "/",
                            version,
                            Arrays.asList(adjList),
                            Arrays.asList(edgePg));
            edges.add(edge);
        }

        GraphInfo graphInfo = new GraphInfo("largeGraph", vertices, edges, "large/", version);

        Assert.assertEquals("largeGraph", graphInfo.getName());
        Assert.assertEquals(10, graphInfo.getVertexInfos().size());
        Assert.assertEquals(5, graphInfo.getEdgeInfos().size());
        Assert.assertEquals("large/", graphInfo.getPrefix());
    }
}
