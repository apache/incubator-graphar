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

import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.graphar.info.type.AdjListType;
import org.junit.Assert;

/**
 * Utility class for common test verifications. Separates verification logic from test methods for
 * better readability.
 */
public class TestVerificationUtils {

    /** Verifies that all expected files for a GraphInfo were created in the given directory. */
    public static void verifyGraphInfoFilesSaved(String saveDirectory, GraphInfo graphInfo) {
        verifyGraphFileExists(saveDirectory, graphInfo);
        verifyVertexFilesExist(saveDirectory, graphInfo);
        verifyEdgeFilesExist(saveDirectory, graphInfo);
    }

    /** Verifies that the main graph YAML file exists. */
    public static void verifyGraphFileExists(String saveDirectory, GraphInfo graphInfo) {
        String graphFilePath = saveDirectory + "/" + graphInfo.getName() + ".graph.yaml";
        Assert.assertTrue(
                "Graph file should exist: " + graphFilePath, new File(graphFilePath).exists());
    }

    /** Verifies that all vertex YAML files exist. */
    public static void verifyVertexFilesExist(String saveDirectory, GraphInfo graphInfo) {
        for (VertexInfo vertexInfo : graphInfo.getVertexInfos()) {
            String vertexFilePath = saveDirectory + "/" + vertexInfo.getType() + ".vertex.yaml";
            Assert.assertTrue(
                    "Vertex file should exist: " + vertexFilePath,
                    new File(vertexFilePath).exists());
        }
    }

    /** Verifies that all edge YAML files exist. */
    public static void verifyEdgeFilesExist(String saveDirectory, GraphInfo graphInfo) {
        for (EdgeInfo edgeInfo : graphInfo.getEdgeInfos()) {
            String edgeFilePath = saveDirectory + "/" + edgeInfo.getConcat() + ".edge.yaml";
            Assert.assertTrue(
                    "Edge file should exist: " + edgeFilePath, new File(edgeFilePath).exists());
        }
    }

    /** Verifies basic GraphInfo properties. */
    public static void verifyGraphInfoBasics(
            GraphInfo graphInfo,
            String expectedName,
            int expectedVertexCount,
            int expectedEdgeCount) {
        Assert.assertNotNull("GraphInfo should not be null", graphInfo);
        Assert.assertEquals("Graph name mismatch", expectedName, graphInfo.getName());
        Assert.assertNotNull("Vertex infos should not be null", graphInfo.getVertexInfos());
        Assert.assertEquals(
                "Vertex count mismatch", expectedVertexCount, graphInfo.getVertexInfos().size());
        Assert.assertNotNull("Edge infos should not be null", graphInfo.getEdgeInfos());
        Assert.assertEquals(
                "Edge count mismatch", expectedEdgeCount, graphInfo.getEdgeInfos().size());
    }

    /** Verifies VertexInfo properties. */
    public static void verifyVertexInfo(
            VertexInfo vertexInfo,
            String expectedType,
            int expectedChunkSize,
            String expectedPrefix) {
        Assert.assertNotNull("VertexInfo should not be null", vertexInfo);
        Assert.assertEquals("Vertex type mismatch", expectedType, vertexInfo.getType());
        Assert.assertEquals(
                "Vertex chunk size mismatch", expectedChunkSize, vertexInfo.getChunkSize());
        Assert.assertEquals("Vertex prefix mismatch", expectedPrefix, vertexInfo.getPrefix());
    }

    /** Verifies EdgeInfo properties. */
    public static void verifyEdgeInfo(
            EdgeInfo edgeInfo,
            String expectedSrcType,
            String expectedEdgeType,
            String expectedDstType,
            int expectedChunkSize) {
        Assert.assertNotNull("EdgeInfo should not be null", edgeInfo);
        Assert.assertEquals("Source type mismatch", expectedSrcType, edgeInfo.getSrcType());
        Assert.assertEquals("Edge type mismatch", expectedEdgeType, edgeInfo.getEdgeType());
        Assert.assertEquals("Destination type mismatch", expectedDstType, edgeInfo.getDstType());
        Assert.assertEquals("Edge chunk size mismatch", expectedChunkSize, edgeInfo.getChunkSize());
    }

    /** Verifies Property attributes. */
    public static void verifyProperty(
            Property property,
            String expectedName,
            boolean expectedPrimary,
            boolean expectedNullable) {
        Assert.assertNotNull("Property should not be null", property);
        Assert.assertEquals("Property name mismatch", expectedName, property.getName());
        Assert.assertEquals(
                "Property primary flag mismatch", expectedPrimary, property.isPrimary());
        Assert.assertEquals(
                "Property nullable flag mismatch", expectedNullable, property.isNullable());
    }

    /** Verifies that a directory does not exist or is empty. */
    public static void verifyDirectoryClean(String directoryPath) {
        File dir = new File(directoryPath);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            Assert.assertTrue(
                    "Directory should be empty: " + directoryPath,
                    files == null || files.length == 0);
        }
    }

    /** Verifies that a directory exists and contains files. */
    public static void verifyDirectoryHasFiles(String directoryPath) {
        File dir = new File(directoryPath);
        Assert.assertTrue("Directory should exist: " + directoryPath, dir.exists());
        Assert.assertTrue("Directory should be a directory: " + directoryPath, dir.isDirectory());
        File[] files = dir.listFiles();
        Assert.assertNotNull("Directory should be readable: " + directoryPath, files);
        Assert.assertTrue("Directory should contain files: " + directoryPath, files.length > 0);
    }

    /**
     * Compares two GraphInfo objects for equality, including their VertexInfo and EdgeInfo objects.
     *
     * @param expected the expected GraphInfo
     * @param actual the actual GraphInfo
     * @return true if both GraphInfo objects are equal, false otherwise
     */
    public static boolean equalsGraphInfo(GraphInfo expected, GraphInfo actual) {
        if (expected == actual) {
            return true;
        }
        if (expected == null || actual == null) {
            return false;
        }

        Assert.assertEquals("GraphInfo name mismatch", expected.getName(), actual.getName());
        Assert.assertEquals(
                "GraphInfo baseUri mismatch", expected.getBaseUri(), actual.getBaseUri());
        Assert.assertEquals(
                "GraphInfo version mismatch",
                expected.getVersion().toString(),
                actual.getVersion().toString());
        Assert.assertTrue(
                "VertexInfo list mismatch",
                equalsVertexInfoList(expected.getVertexInfos(), actual.getVertexInfos()));
        Assert.assertTrue(
                "EdgeInfo list mismatch",
                equalsEdgeInfoList(expected.getEdgeInfos(), actual.getEdgeInfos()));
        return true;
    }

    /**
     * Compares two lists of VertexInfo objects for equality.
     *
     * @param expected the expected list of VertexInfo objects
     * @param actual the actual list of VertexInfo objects
     * @return true if both lists are equal, false otherwise
     */
    public static boolean equalsVertexInfoList(List<VertexInfo> expected, List<VertexInfo> actual) {
        if (expected == actual) {
            return true;
        }
        if (expected == null || actual == null || expected.size() != actual.size()) {
            return false;
        }

        for (int i = 0; i < expected.size(); i++) {
            Assert.assertTrue(
                    "VertexInfo at index " + i + " mismatch",
                    equalsVertexInfo(expected.get(i), actual.get(i)));
        }
        return true;
    }

    /**
     * Compares two lists of EdgeInfo objects for equality.
     *
     * @param expected the expected list of EdgeInfo objects
     * @param actual the actual list of EdgeInfo objects
     * @return true if both lists are equal, false otherwise
     */
    public static boolean equalsEdgeInfoList(List<EdgeInfo> expected, List<EdgeInfo> actual) {
        if (expected == actual) {
            return true;
        }
        if (expected == null || actual == null || expected.size() != actual.size()) {
            return false;
        }

        for (int i = 0; i < expected.size(); i++) {
            Assert.assertTrue(
                    "EdgeInfo at index " + i + " mismatch",
                    equalsEdgeInfo(expected.get(i), actual.get(i)));
        }
        return true;
    }

    /**
     * Compares two VertexInfo objects for equality.
     *
     * @param expected the expected VertexInfo
     * @param actual the actual VertexInfo
     * @return true if both VertexInfo objects are equal, false otherwise
     */
    public static boolean equalsVertexInfo(VertexInfo expected, VertexInfo actual) {
        if (expected == actual) {
            return true;
        }
        if (expected == null || actual == null) {
            return false;
        }

        Assert.assertEquals("VertexInfo labels mismatch", expected.getLabels(), actual.getLabels());

        Assert.assertEquals("VertexInfo type mismatch", expected.getType(), actual.getType());
        Assert.assertEquals(
                "VertexInfo chunk size mismatch", expected.getChunkSize(), actual.getChunkSize());
        Assert.assertEquals("VertexInfo prefix mismatch", expected.getPrefix(), actual.getPrefix());
        Assert.assertEquals(
                "VertexInfo version mismatch",
                expected.getVersion().toString(),
                actual.getVersion().toString());
        Assert.assertTrue(
                "VertexInfo property groups mismatch",
                equalsPropertyGroupList(expected.getPropertyGroups(), actual.getPropertyGroups()));
        return true;
    }

    /**
     * Compares two EdgeInfo objects for equality.
     *
     * @param expected the expected EdgeInfo
     * @param actual the actual EdgeInfo
     * @return true if both EdgeInfo objects are equal, false otherwise
     */
    public static boolean equalsEdgeInfo(EdgeInfo expected, EdgeInfo actual) {
        if (expected == actual) {
            return true;
        }
        if (expected == null || actual == null) {
            return false;
        }

        Assert.assertEquals(
                "EdgeInfo source type mismatch", expected.getSrcType(), actual.getSrcType());
        Assert.assertEquals(
                "EdgeInfo edge type mismatch", expected.getEdgeType(), actual.getEdgeType());
        Assert.assertEquals(
                "EdgeInfo destination type mismatch", expected.getDstType(), actual.getDstType());
        Assert.assertEquals(
                "EdgeInfo chunk size mismatch", expected.getChunkSize(), actual.getChunkSize());
        Assert.assertEquals(
                "EdgeInfo source chunk size mismatch",
                expected.getSrcChunkSize(),
                actual.getSrcChunkSize());
        Assert.assertEquals(
                "EdgeInfo destination chunk size mismatch",
                expected.getDstChunkSize(),
                actual.getDstChunkSize());
        Assert.assertEquals(
                "EdgeInfo directed mismatch", expected.isDirected(), actual.isDirected());
        Assert.assertEquals("EdgeInfo prefix mismatch", expected.getPrefix(), actual.getPrefix());
        Assert.assertEquals(
                "EdgeInfo version mismatch",
                expected.getVersion().toString(),
                actual.getVersion().toString());
        Assert.assertTrue(
                "EdgeInfo adjacent lists mismatch",
                equalsAdjacentListMap(expected.getAdjacentLists(), actual.getAdjacentLists()));
        Assert.assertTrue(
                "EdgeInfo property groups mismatch",
                equalsPropertyGroupList(expected.getPropertyGroups(), actual.getPropertyGroups()));
        return true;
    }

    /**
     * Compares two PropertyGroups objects for equality.
     *
     * @param expected the expected list of PropertyGroup objects
     * @param actual the actual list of PropertyGroup objects
     * @return true if both lists are equal, false otherwise
     */
    private static boolean equalsPropertyGroupList(
            List<PropertyGroup> expected, List<PropertyGroup> actual) {
        if (expected == actual) {
            return true;
        }
        if (expected == null || actual == null || expected.size() != actual.size()) {
            return false;
        }

        for (int i = 0; i < expected.size(); i++) {
            Assert.assertTrue(
                    "PropertyGroup at index " + i + " mismatch",
                    equalsPropertyGroup(expected.get(i), actual.get(i)));
        }
        return true;
    }

    /**
     * Compares two PropertyGroup objects for equality.
     *
     * @param expected the expected PropertyGroup
     * @param actual the actual PropertyGroup
     * @return true if both PropertyGroup objects are equal, false otherwise
     */
    private static boolean equalsPropertyGroup(PropertyGroup expected, PropertyGroup actual) {
        if (expected == actual) {
            return true;
        }
        if (expected == null || actual == null) {
            return false;
        }

        Assert.assertEquals(
                "PropertyGroup prefix mismatch", expected.getPrefix(), actual.getPrefix());
        Assert.assertEquals(
                "PropertyGroup file type mismatch", expected.getFileType(), actual.getFileType());
        Assert.assertTrue(
                "PropertyGroup properties mismatch",
                equalsPropertyList(expected.getPropertyList(), actual.getPropertyList()));
        return true;
    }

    /**
     * Compares two lists of Property objects for equality.
     *
     * @param expected the expected list of Property objects
     * @param actual the actual list of Property objects
     * @return true if both lists are equal, false otherwise
     */
    private static boolean equalsPropertyList(List<Property> expected, List<Property> actual) {
        if (expected == actual) {
            return true;
        }
        if (expected == null || actual == null || expected.size() != actual.size()) {
            return false;
        }

        for (int i = 0; i < expected.size(); i++) {
            Assert.assertTrue(
                    "Property at index " + i + " mismatch",
                    equalsProperty(expected.get(i), actual.get(i)));
        }
        return true;
    }

    /**
     * Compares two Property objects for equality.
     *
     * @param expected the expected Property
     * @param actual the actual Property
     * @return true if both Property objects are equal, false otherwise
     */
    private static boolean equalsProperty(Property expected, Property actual) {
        if (expected == actual) {
            return true;
        }
        if (expected == null || actual == null) {
            return false;
        }

        Assert.assertEquals("Property name mismatch", expected.getName(), actual.getName());
        Assert.assertEquals(
                "Property primary flag mismatch", expected.isPrimary(), actual.isPrimary());
        Assert.assertEquals(
                "Property nullable flag mismatch", expected.isNullable(), actual.isNullable());
        Assert.assertEquals(
                "Property data type mismatch", expected.getDataType(), actual.getDataType());
        return true;
    }

    /**
     * Compares two maps of AdjacentList objects for equality.
     *
     * @param expected the expected map of AdjacentList objects
     * @param actual the actual map of AdjacentList objects
     * @return true if both maps are equal, false otherwise
     */
    private static boolean equalsAdjacentListMap(
            Map<AdjListType, AdjacentList> expected, Map<AdjListType, AdjacentList> actual) {
        if (expected == actual) {
            return true;
        }
        if (expected == null || actual == null || expected.size() != actual.size()) {
            return false;
        }

        for (Map.Entry<AdjListType, AdjacentList> entry : expected.entrySet()) {
            AdjacentList actualValue = actual.get(entry.getKey());
            Assert.assertTrue(
                    "AdjacentList for type " + entry.getKey() + " mismatch",
                    equalsAdjacentList(entry.getValue(), actualValue));
        }
        return true;
    }

    /**
     * Compares two AdjacentList objects for equality.
     *
     * @param expected the expected AdjacentList
     * @param actual the actual AdjacentList
     * @return true if both AdjacentList objects are equal, false otherwise
     */
    private static boolean equalsAdjacentList(AdjacentList expected, AdjacentList actual) {
        if (expected == actual) {
            return true;
        }
        if (expected == null || actual == null) {
            return false;
        }

        Assert.assertEquals("AdjacentList type mismatch", expected.getType(), actual.getType());
        Assert.assertEquals(
                "AdjacentList file type mismatch", expected.getFileType(), actual.getFileType());
        Assert.assertEquals(
                "AdjacentList prefix mismatch", expected.getPrefix(), actual.getPrefix());
        return true;
    }
}
