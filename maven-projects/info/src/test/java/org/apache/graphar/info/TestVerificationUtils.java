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
}
