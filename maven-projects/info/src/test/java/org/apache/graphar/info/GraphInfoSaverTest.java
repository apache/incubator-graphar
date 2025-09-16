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

import java.net.URI;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.List;
import org.apache.graphar.info.loader.impl.LocalFileSystemStringGraphInfoLoader;
import org.apache.graphar.info.saver.GraphInfoSaver;
import org.apache.graphar.info.saver.impl.LocalFileSystemYamlGraphSaver;
import org.apache.graphar.info.yaml.EdgeYaml;
import org.apache.graphar.info.yaml.GraphYaml;
import org.apache.graphar.info.yaml.VertexYaml;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class GraphInfoSaverTest extends BaseFileSystemTest {

    private String testSaveDirectory;
    private GraphInfoSaver graphInfoSaver;
    private GraphInfo testGraphInfo;

    @Before
    public void setUp() {
        verifyTestPrerequisites();
        testSaveDirectory = createCleanTestDirectory("ldbc_sample");
        graphInfoSaver = new LocalFileSystemYamlGraphSaver();
        testGraphInfo = TestDataFactory.createSampleGraphInfo();
    }

    @After
    public void tearDown() {
        // Test data will be preserved for debugging - cleanup happens before next test run
        System.out.println("Test data saved in: " + testSaveDirectory);
    }

    @Test
    public void testDump() {
        List<VertexInfo> vertexInfos = new ArrayList<>();
        for (VertexInfo vertexInfo : testGraphInfo.getVertexInfos()) {
            String vertexYamlString = vertexInfo.dump();
            Yaml vertexYamlLoader =
                    new Yaml(new Constructor(VertexYaml.class, new LoaderOptions()));
            VertexYaml vertexYaml = vertexYamlLoader.load(vertexYamlString);
            VertexInfo vertexInfoFromYaml = TestUtil.buildVertexInfoFromYaml(vertexYaml);
            vertexInfos.add(vertexInfoFromYaml);
        }
        List<EdgeInfo> edgeInfos = new ArrayList<>();
        for (EdgeInfo edgeInfo : testGraphInfo.getEdgeInfos()) {
            String edgeYamlString = edgeInfo.dump();
            Yaml edgeYamlLoader = new Yaml(new Constructor(EdgeYaml.class, new LoaderOptions()));
            EdgeYaml EdgeYaml = edgeYamlLoader.load(edgeYamlString);
            EdgeInfo EdgeInfoFromYaml = TestUtil.buildEdgeInfoFromYaml(EdgeYaml);
            edgeInfos.add(EdgeInfoFromYaml);
        }
        String testGraphInfoString = testGraphInfo.dump();
        Yaml GraphYamlLoader = new Yaml(new Constructor(GraphYaml.class, new LoaderOptions()));
        GraphYaml graphYaml = GraphYamlLoader.load(testGraphInfoString);
        GraphInfo graphInfoFromYaml =
                new GraphInfo(
                        graphYaml.getName(),
                        vertexInfos,
                        edgeInfos,
                        graphYaml.getPrefix(),
                        graphYaml.getVersion());
        Assert.assertTrue(TestVerificationUtils.equalsGraphInfo(testGraphInfo, graphInfoFromYaml));
    }

    @Test
    public void testSave() {
        try {
            URI graphInfoUri =
                    URI.create(
                            testSaveDirectory
                                    + FileSystems.getDefault().getSeparator()
                                    + testGraphInfo.getName()
                                    + ".graph.yaml");
            graphInfoSaver.save(graphInfoUri, testGraphInfo);
            TestVerificationUtils.verifyGraphInfoFilesSaved(testSaveDirectory, testGraphInfo);
            LocalFileSystemStringGraphInfoLoader graphInfoLoader =
                    new LocalFileSystemStringGraphInfoLoader();
            GraphInfo graphInfo = graphInfoLoader.loadGraphInfo(graphInfoUri);
            TestVerificationUtils.equalsGraphInfo(testGraphInfo, graphInfo);
        } catch (Exception e) {
            throw new RuntimeException("Failed to save graph info", e);
        }
    }

    @Test
    public void testSaveMinimalGraph() {
        GraphInfo minimalGraph = TestDataFactory.createMinimalGraphInfo();
        try {
            URI graphInfoUri =
                    URI.create(
                            testSaveDirectory
                                    + FileSystems.getDefault().getSeparator()
                                    + minimalGraph.getName()
                                    + ".graph.yaml");
            graphInfoSaver.save(graphInfoUri, minimalGraph);
            TestVerificationUtils.verifyGraphFileExists(testSaveDirectory, minimalGraph);
            TestVerificationUtils.verifyVertexFilesExist(testSaveDirectory, minimalGraph);
            LocalFileSystemStringGraphInfoLoader graphInfoLoader =
                    new LocalFileSystemStringGraphInfoLoader();
            GraphInfo graphInfo = graphInfoLoader.loadGraphInfo(graphInfoUri);
            TestVerificationUtils.equalsGraphInfo(minimalGraph, graphInfo);
            // No edge files expected for minimal graph
        } catch (Exception e) {
            throw new RuntimeException("Failed to save minimal graph info", e);
        }
    }

    @Test
    public void testSaveGraph2DiffPath() {
        for (VertexInfo vertexInfo : testGraphInfo.getVertexInfos()) {
            testGraphInfo.setStoreUri(
                    vertexInfo,
                    URI.create(testSaveDirectory + "/test_vertices/")
                            .resolve(testGraphInfo.getStoreUri(vertexInfo)));
        }

        for (EdgeInfo edgeInfo : testGraphInfo.getEdgeInfos()) {
            testGraphInfo.setStoreUri(
                    edgeInfo,
                    URI.create(testSaveDirectory + "/test_edges/")
                            .resolve(testGraphInfo.getStoreUri(edgeInfo)));
        }
        try {
            URI graphInfoUri =
                    URI.create(
                            testSaveDirectory
                                    + FileSystems.getDefault().getSeparator()
                                    + testGraphInfo.getName()
                                    + ".graph.yaml");
            graphInfoSaver.save(graphInfoUri, testGraphInfo);
            TestVerificationUtils.verifyGraphFileExists(testSaveDirectory, testGraphInfo);
            TestVerificationUtils.verifyVertexFilesExist(
                    testSaveDirectory + "/test_vertices", testGraphInfo);
            TestVerificationUtils.verifyEdgeFilesExist(
                    testSaveDirectory + "/test_edges", testGraphInfo);
            // No edge files expected for minimal graph
            LocalFileSystemStringGraphInfoLoader graphInfoLoader =
                    new LocalFileSystemStringGraphInfoLoader();
            GraphInfo graphInfo = graphInfoLoader.loadGraphInfo(graphInfoUri);
            TestVerificationUtils.equalsGraphInfo(testGraphInfo, graphInfo);
        } catch (Exception e) {
            throw new RuntimeException("Failed to save test graph info", e);
        }
    }

    @Test
    public void testSaveDirectoryCreation() {
        String nestedDir = testSaveDirectory + "/nested/deep/directory";
        try {
            URI graphInfoUri =
                    URI.create(
                            nestedDir
                                    + FileSystems.getDefault().getSeparator()
                                    + testGraphInfo.getName()
                                    + ".graph.yaml");
            graphInfoSaver.save(graphInfoUri, testGraphInfo);
            TestVerificationUtils.verifyDirectoryHasFiles(nestedDir);
            TestVerificationUtils.verifyGraphInfoFilesSaved(nestedDir, testGraphInfo);
        } catch (Exception e) {
            throw new RuntimeException("Failed to save to nested directory", e);
        }
    }
}
