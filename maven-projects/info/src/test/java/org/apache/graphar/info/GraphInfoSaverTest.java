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

import org.apache.graphar.info.saver.GraphInfoSaver;
import org.apache.graphar.info.saver.impl.LocalYamlGraphInfoSaver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.nio.file.FileSystems;

public class GraphInfoSaverTest extends BaseFileSystemTest {

    private String testSaveDirectory;
    private GraphInfoSaver graphInfoSaver;
    private GraphInfo testGraphInfo;

    @Before
    public void setUp() {
        verifyTestPrerequisites();
        testSaveDirectory = createCleanTestDirectory("ldbc_sample");
        graphInfoSaver = new LocalYamlGraphInfoSaver();
        testGraphInfo = TestDataFactory.createSampleGraphInfo();
    }

    @After
    public void tearDown() {
        // Test data will be preserved for debugging - cleanup happens before next test run
        System.out.println("Test data saved in: " + testSaveDirectory);
    }

    @Test
    public void testSave() {
        try {
            URI graphInfoUri = URI.create(testSaveDirectory + FileSystems.getDefault().getSeparator() + testGraphInfo.getName() + ".graph.yaml");
            graphInfoSaver.save(graphInfoUri, testGraphInfo);
            TestVerificationUtils.verifyGraphInfoFilesSaved(testSaveDirectory, testGraphInfo);
        } catch (Exception e) {
            throw new RuntimeException("Failed to save graph info", e);
        }
    }

    @Test
    public void testSaveMinimalGraph() {
        GraphInfo minimalGraph = TestDataFactory.createMinimalGraphInfo();
        try {
            URI graphInfoUri = URI.create(testSaveDirectory + FileSystems.getDefault().getSeparator() + testGraphInfo.getName() + ".graph.yaml");
            graphInfoSaver.save(graphInfoUri, minimalGraph);
            TestVerificationUtils.verifyGraphFileExists(testSaveDirectory, minimalGraph);
            TestVerificationUtils.verifyVertexFilesExist(testSaveDirectory, minimalGraph);
            // No edge files expected for minimal graph
        } catch (Exception e) {
            throw new RuntimeException("Failed to save minimal graph info", e);
        }
    }

    @Test
    public void testSaveDirectoryCreation() {
        String nestedDir = testSaveDirectory + "/nested/deep/directory";
        try {
            URI graphInfoUri = URI.create(nestedDir + FileSystems.getDefault().getSeparator() + testGraphInfo.getName() + ".graph.yaml");
            graphInfoSaver.save(graphInfoUri, testGraphInfo);
            TestVerificationUtils.verifyDirectoryHasFiles(nestedDir);
            TestVerificationUtils.verifyGraphInfoFilesSaved(nestedDir, testGraphInfo);
        } catch (Exception e) {
            throw new RuntimeException("Failed to save to nested directory", e);
        }
    }
}
