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

import org.apache.graphar.info.saver.GraphSaver;
import org.apache.graphar.info.saver.LocalYamlGraphSaver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class GraphSaverTest extends BaseFileSystemTest {

    private String testSaveDirectory;
    private GraphSaver graphSaver;
    private GraphInfo testGraphInfo;

    @Before
    public void setUp() {
        verifyTestPrerequisites();
        testSaveDirectory = createCleanTestDirectory("ldbc_sample");
        graphSaver = new LocalYamlGraphSaver();
        testGraphInfo = TestDataFactory.createSampleGraphInfo();
    }

    @After
    public void tearDown() {
        // Clean up test directory after each test
        cleanupDirectory(testSaveDirectory);
    }

    @Test
    public void testSave() {
        try {
            graphSaver.save(testSaveDirectory, testGraphInfo);
            TestVerificationUtils.verifyGraphInfoFilesSaved(testSaveDirectory, testGraphInfo);
        } catch (Exception e) {
            throw new RuntimeException("Failed to save graph info", e);
        }
    }

    @Test
    public void testSaveMinimalGraph() {
        GraphInfo minimalGraph = TestDataFactory.createMinimalGraphInfo();
        try {
            graphSaver.save(testSaveDirectory, minimalGraph);
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
            graphSaver.save(nestedDir, testGraphInfo);
            TestVerificationUtils.verifyDirectoryHasFiles(nestedDir);
            TestVerificationUtils.verifyGraphInfoFilesSaved(nestedDir, testGraphInfo);
        } catch (Exception e) {
            throw new RuntimeException("Failed to save to nested directory", e);
        }
    }
}
