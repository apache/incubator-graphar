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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Base class for tests that require file system operations. Provides common setup and cleanup
 * functionality.
 */
public abstract class BaseFileSystemTest {

    protected static final String TEST_OUTPUT_DIR = TestUtil.SAVE_DIR;

    @BeforeClass
    public static void setUpClass() {
        TestUtil.checkTestData();
        ensureTestOutputDirectoryExists();
    }

    @AfterClass
    public static void tearDownClass() {
        // Optionally clean up test directories after all tests
        // cleanupTestOutputDirectory();
    }

    /** Ensures the test output directory exists. */
    protected static void ensureTestOutputDirectoryExists() {
        try {
            Path testDir = Paths.get(TEST_OUTPUT_DIR);
            Files.createDirectories(testDir);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to create test output directory: " + TEST_OUTPUT_DIR, e);
        }
    }

    /**
     * Cleans up a specific test directory. Use this in @Before or @After methods for test-specific
     * cleanup.
     */
    protected void cleanupDirectory(String directoryPath) {
        try {
            Path dir = Paths.get(directoryPath);
            if (Files.exists(dir)) {
                Files.walk(dir)
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
        } catch (Exception e) {
            // Log warning but don't fail the test due to cleanup issues
            System.err.println(
                    "Warning: Failed to cleanup directory "
                            + directoryPath
                            + ": "
                            + e.getMessage());
        }
    }

    /** Creates a test-specific subdirectory and ensures it's clean. */
    protected String createCleanTestDirectory(String subdirectoryName) {
        String testDir = TEST_OUTPUT_DIR + "/" + subdirectoryName;
        cleanupDirectory(testDir);

        try {
            Files.createDirectories(Paths.get(testDir));
        } catch (Exception e) {
            throw new RuntimeException("Failed to create test directory: " + testDir, e);
        }

        return testDir;
    }

    /** Verifies that test prerequisites are met. */
    protected void verifyTestPrerequisites() {
        TestUtil.checkTestData();
    }

    /**
     * Cleans up the entire test output directory. Use with caution - this removes all test output
     * files.
     */
    private static void cleanupTestOutputDirectory() {
        try {
            Path testDir = Paths.get(TEST_OUTPUT_DIR);
            if (Files.exists(testDir)) {
                Files.walk(testDir)
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
        } catch (Exception e) {
            System.err.println(
                    "Warning: Failed to cleanup test output directory: " + e.getMessage());
        }
    }
}
