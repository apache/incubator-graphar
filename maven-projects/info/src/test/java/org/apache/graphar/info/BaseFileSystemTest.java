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
        // Clean up from previous test runs before starting new tests
        cleanupTestOutputDirectory();
        ensureTestOutputDirectoryExists();
    }

    @AfterClass
    public static void tearDownClass() {
        // Keep test output for debugging - cleanup will happen before next test run
        System.out.println("Test output preserved in: " + TEST_OUTPUT_DIR);
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
     * Cleans up a specific test directory. Use this in @Before methods for test-specific
     * pre-cleanup, or when you need to reset a directory during test execution.
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

    /** 
     * Creates a test-specific subdirectory and ensures it's clean.
     * This method will clean existing content before creating the directory.
     */
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

    /**
     * Creates a test-specific subdirectory without cleaning existing content.
     * Use this when you want to preserve existing files in the directory.
     */
    protected String ensureTestDirectory(String subdirectoryName) {
        String testDir = TEST_OUTPUT_DIR + "/" + subdirectoryName;
        
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
        // Note: BaseFileSystemTest doesn't require external test data for basic operations
        // Individual tests that need external data should check TestUtil.getTestData() != null
    }

    /**
     * Cleans up the entire test output directory. 
     * Called automatically before test runs to ensure clean state.
     */
    private static void cleanupTestOutputDirectory() {
        try {
            Path testDir = Paths.get(TEST_OUTPUT_DIR);
            if (Files.exists(testDir)) {
                System.out.println("Cleaning up previous test output from: " + testDir);
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