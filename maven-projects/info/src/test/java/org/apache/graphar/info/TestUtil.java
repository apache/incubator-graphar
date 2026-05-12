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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.graphar.info.loader.GraphInfoLoader;
import org.apache.graphar.info.type.AdjListType;
import org.apache.graphar.info.type.DataType;
import org.apache.graphar.info.type.FileType;
import org.apache.graphar.info.yaml.AdjacentListYaml;
import org.apache.graphar.info.yaml.EdgeYaml;
import org.apache.graphar.info.yaml.PropertyGroupYaml;
import org.apache.graphar.info.yaml.VertexYaml;

public class TestUtil {
    private static String GAR_TEST_DATA = null;

    static final String SAVE_DIR =
            System.getProperty("test.output.dir", "target/test-output") + "/";

    private static final String CSV_LDBC_SAMPLE_GRAPH_PATH =
            "/ldbc_sample/csv/ldbc_sample.graph.yml";
    private static final String PARQUET_LDBC_SAMPLE_GRAPH_PATH =
            "/ldbc_sample/parquet/ldbc_sample.graph.yml";
    private static final String LDBC_GRAPH_PATH = "/ldbc/parquet/ldbc.graph.yml";

    public static String getTestData() {
        return GAR_TEST_DATA;
    }

    public static boolean hasTestData() {
        checkTestData();
        return GAR_TEST_DATA != null && new java.io.File(GAR_TEST_DATA).exists();
    }

    public static String getCSVLdbcSampleGraphPath() {
        return getTestData() + "/" + CSV_LDBC_SAMPLE_GRAPH_PATH;
    }

    public static URI getCSVLdbcSampleGraphURI() {
        return URI.create(getCSVLdbcSampleGraphPath());
    }

    public static String getParquetLdbcSampleGraphPath() {
        return getTestData() + "/" + PARQUET_LDBC_SAMPLE_GRAPH_PATH;
    }

    public static URI getParquetLdbcSampleGraphURI() {
        return URI.create(getParquetLdbcSampleGraphPath());
    }

    public static String getLdbcGraphPath() {
        return getTestData() + "/" + LDBC_GRAPH_PATH;
    }

    public static URI getLdbcGraphURI() {
        return URI.create(getLdbcGraphPath());
    }

    public static VertexInfo buildVertexInfoFromYaml(VertexYaml vertexYaml) {
        return new VertexInfo(
                vertexYaml.getType(),
                vertexYaml.getChunk_size(),
                vertexYaml.getProperty_groups().stream()
                        .map(PropertyGroupYaml::toPropertyGroup)
                        .collect(Collectors.toList()),
                vertexYaml.getPrefix(),
                vertexYaml.getVersion());
    }

    public static EdgeInfo buildEdgeInfoFromYaml(EdgeYaml edgeYaml) {
        return new EdgeInfo(
                edgeYaml.getSrc_type(),
                edgeYaml.getEdge_type(),
                edgeYaml.getDst_type(),
                edgeYaml.getChunk_size(),
                edgeYaml.getSrc_chunk_size(),
                edgeYaml.getDst_chunk_size(),
                edgeYaml.isDirected(),
                edgeYaml.getPrefix(),
                edgeYaml.getVersion(),
                edgeYaml.getAdj_lists().stream()
                        .map(AdjacentListYaml::toAdjacentList)
                        .collect(Collectors.toUnmodifiableList()),
                edgeYaml.getProperty_groups().stream()
                        .map(PropertyGroupYaml::toPropertyGroup)
                        .collect(Collectors.toList()));
    }

    public static final AdjacentList orderedBySource =
            new AdjacentList(AdjListType.ordered_by_source, FileType.CSV, "ordered_by_source/");
    public static final AdjacentList orderedByDest =
            new AdjacentList(AdjListType.ordered_by_dest, FileType.CSV, "ordered_by_dest/");
    public static final Property creationDate =
            new Property("creationDate", DataType.STRING, false, true);
    public static final PropertyGroup pg3 =
            new PropertyGroup(List.of(creationDate), FileType.CSV, "creationDate/");

    public static final Property id = new Property("id", DataType.INT64, true, false);
    public static final Property firstName =
            new Property("firstName", DataType.STRING, false, true);
    public static final Property lastName = new Property("lastName", DataType.STRING, false, true);
    public static final Property gender = new Property("gender", DataType.STRING, false, true);
    public static final PropertyGroup pg1 = new PropertyGroup(List.of(id), FileType.CSV, "id/");
    public static final PropertyGroup pg2 =
            new PropertyGroup(
                    List.of(firstName, lastName, gender),
                    FileType.CSV,
                    "firstName_lastName_gender/");
    public static final VertexInfo person =
            new VertexInfo("person", 100, List.of(pg1, pg2), "vertex/person/", "gar/v1");

    /**
     * Gets the real LDBC sample GraphInfo by loading it from test data files. This replaces the old
     * mock data approach with real file-based data loading.
     */
    public static GraphInfo getLdbcSampleDataSet() {
        checkTestData();
        try {
            GraphInfoLoader loader =
                    new org.apache.graphar.info.loader.impl.LocalFileSystemStreamGraphInfoLoader();
            return loader.loadGraphInfo(getCSVLdbcSampleGraphURI());
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to load real LDBC sample data: " + e.getMessage(), e);
        }
    }

    public static void checkTestData() {
        // Always try to find test data freshly to avoid stale cached values
        String testDataPath = null;

        // 1. First try environment variable GAR_TEST_DATA
        testDataPath = System.getenv("GAR_TEST_DATA");

        // 2. Try system property (for custom paths)
        if (testDataPath == null) {
            testDataPath = System.getProperty("gar.test.data");
        }

        // 3. Default to project root testing directory
        if (testDataPath == null) {
            String[] possiblePaths = {
                "../../testing", // from info module to project root
                "../testing", // from maven-projects to project root
                "testing" // from project root
            };

            for (String path : possiblePaths) {
                java.io.File testDir = new java.io.File(path).getAbsoluteFile();
                if (testDir.exists() && testDir.isDirectory()) {
                    // Verify expected test data exists
                    java.io.File sampleGraph =
                            new java.io.File(testDir, "ldbc_sample/csv/ldbc_sample.graph.yml");
                    if (sampleGraph.exists()) {
                        testDataPath = testDir.getAbsolutePath();
                        break;
                    }
                }
            }
        }

        // Verify test data directory
        boolean dataExists = false;
        if (testDataPath != null) {
            java.io.File testDir = new java.io.File(testDataPath);
            java.io.File sampleGraph =
                    new java.io.File(testDir, "ldbc_sample/csv/ldbc_sample.graph.yml");
            dataExists = testDir.exists() && sampleGraph.exists();
        }

        if (!dataExists) {
            throw new RuntimeException(
                    "GAR_TEST_DATA not found or invalid. "
                            + "Please set GAR_TEST_DATA environment variable to point to the testing directory "
                            + "or ensure the testing directory exists with ldbc_sample/csv/ldbc_sample.graph.yml");
        }

        // Only set the static variable after successful validation
        GAR_TEST_DATA = testDataPath;
    }

    public static String getBaseGraphInfoYaml() {
        return "type: person\n"
                + "chunk_size: 100\n"
                + "prefix: vertex/person/\n"
                + "property_groups:\n"
                + "  - properties:\n"
                + "      - name: id\n"
                + "        data_type: int64\n"
                + "        is_primary: true\n"
                + "        is_nullable: false\n"
                + "    prefix: id/\n"
                + "    file_type: csv\n"
                + "  - properties:\n"
                + "      - name: firstName\n"
                + "        data_type: string\n"
                + "        is_primary: false\n"
                + "      - name: lastName\n"
                + "        data_type: string\n"
                + "        is_primary: false\n"
                + "      - name: gender\n"
                + "        data_type: string\n"
                + "        is_primary: false\n"
                + "        is_nullable: true\n"
                + "    prefix: /tmp/vertex/person/firstName_lastName_gender/\n"
                + "    file_type: csv\n"
                + "version: gar/v1\n";
    }

    public static String getS3GraphInfoYaml() {
        return "type: person\n"
                + "chunk_size: 100\n"
                + "prefix: s3://graphar/vertex/person/\n"
                + "property_groups:\n"
                + "  - properties:\n"
                + "      - name: id\n"
                + "        data_type: int64\n"
                + "        is_primary: true\n"
                + "        is_nullable: false\n"
                + "    prefix: id/\n"
                + "    file_type: csv\n"
                + "  - properties:\n"
                + "      - name: firstName\n"
                + "        data_type: string\n"
                + "        is_primary: false\n"
                + "      - name: lastName\n"
                + "        data_type: string\n"
                + "        is_primary: false\n"
                + "      - name: gender\n"
                + "        data_type: string\n"
                + "        is_primary: false\n"
                + "        is_nullable: true\n"
                + "    prefix: s3://tmp/vertex/person/firstName_lastName_gender/\n"
                + "    file_type: csv\n"
                + "version: gar/v1\n";
    }

    public static String getHdfsGraphInfoYaml() {
        return "type: person\n"
                + "chunk_size: 100\n"
                + "prefix: hdfs://graphar/vertex/person/\n"
                + "property_groups:\n"
                + "  - properties:\n"
                + "      - name: id\n"
                + "        data_type: int64\n"
                + "        is_primary: true\n"
                + "        is_nullable: false\n"
                + "    prefix: id/\n"
                + "    file_type: csv\n"
                + "  - properties:\n"
                + "      - name: firstName\n"
                + "        data_type: string\n"
                + "        is_primary: false\n"
                + "      - name: lastName\n"
                + "        data_type: string\n"
                + "        is_primary: false\n"
                + "      - name: gender\n"
                + "        data_type: string\n"
                + "        is_primary: false\n"
                + "        is_nullable: true\n"
                + "    prefix: hdfs://tmp/vertex/person/firstName_lastName_gender/\n"
                + "    file_type: csv\n"
                + "version: gar/v1\n";
    }

    public static String getFileGraphInfoYaml() {
        return "type: person\n"
                + "chunk_size: 100\n"
                + "prefix: file:///graphar/vertex/person/\n"
                + "property_groups:\n"
                + "  - properties:\n"
                + "      - name: id\n"
                + "        data_type: int64\n"
                + "        is_primary: true\n"
                + "        is_nullable: false\n"
                + "    prefix: id/\n"
                + "    file_type: csv\n"
                + "  - properties:\n"
                + "      - name: firstName\n"
                + "        data_type: string\n"
                + "        is_primary: false\n"
                + "      - name: lastName\n"
                + "        data_type: string\n"
                + "        is_primary: false\n"
                + "      - name: gender\n"
                + "        data_type: string\n"
                + "        is_primary: false\n"
                + "        is_nullable: true\n"
                + "    prefix: file:///tmp/vertex/person/firstName_lastName_gender/\n"
                + "    file_type: csv\n"
                + "version: gar/v1\n";
    }
}
