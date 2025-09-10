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
import org.apache.graphar.info.type.AdjListType;
import org.apache.graphar.info.type.DataType;
import org.apache.graphar.info.type.FileType;
import org.junit.Assume;

public class TestUtil {
    private static String GAR_TEST_DATA = null;

    static final String SAVE_DIR = "/tmp/graphar/test/";

    private static final String LDBC_SAMPLE_GRAPH_PATH = "/ldbc_sample/csv/ldbc_sample.graph.yml";

    public static String getTestData() {
        return GAR_TEST_DATA;
    }

    public static String getLdbcSampleGraphPath() {
        return getTestData() + "/" + LDBC_SAMPLE_GRAPH_PATH;
    }

    public static URI getLdbcSampleGraphURI() {
        return URI.create(getLdbcSampleGraphPath());
    }

    public static final AdjacentList orderedBySource =
            new AdjacentList(AdjListType.ordered_by_source, FileType.CSV, "ordered_by_source/");
    public static final AdjacentList orderedByDest =
            new AdjacentList(AdjListType.ordered_by_dest, FileType.CSV, "ordered_by_dest/");
    public static final Property creationDate =
            new Property("creationDate", DataType.STRING, false, false);
    public static final PropertyGroup pg3 =
            new PropertyGroup(List.of(creationDate), FileType.CSV, "creationDate/");

    public static final Property id = new Property("id", DataType.INT64, true, false);
    public static final Property firstName =
            new Property("firstName", DataType.STRING, false, false);
    public static final Property lastName = new Property("lastName", DataType.STRING, false, false);
    public static final Property gender = new Property("gender", DataType.STRING, false, true);
    public static final PropertyGroup pg1 = new PropertyGroup(List.of(id), FileType.CSV, "id/");
    public static final PropertyGroup pg2 =
            new PropertyGroup(
                    List.of(firstName, lastName, gender), FileType.CSV, "firstName_lastName");
    public static final VertexInfo person =
            new VertexInfo("person", 100, List.of(pg1, pg2), "vertex/person/", "gar/v1");

    public static GraphInfo getLdbcSampleDataSet() {
        // create vertex info of yaml:
        // type: person
        // chunk_size: 100
        // prefix: vertex/person/
        // property_groups:
        //  - properties:
        //      - name: id
        //        data_type: int64
        //        is_primary: true
        //        is_nullable: false
        //    prefix: id/
        //    file_type: csv
        //  - properties:
        //      - name: firstName
        //        data_type: string
        //        is_primary: false
        //      - name: lastName
        //        data_type: string
        //        is_primary: false
        //      - name: gender
        //        data_type: string
        //        is_primary: false
        //        is_nullable: true
        //    prefix: firstName_lastName_gender/
        //    file_type: csv
        // version: gar/v1

        // create edge info of yaml:
        // src_type: person
        // edge_type: knows
        // dst_type: person
        // chunk_size: 1024
        // src_chunk_size: 100
        // dst_chunk_size: 100
        // directed: false
        // prefix: edge/person_knows_person/
        // adj_lists:
        //  - ordered: true
        //    aligned_by: src
        //    prefix: ordered_by_source/
        //    file_type: csv
        //  - ordered: true
        //    aligned_by: dst
        //    prefix: ordered_by_dest/
        //    file_type: csv
        // property_groups:
        //  - prefix: creationDate/
        //    file_type: csv
        //    properties:
        //      - name: creationDate
        //        data_type: string
        //        is_primary: false
        // version: gar/v1

        EdgeInfo knows =
                EdgeInfo.builder()
                        .edgeTriplet("person", "knows", "person")
                        .chunkSize(1024)
                        .srcChunkSize(100)
                        .dstChunkSize(100)
                        .directed(false)
                        .baseUri(URI.create("edge/person_knows_person/"))
                        .version("gar/v1")
                        .adjacentLists(List.of(orderedBySource, orderedByDest))
                        .addPropertyGroups(List.of(pg3))
                        .build();

        // create graph info of yaml:
        // name: ldbc_sample
        // vertices:
        //  - person.vertex.yml
        // edges:
        //  - person_knows_person.edge.yml
        // version: gar/v1
        return new GraphInfo("ldbc_sample", List.of(person), List.of(knows), "", "gar/v1");
    }

    public static void checkTestData() {
        if (GAR_TEST_DATA == null) {
            // 1. First try system property (convenient for Maven command line)
            GAR_TEST_DATA = System.getProperty("gar.test.data");

            // 2. Then try environment variable (backward compatibility)
            if (GAR_TEST_DATA == null) {
                GAR_TEST_DATA = System.getenv("GAR_TEST_DATA");
            }

            // 3. Auto-detect common locations
            if (GAR_TEST_DATA == null) {
                String[] possiblePaths = {
                    "../../testing", // from info module
                    "../testing", // from maven-projects
                    "testing", // from project root
                    "graphar-testing", // common CI naming
                    "../incubator-graphar-testing" // sibling directory
                };

                for (String path : possiblePaths) {
                    java.io.File testDir = new java.io.File(path).getAbsoluteFile();
                    if (testDir.exists() && testDir.isDirectory()) {
                        // Verify expected test data exists
                        java.io.File sampleGraph =
                                new java.io.File(testDir, "ldbc_sample/csv/ldbc_sample.graph.yml");
                        if (sampleGraph.exists()) {
                            GAR_TEST_DATA = testDir.getAbsolutePath();
                            System.out.println("Found test data at: " + GAR_TEST_DATA);
                            break;
                        }
                    }
                }
            }
        }

        // Verify test data directory
        boolean dataExists = false;
        if (GAR_TEST_DATA != null) {
            java.io.File testDir = new java.io.File(GAR_TEST_DATA);
            java.io.File sampleGraph =
                    new java.io.File(testDir, "ldbc_sample/csv/ldbc_sample.graph.yml");
            dataExists = testDir.exists() && sampleGraph.exists();
        }

        // Friendly error message
        Assume.assumeTrue(
                "GraphAr test data not found. To run tests, please:\n"
                        + "\n"
                        + "Option 1: Clone test data repository\n"
                        + "  git clone https://github.com/apache/incubator-graphar-testing.git testing\n"
                        + "\n"
                        + "Option 2: Set test data path\n"
                        + "  mvn test -Dgar.test.data=/path/to/testing\n"
                        + "  OR\n"
                        + "  export GAR_TEST_DATA=/path/to/testing\n"
                        + "\n"
                        + "Searched locations: testing/, ../testing/, ../../testing/\n"
                        + "Current GAR_TEST_DATA: "
                        + GAR_TEST_DATA,
                dataExists);
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
