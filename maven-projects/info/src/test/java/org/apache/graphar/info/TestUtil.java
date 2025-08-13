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

import java.util.List;
import org.apache.graphar.info.type.AdjListType;
import org.apache.graphar.info.type.DataType;
import org.apache.graphar.info.type.FileType;
import org.junit.Assume;

public class TestUtil {
    private static String GAR_TEST_DATA = null;

    static final String SAVE_DIR = "/tmp/graphar/test/";

    private static String LDBC_SAMPLE_GRAPH_PATH = "/ldbc_sample/csv/ldbc_sample.graph.yml";

    public static String getTestData() {
        return GAR_TEST_DATA;
    }

    public static String getLdbcSampleGraphPath() {
        return getTestData() + "/" + LDBC_SAMPLE_GRAPH_PATH;
    }

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

        Property id = new Property("id", DataType.INT64, true, false);
        Property firstName = new Property("firstName", DataType.STRING, false, false);
        Property lastName = new Property("lastName", DataType.STRING, false, false);
        Property gender = new Property("gender", DataType.STRING, false, true);
        PropertyGroup pg1 = new PropertyGroup(List.of(id), FileType.CSV, "id/");
        PropertyGroup pg2 =
                new PropertyGroup(
                        List.of(firstName, lastName, gender), FileType.CSV, "firstName_lastName");
        VertexInfo person =
                new VertexInfo("person", 100, List.of(pg1, pg2), "vertex/person/", "gar/v1");

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
        AdjacentList orderedBySource =
                new AdjacentList(AdjListType.ordered_by_source, FileType.CSV, "ordered_by_source/");
        AdjacentList orderedByDest =
                new AdjacentList(AdjListType.ordered_by_dest, FileType.CSV, "ordered_by_dest/");
        Property creationDate = new Property("creationDate", DataType.STRING, false, false);
        PropertyGroup pg3 = new PropertyGroup(List.of(creationDate), FileType.CSV, "creationDate/");
        EdgeInfo knows =
                new EdgeInfo(
                        "person",
                        "knows",
                        "person",
                        1024,
                        100,
                        100,
                        false,
                        "edge/person_knows_person/",
                        "gar/v1",
                        List.of(orderedBySource, orderedByDest),
                        List.of(pg3));

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
            GAR_TEST_DATA = System.getenv("GAR_TEST_DATA");
        }
        Assume.assumeTrue("GAR_TEST_DATA is not set", GAR_TEST_DATA != null);
    }
}
