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
import org.apache.graphar.info.AdjacentList; 
import org.apache.graphar.info.EdgeInfo;   
import org.apache.graphar.info.GraphInfo;  
import org.apache.graphar.info.Property;   
import org.apache.graphar.info.PropertyGroup; 
import org.apache.graphar.info.VertexInfo; 
import org.apache.graphar.types.AdjListType; 
import org.apache.graphar.types.DataType;   
import org.apache.graphar.types.FileType;   

import org.junit.jupiter.api.Assumptions; 

public class TestUtil {
    private static String GAR_TEST_DATA = null;

    static final String SAVE_DIR = "/tmp/graphar/test/";

    private static String LDBC_SAMPLE_GRAPH_PATH = "/ldbc_sample/csv/ldbc_sample.graph.yml";

    public static String getTestData() {
        return GAR_TEST_DATA;
    }

    public static String getLdbcSampleGraphPath() {
        if (getTestData() == null) return null; 
        return getTestData() + "/" + LDBC_SAMPLE_GRAPH_PATH;
    }

    public static GraphInfo getLdbcSampleDataSet() {
        Property id = new Property("id", DataType.INT64, true, false);
        Property firstName = new Property("firstName", DataType.STRING, false, false);
        Property lastName = new Property("lastName", DataType.STRING, false, false);
        Property gender = new Property("gender", DataType.STRING, false, true);
        Property creationDate = new Property("creationDate", DataType.STRING, false, false);

        PropertyGroup personPg1 = new PropertyGroup(List.of(id), FileType.CSV, "id/");
        PropertyGroup personPg2 =
                new PropertyGroup(
                        List.of(firstName, lastName, gender),
                        FileType.CSV,
                        "firstName_lastName_gender/");
        
        VertexInfo person =
                new VertexInfo(
                        "person",
                        100,
                        List.of(personPg1, personPg2),
                        "vertex/person/", 
                        "gar/v1");

        AdjacentList orderedBySource =
                new AdjacentList(AdjListType.ordered_by_source, FileType.CSV, "ordered_by_source/"); 
        AdjacentList orderedByDest =
                new AdjacentList(
                        AdjListType.ordered_by_dest, FileType.CSV, "ordered_by_dest/"); 

        PropertyGroup knowsPg1 = new PropertyGroup(List.of(creationDate), FileType.CSV, "creationDate/");
        
        EdgeInfo knows =
                new EdgeInfo(
                        "person", 
                        "knows",  
                        "person", 
                        100,      
                        1024,     
                        100,      
                        false,    
                        "edge/person_knows_person/", 
                        List.of(orderedBySource, orderedByDest),
                        List.of(knowsPg1),
                        "gar/v1");

        return new GraphInfo(
                "ldbc_sample", 
                List.of(person), 
                List.of(knows), 
                "ldbc_sample/csv/"); 
    }

    public static void checkTestData() {
        if (GAR_TEST_DATA == null) {
            GAR_TEST_DATA = System.getenv("GAR_TEST_DATA");
        }
        Assumptions.assumeTrue(GAR_TEST_DATA != null, "GAR_TEST_DATA is not set"); 
    }
}
