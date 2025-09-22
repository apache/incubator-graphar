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

/**
 * Factory class for creating test data objects. Separates test data creation from test logic for
 * better maintainability.
 */
public class TestDataFactory {

    /**
     * Creates a sample GraphInfo object for testing purposes. This is the same data structure as
     * TestUtil.getLdbcSampleDataSet() but organized as a proper factory method.
     */
    public static GraphInfo createSampleGraphInfo() {
        VertexInfo personVertex = createPersonVertexInfo();
        EdgeInfo knowsEdge = createKnowsEdgeInfo();
        return new GraphInfo(
                "ldbc_sample",
                List.of(personVertex),
                List.of(knowsEdge),
                "file:///test_path",
                "gar/v1");
    }

    /** Creates a person vertex info for testing. */
    public static VertexInfo createPersonVertexInfo() {
        Property id = createIdProperty();
        Property firstName = createProperty("firstName", DataType.STRING, false, false);
        Property lastName = createProperty("lastName", DataType.STRING, false, false);
        Property gender = createProperty("gender", DataType.STRING, false, true);

        PropertyGroup idGroup = createPropertyGroup(List.of(id), FileType.PARQUET, "id/");
        PropertyGroup nameGroup =
                createPropertyGroup(
                        List.of(firstName, lastName, gender),
                        FileType.ORC,
                        "firstName_lastName_gender/");

        return new VertexInfo(
                "person", 100, List.of(idGroup, nameGroup), "vertex/person/", "gar/v1");
    }

    /** Creates a knows edge info for testing. */
    public static EdgeInfo createKnowsEdgeInfo() {
        AdjacentList orderedBySource =
                createAdjacentList(
                        AdjListType.ordered_by_source, FileType.CSV, "ordered_by_source/");
        AdjacentList orderedByDest =
                createAdjacentList(AdjListType.ordered_by_dest, FileType.CSV, "ordered_by_dest/");

        Property creationDate = createProperty("creationDate", DataType.STRING, false, false);
        PropertyGroup dateGroup =
                createPropertyGroup(List.of(creationDate), FileType.CSV, "creationDate/");

        return new EdgeInfo(
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
                List.of(dateGroup));
    }

    /** Creates a standard ID property for testing. */
    public static Property createIdProperty() {
        return createProperty("id", DataType.INT64, true, false);
    }

    /** Creates a property with given parameters. */
    public static Property createProperty(
            String name, DataType dataType, boolean isPrimary, boolean isNullable) {
        return new Property(name, dataType, isPrimary, isNullable);
    }

    /** Creates a property group with given parameters. */
    public static PropertyGroup createPropertyGroup(
            List<Property> properties, FileType fileType, String prefix) {
        return new PropertyGroup(properties, fileType, prefix);
    }

    /** Creates an adjacent list with given parameters. */
    public static AdjacentList createAdjacentList(
            AdjListType type, FileType fileType, String prefix) {
        return new AdjacentList(type, fileType, prefix);
    }

    /** Creates a minimal GraphInfo for basic testing. */
    public static GraphInfo createMinimalGraphInfo() {
        Property id = createIdProperty();
        PropertyGroup idGroup = createPropertyGroup(List.of(id), FileType.CSV, "id/");
        VertexInfo vertex = new VertexInfo("test", 100, List.of(idGroup), "vertex/test/", "gar/v1");
        return new GraphInfo("test_graph", List.of(vertex), List.of(), "/tmp", "gar/v1");
    }
}
