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

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import org.apache.graphar.info.loader.GraphInfoLoader;
import org.apache.graphar.info.loader.impl.LocalFileSystemStringGraphInfoLoader;
import org.apache.graphar.info.saver.GraphInfoSaver;
import org.apache.graphar.info.saver.impl.LocalFileSystemYamlGraphSaver;
import org.apache.graphar.info.type.AdjListType;
import org.apache.graphar.info.type.Cardinality;
import org.apache.graphar.info.type.DataType;
import org.apache.graphar.info.type.FileType;
import org.apache.graphar.info.yaml.PropertyYaml;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MultiPropertyTest extends BaseFileSystemTest {

    private String testSaveDirectory;
    private GraphInfoLoader graphInfoLoader;
    private GraphInfoSaver graphInfoSaver;
    private Property singleProperty;
    private Property listProperty;
    private Property setProperty;

    @Before
    public void setUp() {
        testSaveDirectory = createCleanTestDirectory("ldbc_multi_property_sample/");
        graphInfoLoader = new LocalFileSystemStringGraphInfoLoader();
        graphInfoSaver = new LocalFileSystemYamlGraphSaver();
        singleProperty =
                TestDataFactory.createProperty("single_email", DataType.STRING, false, true);
        listProperty =
                TestDataFactory.createProperty(
                        "list_email", DataType.STRING, Cardinality.LIST, false, true);
        setProperty =
                TestDataFactory.createProperty(
                        "set_email", DataType.STRING, Cardinality.SET, false, true);
    }

    @After
    public void tearDown() {
        // Test data will be preserved for debugging - cleanup happens before next test run
        System.out.println("Test data saved in: " + testSaveDirectory);
    }

    @Test
    public void testPropertyWithCardinality() {
        // Test single cardinality property
        Assert.assertEquals("single_email", singleProperty.getName());
        Assert.assertEquals(DataType.STRING, singleProperty.getDataType());
        Assert.assertFalse(singleProperty.isPrimary());
        Assert.assertTrue(singleProperty.isNullable());
        Assert.assertEquals(Cardinality.SINGLE, singleProperty.getCardinality());

        // Test list cardinality property
        Assert.assertEquals("list_email", listProperty.getName());
        Assert.assertEquals(DataType.STRING, listProperty.getDataType());
        Assert.assertFalse(listProperty.isPrimary());
        Assert.assertTrue(listProperty.isNullable());
        Assert.assertEquals(Cardinality.LIST, listProperty.getCardinality());

        // Test set cardinality property
        Assert.assertEquals("set_email", setProperty.getName());
        Assert.assertEquals(DataType.STRING, setProperty.getDataType());
        Assert.assertFalse(setProperty.isPrimary());
        Assert.assertTrue(setProperty.isNullable());
        Assert.assertEquals(Cardinality.SET, setProperty.getCardinality());
    }

    @Test
    public void testPropertyGroupWithMultiProperties() {
        // Create property group with different cardinality properties
        PropertyGroup propertyGroup =
                TestDataFactory.createPropertyGroup(
                        Arrays.asList(singleProperty, listProperty, setProperty),
                        FileType.PARQUET,
                        "emails/");

        Assert.assertEquals(3, propertyGroup.size());
        Assert.assertEquals(FileType.PARQUET, propertyGroup.getFileType());
        Assert.assertEquals("emails/", propertyGroup.getPrefix());

        // Test iteration over properties
        int count = 0;
        for (Property property : propertyGroup) {
            count++;
            if ("single_email".equals(property.getName())) {
                Assert.assertEquals(Cardinality.SINGLE, property.getCardinality());
            } else if ("list_email".equals(property.getName())) {
                Assert.assertEquals(Cardinality.LIST, property.getCardinality());
            } else if ("set_email".equals(property.getName())) {
                Assert.assertEquals(Cardinality.SET, property.getCardinality());
            }
        }
        Assert.assertEquals(3, count);
    }

    @Test
    public void testPropertyYamlWithCardinality() {
        // Test converting Property to PropertyYaml and back
        PropertyYaml singleYaml = new PropertyYaml(singleProperty);
        PropertyYaml listYaml = new PropertyYaml(listProperty);
        PropertyYaml setYaml = new PropertyYaml(setProperty);

        // Check YAML representations
        Assert.assertEquals("single_email", singleYaml.getName());
        Assert.assertNull(singleYaml.getCardinality());

        Assert.assertEquals("list_email", listYaml.getName());
        Assert.assertEquals("list", listYaml.getCardinality());

        Assert.assertEquals("set_email", setYaml.getName());
        Assert.assertEquals("set", setYaml.getCardinality());

        // Convert back to Property objects
        Property singlePropFromYaml = new Property(singleYaml);
        Property listPropFromYaml = new Property(listYaml);
        Property setPropFromYaml = new Property(setYaml);

        // Verify properties are correctly restored
        Assert.assertEquals(singleProperty.getName(), singlePropFromYaml.getName());
        Assert.assertEquals(singleProperty.getDataType(), singlePropFromYaml.getDataType());
        Assert.assertEquals(singleProperty.getCardinality(), singlePropFromYaml.getCardinality());

        Assert.assertEquals(listProperty.getName(), listPropFromYaml.getName());
        Assert.assertEquals(listProperty.getDataType(), listPropFromYaml.getDataType());
        Assert.assertEquals(listProperty.getCardinality(), listPropFromYaml.getCardinality());

        Assert.assertEquals(setProperty.getName(), setPropFromYaml.getName());
        Assert.assertEquals(setProperty.getDataType(), setPropFromYaml.getDataType());
        Assert.assertEquals(setProperty.getCardinality(), setPropFromYaml.getCardinality());
    }

    @Test
    public void testDefaultCardinality() {
        // Test that properties created without specifying cardinality default to SINGLE
        Property defaultProperty =
                TestDataFactory.createProperty("default_prop", DataType.STRING, false, true);
        Assert.assertEquals(Cardinality.SINGLE, defaultProperty.getCardinality());

        // Test YAML conversion with default cardinality
        PropertyYaml defaultYaml = new PropertyYaml(defaultProperty);
        Assert.assertNull(defaultYaml.getCardinality());

        Property defaultPropFromYaml = new Property(defaultYaml);
        Assert.assertEquals(Cardinality.SINGLE, defaultPropFromYaml.getCardinality());
    }

    @Test
    public void testInvalidCardinalityHandling() {
        // Test handling of invalid cardinality in YAML
        PropertyYaml invalidYaml = new PropertyYaml();
        invalidYaml.setName("invalid_prop");
        invalidYaml.setData_type("string");
        invalidYaml.setCardinality("INVALID"); // This should throw an IllegalArgumentException
        IllegalArgumentException illegalArgumentException =
                Assert.assertThrows(
                        IllegalArgumentException.class, () -> new Property(invalidYaml));
        Assert.assertEquals("Unknown cardinality: INVALID", illegalArgumentException.getMessage());
    }

    @Test
    public void testEdgeInfoWithMultiProperty() throws IOException {
        Property weights =
                TestDataFactory.createProperty(
                        "weights", DataType.DOUBLE, Cardinality.LIST, false, true);
        PropertyGroup weights_pg =
                TestDataFactory.createPropertyGroup(
                        Arrays.asList(weights), FileType.CSV, "weights/");
        AdjacentList orderedBySource =
                TestDataFactory.createAdjacentList(
                        AdjListType.ordered_by_source, FileType.PARQUET, "ordered_by_source/");
        EdgeInfo knowsEdgeInfo =
                EdgeInfo.builder()
                        .srcType("person")
                        .edgeType("knows")
                        .dstType("person")
                        .chunkSize(1024)
                        .srcChunkSize(100)
                        .dstChunkSize(100)
                        .directed(false)
                        .prefix("edge/person_knows_person/")
                        .version("gar/v1")
                        .addAdjacentList(orderedBySource)
                        .addPropertyGroups(List.of(weights_pg))
                        .build();
        Assert.assertFalse(knowsEdgeInfo.isValidated());
    }

    @Test
    public void testListPropertyInCsv() {
        Property emails =
                TestDataFactory.createProperty(
                        "emails", DataType.STRING, Cardinality.LIST, false, true);
        PropertyGroup emailInCsv =
                TestDataFactory.createPropertyGroup(List.of(emails), FileType.CSV, "emails_csv/");
        VertexInfo personVertexInfo =
                new VertexInfo("person", 1000, List.of(emailInCsv), "vertex/person/", "gar/v1");
        Assert.assertFalse(personVertexInfo.isValidated());
    }

    @Test
    public void testSetPropertyInCsv() {
        Property emails =
                TestDataFactory.createProperty(
                        "emails", DataType.STRING, Cardinality.SET, false, true);
        PropertyGroup emailInCsv =
                TestDataFactory.createPropertyGroup(List.of(emails), FileType.CSV, "emails_csv/");
        VertexInfo personVertexInfo =
                new VertexInfo("person", 1000, List.of(emailInCsv), "vertex/person/", "gar/v1");
        Assert.assertFalse(personVertexInfo.isValidated());
    }

    @Test
    public void testGraphInfoWithMultiPropertyYamlLoadAndSave() throws IOException {
        // Create a GraphInfo with multi-property support
        Property id = TestDataFactory.createProperty("id", DataType.INT64, true, false);
        Property name = TestDataFactory.createProperty("name", DataType.STRING, false, true);
        Property emails =
                TestDataFactory.createProperty(
                        "emails", DataType.STRING, Cardinality.LIST, false, true);
        Property tags =
                TestDataFactory.createProperty(
                        "tags", DataType.STRING, Cardinality.SET, false, true);
        Property creationDate =
                TestDataFactory.createProperty("creationDate", DataType.STRING, false, false);
        PropertyGroup personIdGroup =
                TestDataFactory.createPropertyGroup(List.of(id), FileType.PARQUET, "id/");
        PropertyGroup personInfoGroup =
                TestDataFactory.createPropertyGroup(
                        Arrays.asList(name, emails, tags), FileType.PARQUET, "info/");

        PropertyGroup edgePropertyGroup =
                TestDataFactory.createPropertyGroup(
                        List.of(creationDate), FileType.PARQUET, "properties/");

        // Create adjacent lists
        AdjacentList orderedBySource =
                TestDataFactory.createAdjacentList(
                        AdjListType.ordered_by_source, FileType.PARQUET, "ordered_by_source/");
        AdjacentList unorderedByDest =
                TestDataFactory.createAdjacentList(
                        AdjListType.unordered_by_dest, FileType.PARQUET, "unordered_by_dest/");

        VertexInfo personVertexInfo =
                new VertexInfo(
                        "person",
                        1000,
                        Arrays.asList(personIdGroup, personInfoGroup),
                        "vertex/person/",
                        "gar/v1");

        EdgeInfo knowsEdgeInfo =
                EdgeInfo.builder()
                        .srcType("person")
                        .edgeType("knows")
                        .dstType("person")
                        .chunkSize(1024)
                        .srcChunkSize(100)
                        .dstChunkSize(100)
                        .directed(false)
                        .prefix("edge/person_knows_person/")
                        .version("gar/v1")
                        .addAdjacentList(orderedBySource)
                        .addAdjacentList(unorderedByDest)
                        .addPropertyGroups(Arrays.asList(edgePropertyGroup))
                        .build();

        GraphInfo graphInfo =
                new GraphInfo(
                        "test_graph",
                        Arrays.asList(personVertexInfo),
                        Arrays.asList(knowsEdgeInfo),
                        "/tmp/",
                        "gar/v1");

        // Check that the generated YAML files contain cardinality information
        String graphYamlFilePath = testSaveDirectory + "/test_graph.graph.yml";

        // Save GraphInfo to YAML files
        graphInfoSaver.save(URI.create(testSaveDirectory), graphInfo);

        // Load GraphInfo from YAML files
        GraphInfo loadedGraphInfo = graphInfoLoader.loadGraphInfo(URI.create(graphYamlFilePath));
        Assert.assertTrue(TestVerificationUtils.equalsGraphInfo(graphInfo, loadedGraphInfo));
    }

    @Test
    public void testLoadFromTestData() throws IOException {
        URI GRAPH_PATH_URI = TestUtil.getLdbcGraphURI();
        GraphInfo graphInfo = graphInfoLoader.loadGraphInfo(GRAPH_PATH_URI);
        VertexInfo personInfo = graphInfo.getVertexInfo("person");
        Assert.assertEquals(Cardinality.SINGLE, personInfo.getCardinality("firstName"));
        Assert.assertEquals(Cardinality.SINGLE, personInfo.getCardinality("lastName"));
        Assert.assertEquals(Cardinality.SINGLE, personInfo.getCardinality("gender"));
        Assert.assertEquals(Cardinality.SINGLE, personInfo.getCardinality("locationIP"));
        Assert.assertEquals(Cardinality.SINGLE, personInfo.getCardinality("browserUsed"));
        Assert.assertEquals(Cardinality.SINGLE, personInfo.getCardinality("id"));
        Assert.assertEquals(Cardinality.LIST, personInfo.getCardinality("emails"));
    }

    @Test
    public void testDumpCardinalities() throws IOException {
        Property id = new Property("id", DataType.INT64, true, false);
        PropertyGroup properties = new PropertyGroup(List.of(id), FileType.PARQUET, "id/");
        VertexInfo vertexInfo =
                new VertexInfo("person", 100, List.of(properties), "vertex/person/", "gar/v1");
        String dump = vertexInfo.dump();
        Assert.assertFalse(dump.contains("cardinality:"));

        id = new Property("id", DataType.INT64, Cardinality.SINGLE, true, false);
        properties = new PropertyGroup(List.of(id), FileType.PARQUET, "id/");
        vertexInfo = new VertexInfo("person", 100, List.of(properties), "vertex/person/", "gar/v1");
        dump = vertexInfo.dump();
        Assert.assertFalse(dump.contains("cardinality:"));

        id = new Property("id", DataType.INT64, Cardinality.LIST, true, false);
        properties = new PropertyGroup(List.of(id), FileType.PARQUET, "id/");
        vertexInfo = new VertexInfo("person", 100, List.of(properties), "vertex/person/", "gar/v1");
        dump = vertexInfo.dump();
        Assert.assertTrue(dump.contains("cardinality: list"));

        id = new Property("id", DataType.INT64, Cardinality.SET, true, false);
        properties = new PropertyGroup(List.of(id), FileType.PARQUET, "id/");
        vertexInfo = new VertexInfo("person", 100, List.of(properties), "vertex/person/", "gar/v1");
        dump = vertexInfo.dump();
        Assert.assertTrue(dump.contains("cardinality: set"));
    }
}
