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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.graphar.info.type.DataType;
import org.apache.graphar.info.type.FileType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PropertyGroupTest {

    private Property idProperty;
    private Property nameProperty;
    private Property testProperty;
    private PropertyGroup basicGroup;
    private PropertyGroup singlePropertyGroup;
    private PropertyGroup emptyGroup;

    @Before
    public void setUp() {
        idProperty = TestDataFactory.createIdProperty();
        nameProperty = TestDataFactory.createProperty("name", DataType.STRING, false, true);
        testProperty = TestDataFactory.createProperty("test", DataType.STRING, false, true);

        basicGroup =
                TestDataFactory.createPropertyGroup(
                        Arrays.asList(idProperty, nameProperty), FileType.CSV, "test/");
        singlePropertyGroup =
                TestDataFactory.createPropertyGroup(
                        Arrays.asList(testProperty), FileType.PARQUET, "single/");
        emptyGroup = TestDataFactory.createPropertyGroup(new ArrayList<>(), FileType.CSV, "empty/");
    }

    @Test
    public void testPropertyGroupBasicConstruction() {
        Assert.assertEquals(2, basicGroup.size());
        Assert.assertEquals(FileType.CSV, basicGroup.getFileType());
        Assert.assertEquals("test/", basicGroup.getPrefix());
        Assert.assertEquals(Arrays.asList(idProperty, nameProperty), basicGroup.getPropertyList());
    }

    @Test
    public void testPropertyGroupWithAllFileTypes() {
        List<Property> properties = Arrays.asList(testProperty);

        PropertyGroup csvGroup =
                TestDataFactory.createPropertyGroup(properties, FileType.CSV, "csv/");
        Assert.assertEquals(FileType.CSV, csvGroup.getFileType());

        PropertyGroup parquetGroup =
                TestDataFactory.createPropertyGroup(properties, FileType.PARQUET, "parquet/");
        Assert.assertEquals(FileType.PARQUET, parquetGroup.getFileType());

        PropertyGroup orcGroup =
                TestDataFactory.createPropertyGroup(properties, FileType.ORC, "orc/");
        Assert.assertEquals(FileType.ORC, orcGroup.getFileType());
    }

    @Test
    public void testPropertyGroupIteration() {
        Property prop1 = TestDataFactory.createProperty("prop1", DataType.INT32, false, false);
        Property prop2 = TestDataFactory.createProperty("prop2", DataType.STRING, false, true);
        Property prop3 = TestDataFactory.createProperty("prop3", DataType.DOUBLE, false, true);
        List<Property> properties = Arrays.asList(prop1, prop2, prop3);

        PropertyGroup pg =
                TestDataFactory.createPropertyGroup(properties, FileType.PARQUET, "test/");

        int count = 0;
        for (Property prop : pg) {
            Assert.assertTrue(properties.contains(prop));
            count++;
        }
        Assert.assertEquals(3, count);
    }

    @Test
    public void testPropertyGroupToString() {
        Property id = TestDataFactory.createProperty("id", DataType.INT64, true, false);
        Property firstName =
                TestDataFactory.createProperty("firstName", DataType.STRING, false, true);
        Property lastName =
                TestDataFactory.createProperty("lastName", DataType.STRING, false, true);
        List<Property> properties = Arrays.asList(id, firstName, lastName);

        PropertyGroup pg = TestDataFactory.createPropertyGroup(properties, FileType.CSV, "names/");

        String result = pg.toString();
        Assert.assertTrue(result.contains("id"));
        Assert.assertTrue(result.contains("firstName"));
        Assert.assertTrue(result.contains("lastName"));
        // Should be joined by separator
        Assert.assertTrue(result.contains("_"));
    }

    @Test
    public void testPropertyGroupWithSingleProperty() {
        Assert.assertEquals(1, singlePropertyGroup.size());
        Assert.assertEquals("test", singlePropertyGroup.toString());
        Assert.assertEquals(testProperty, singlePropertyGroup.getPropertyList().get(0));
    }

    @Test
    public void testPropertyGroupWithEmptyList() {
        Assert.assertEquals(0, emptyGroup.size());
        Assert.assertTrue(emptyGroup.getPropertyList().isEmpty());
        Assert.assertTrue(emptyGroup.getPropertyMap().isEmpty());
        Assert.assertEquals("", emptyGroup.toString());
    }

    @Test
    public void testPropertyGroupWithNullPrefix() {
        PropertyGroup pg =
                TestDataFactory.createPropertyGroup(
                        Arrays.asList(testProperty), FileType.CSV, null);

        Assert.assertEquals(1, pg.size());
        Assert.assertNull(pg.getPrefix());
    }

    @Test
    public void testPropertyGroupWithEmptyPrefix() {
        PropertyGroup pg =
                TestDataFactory.createPropertyGroup(Arrays.asList(testProperty), FileType.CSV, "");

        Assert.assertEquals(1, pg.size());
        Assert.assertEquals("", pg.getPrefix());
    }

    @Test
    public void testPropertyGroupAddProperty() {
        Property existingProp =
                TestDataFactory.createProperty("existing", DataType.INT32, false, false);
        PropertyGroup pg =
                TestDataFactory.createPropertyGroup(
                        Arrays.asList(existingProp), FileType.CSV, "test/");

        // Add a new property
        Property newProp = TestDataFactory.createProperty("new", DataType.STRING, false, true);
        Optional<PropertyGroup> result = pg.addPropertyAsNew(newProp);

        Assert.assertTrue(result.isPresent());
        PropertyGroup newPg = result.get();
        Assert.assertEquals(2, newPg.size());
        Assert.assertTrue(newPg.getPropertyMap().containsKey("existing"));
        Assert.assertTrue(newPg.getPropertyMap().containsKey("new"));
    }

    @Test
    public void testPropertyGroupAddDuplicateProperty() {
        Property prop1 = TestDataFactory.createProperty("test", DataType.INT32, false, false);
        PropertyGroup pg =
                TestDataFactory.createPropertyGroup(Arrays.asList(prop1), FileType.CSV, "test/");

        // Try to add property with same name
        Property prop2 = TestDataFactory.createProperty("test", DataType.STRING, false, true);
        Optional<PropertyGroup> result = pg.addPropertyAsNew(prop2);

        Assert.assertFalse(result.isPresent());
    }

    @Test
    public void testPropertyGroupAddNullProperty() {
        Property existingProp =
                TestDataFactory.createProperty("existing", DataType.INT32, false, false);
        PropertyGroup pg =
                TestDataFactory.createPropertyGroup(
                        Arrays.asList(existingProp), FileType.CSV, "test/");

        Optional<PropertyGroup> result = pg.addPropertyAsNew(null);

        Assert.assertFalse(result.isPresent());
    }

    @Test
    public void testPropertyGroupPropertyMap() {
        Property id = TestDataFactory.createProperty("id", DataType.INT64, true, false);
        Property name = TestDataFactory.createProperty("name", DataType.STRING, false, true);
        Property age = TestDataFactory.createProperty("age", DataType.INT32, false, true);
        List<Property> properties = Arrays.asList(id, name, age);

        PropertyGroup pg =
                TestDataFactory.createPropertyGroup(properties, FileType.PARQUET, "person/");

        Assert.assertEquals(3, pg.getPropertyMap().size());
        Assert.assertEquals(id, pg.getPropertyMap().get("id"));
        Assert.assertEquals(name, pg.getPropertyMap().get("name"));
        Assert.assertEquals(age, pg.getPropertyMap().get("age"));
        Assert.assertNull(pg.getPropertyMap().get("nonexistent"));
    }

    @Test
    public void testPropertyGroupImmutability() {
        Property originalProp =
                TestDataFactory.createProperty("test", DataType.STRING, false, true);
        List<Property> originalProperties = new ArrayList<>(Arrays.asList(originalProp));

        PropertyGroup pg =
                TestDataFactory.createPropertyGroup(originalProperties, FileType.CSV, "test/");

        // Modify original list - should not affect PropertyGroup
        originalProperties.add(TestDataFactory.createProperty("new", DataType.INT32, false, false));

        Assert.assertEquals(1, pg.size()); // Should still be 1
        Assert.assertEquals(1, pg.getPropertyList().size());

        // Try to modify returned lists - should throw exception or be unmodifiable
        try {
            pg.getPropertyList()
                    .add(TestDataFactory.createProperty("another", DataType.BOOL, false, false));
            Assert.fail("Should not be able to modify property list");
        } catch (UnsupportedOperationException expected) {
            // This is expected behavior
        }

        try {
            pg.getPropertyMap()
                    .put(
                            "test2",
                            TestDataFactory.createProperty("test2", DataType.FLOAT, false, false));
            Assert.fail("Should not be able to modify property map");
        } catch (UnsupportedOperationException expected) {
            // This is expected behavior
        }
    }

    @Test
    public void testPropertyGroupWithComplexDataTypes() {
        Property listProp = TestDataFactory.createProperty("items", DataType.LIST, false, true);
        Property boolProp = TestDataFactory.createProperty("flag", DataType.BOOL, false, false);
        Property doubleProp = TestDataFactory.createProperty("score", DataType.DOUBLE, false, true);
        List<Property> properties = Arrays.asList(listProp, boolProp, doubleProp);

        PropertyGroup pg =
                TestDataFactory.createPropertyGroup(properties, FileType.PARQUET, "complex/");

        Assert.assertEquals(3, pg.size());
        Assert.assertEquals(DataType.LIST, pg.getPropertyMap().get("items").getDataType());
        Assert.assertEquals(DataType.BOOL, pg.getPropertyMap().get("flag").getDataType());
        Assert.assertEquals(DataType.DOUBLE, pg.getPropertyMap().get("score").getDataType());
    }
}
