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
import org.junit.Test;

public class PropertyGroupTest {

    @Test
    public void testPropertyGroupBasicConstruction() {
        Property id = new Property("id", DataType.INT32, true, false);
        Property name = new Property("name", DataType.STRING, false, true);
        List<Property> properties = Arrays.asList(id, name);

        PropertyGroup pg = new PropertyGroup(properties, FileType.CSV, "test/");

        Assert.assertEquals(2, pg.size());
        Assert.assertEquals(FileType.CSV, pg.getFileType());
        Assert.assertEquals("test/", pg.getPrefix());
        Assert.assertEquals(properties, pg.getPropertyList());
    }

    @Test
    public void testPropertyGroupWithAllFileTypes() {
        Property prop = new Property("test", DataType.STRING, false, true);
        List<Property> properties = Arrays.asList(prop);

        PropertyGroup csvGroup = new PropertyGroup(properties, FileType.CSV, "csv/");
        Assert.assertEquals(FileType.CSV, csvGroup.getFileType());

        PropertyGroup parquetGroup = new PropertyGroup(properties, FileType.PARQUET, "parquet/");
        Assert.assertEquals(FileType.PARQUET, parquetGroup.getFileType());

        PropertyGroup orcGroup = new PropertyGroup(properties, FileType.ORC, "orc/");
        Assert.assertEquals(FileType.ORC, orcGroup.getFileType());
    }

    @Test
    public void testPropertyGroupIteration() {
        Property prop1 = new Property("prop1", DataType.INT32, false, false);
        Property prop2 = new Property("prop2", DataType.STRING, false, true);
        Property prop3 = new Property("prop3", DataType.DOUBLE, false, true);
        List<Property> properties = Arrays.asList(prop1, prop2, prop3);

        PropertyGroup pg = new PropertyGroup(properties, FileType.PARQUET, "test/");

        int count = 0;
        for (Property prop : pg) {
            Assert.assertTrue(properties.contains(prop));
            count++;
        }
        Assert.assertEquals(3, count);
    }

    @Test
    public void testPropertyGroupToString() {
        Property id = new Property("id", DataType.INT64, true, false);
        Property firstName = new Property("firstName", DataType.STRING, false, true);
        Property lastName = new Property("lastName", DataType.STRING, false, true);
        List<Property> properties = Arrays.asList(id, firstName, lastName);

        PropertyGroup pg = new PropertyGroup(properties, FileType.CSV, "names/");

        String result = pg.toString();
        Assert.assertTrue(result.contains("id"));
        Assert.assertTrue(result.contains("firstName"));
        Assert.assertTrue(result.contains("lastName"));
        // Should be joined by separator
        Assert.assertTrue(result.contains("_"));
    }

    @Test
    public void testPropertyGroupWithSingleProperty() {
        Property singleProp = new Property("single", DataType.BOOL, false, false);
        List<Property> properties = Arrays.asList(singleProp);

        PropertyGroup pg = new PropertyGroup(properties, FileType.PARQUET, "single/");

        Assert.assertEquals(1, pg.size());
        Assert.assertEquals("single", pg.toString());
        Assert.assertEquals(singleProp, pg.getPropertyList().get(0));
    }

    @Test
    public void testPropertyGroupWithEmptyList() {
        List<Property> emptyProperties = new ArrayList<>();

        PropertyGroup pg = new PropertyGroup(emptyProperties, FileType.CSV, "empty/");

        Assert.assertEquals(0, pg.size());
        Assert.assertTrue(pg.getPropertyList().isEmpty());
        Assert.assertTrue(pg.getPropertyMap().isEmpty());
        Assert.assertEquals("", pg.toString());
    }

    @Test
    public void testPropertyGroupWithNullPrefix() {
        Property prop = new Property("test", DataType.STRING, false, true);
        List<Property> properties = Arrays.asList(prop);

        PropertyGroup pg = new PropertyGroup(properties, FileType.CSV, null);

        Assert.assertEquals(1, pg.size());
        Assert.assertNull(pg.getPrefix());
    }

    @Test
    public void testPropertyGroupWithEmptyPrefix() {
        Property prop = new Property("test", DataType.STRING, false, true);
        List<Property> properties = Arrays.asList(prop);

        PropertyGroup pg = new PropertyGroup(properties, FileType.CSV, "");

        Assert.assertEquals(1, pg.size());
        Assert.assertEquals("", pg.getPrefix());
    }

    @Test
    public void testPropertyGroupAddProperty() {
        Property existingProp = new Property("existing", DataType.INT32, false, false);
        List<Property> properties = Arrays.asList(existingProp);
        PropertyGroup pg = new PropertyGroup(properties, FileType.CSV, "test/");

        // Add a new property
        Property newProp = new Property("new", DataType.STRING, false, true);
        Optional<PropertyGroup> result = pg.addPropertyAsNew(newProp);

        Assert.assertTrue(result.isPresent());
        PropertyGroup newPg = result.get();
        Assert.assertEquals(2, newPg.size());
        Assert.assertTrue(newPg.getPropertyMap().containsKey("existing"));
        Assert.assertTrue(newPg.getPropertyMap().containsKey("new"));
    }

    @Test
    public void testPropertyGroupAddDuplicateProperty() {
        Property prop1 = new Property("test", DataType.INT32, false, false);
        List<Property> properties = Arrays.asList(prop1);
        PropertyGroup pg = new PropertyGroup(properties, FileType.CSV, "test/");

        // Try to add property with same name
        Property prop2 = new Property("test", DataType.STRING, false, true);
        Optional<PropertyGroup> result = pg.addPropertyAsNew(prop2);

        Assert.assertFalse(result.isPresent());
    }

    @Test
    public void testPropertyGroupAddNullProperty() {
        Property existingProp = new Property("existing", DataType.INT32, false, false);
        List<Property> properties = Arrays.asList(existingProp);
        PropertyGroup pg = new PropertyGroup(properties, FileType.CSV, "test/");

        Optional<PropertyGroup> result = pg.addPropertyAsNew(null);

        Assert.assertFalse(result.isPresent());
    }

    @Test
    public void testPropertyGroupPropertyMap() {
        Property id = new Property("id", DataType.INT64, true, false);
        Property name = new Property("name", DataType.STRING, false, true);
        Property age = new Property("age", DataType.INT32, false, true);
        List<Property> properties = Arrays.asList(id, name, age);

        PropertyGroup pg = new PropertyGroup(properties, FileType.PARQUET, "person/");

        Assert.assertEquals(3, pg.getPropertyMap().size());
        Assert.assertEquals(id, pg.getPropertyMap().get("id"));
        Assert.assertEquals(name, pg.getPropertyMap().get("name"));
        Assert.assertEquals(age, pg.getPropertyMap().get("age"));
        Assert.assertNull(pg.getPropertyMap().get("nonexistent"));
    }

    @Test
    public void testPropertyGroupImmutability() {
        Property originalProp = new Property("test", DataType.STRING, false, true);
        List<Property> originalProperties = new ArrayList<>(Arrays.asList(originalProp));

        PropertyGroup pg = new PropertyGroup(originalProperties, FileType.CSV, "test/");

        // Modify original list - should not affect PropertyGroup
        originalProperties.add(new Property("new", DataType.INT32, false, false));

        Assert.assertEquals(1, pg.size()); // Should still be 1
        Assert.assertEquals(1, pg.getPropertyList().size());

        // Try to modify returned lists - should throw exception or be unmodifiable
        try {
            pg.getPropertyList().add(new Property("another", DataType.BOOL, false, false));
            Assert.fail("Should not be able to modify property list");
        } catch (UnsupportedOperationException expected) {
            // This is expected behavior
        }

        try {
            pg.getPropertyMap().put("test2", new Property("test2", DataType.FLOAT, false, false));
            Assert.fail("Should not be able to modify property map");
        } catch (UnsupportedOperationException expected) {
            // This is expected behavior
        }
    }

    @Test
    public void testPropertyGroupWithComplexDataTypes() {
        Property listProp = new Property("items", DataType.LIST, false, true);
        Property boolProp = new Property("flag", DataType.BOOL, false, false);
        Property doubleProp = new Property("score", DataType.DOUBLE, false, true);
        List<Property> properties = Arrays.asList(listProp, boolProp, doubleProp);

        PropertyGroup pg = new PropertyGroup(properties, FileType.PARQUET, "complex/");

        Assert.assertEquals(3, pg.size());
        Assert.assertEquals(DataType.LIST, pg.getPropertyMap().get("items").getDataType());
        Assert.assertEquals(DataType.BOOL, pg.getPropertyMap().get("flag").getDataType());
        Assert.assertEquals(DataType.DOUBLE, pg.getPropertyMap().get("score").getDataType());
    }
}
