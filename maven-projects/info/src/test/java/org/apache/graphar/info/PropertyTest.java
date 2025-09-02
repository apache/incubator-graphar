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

import org.apache.graphar.info.type.DataType;
import org.junit.Assert;
import org.junit.Test;

public class PropertyTest {

    @Test
    public void testPropertyBasicConstruction() {
        Property prop = new Property("id", DataType.INT32, true, false);

        Assert.assertEquals("id", prop.getName());
        Assert.assertEquals(DataType.INT32, prop.getDataType());
        Assert.assertTrue(prop.isPrimary());
        Assert.assertFalse(prop.isNullable());
    }

    @Test
    public void testPropertyWithStringType() {
        Property prop = new Property("name", DataType.STRING, false, true);

        Assert.assertEquals("name", prop.getName());
        Assert.assertEquals(DataType.STRING, prop.getDataType());
        Assert.assertFalse(prop.isPrimary());
        Assert.assertTrue(prop.isNullable());
    }

    @Test
    public void testPropertyWithAllDataTypes() {
        Property boolProp = new Property("flag", DataType.BOOL, false, false);
        Assert.assertEquals(DataType.BOOL, boolProp.getDataType());

        Property int32Prop = new Property("count", DataType.INT32, true, false);
        Assert.assertEquals(DataType.INT32, int32Prop.getDataType());

        Property int64Prop = new Property("bigCount", DataType.INT64, false, false);
        Assert.assertEquals(DataType.INT64, int64Prop.getDataType());

        Property floatProp = new Property("score", DataType.FLOAT, false, true);
        Assert.assertEquals(DataType.FLOAT, floatProp.getDataType());

        Property doubleProp = new Property("precision", DataType.DOUBLE, false, true);
        Assert.assertEquals(DataType.DOUBLE, doubleProp.getDataType());

        Property stringProp = new Property("text", DataType.STRING, false, true);
        Assert.assertEquals(DataType.STRING, stringProp.getDataType());

        Property listProp = new Property("items", DataType.LIST, false, true);
        Assert.assertEquals(DataType.LIST, listProp.getDataType());
    }

    @Test
    public void testPropertyFlagCombinations() {
        // Primary and non-nullable (typical ID field)
        Property primaryNonNull = new Property("id", DataType.INT64, true, false);
        Assert.assertTrue(primaryNonNull.isPrimary());
        Assert.assertFalse(primaryNonNull.isNullable());

        // Primary and nullable (unusual but valid)
        Property primaryNull = new Property("optionalId", DataType.INT64, true, true);
        Assert.assertTrue(primaryNull.isPrimary());
        Assert.assertTrue(primaryNull.isNullable());

        // Non-primary and non-nullable (required field)
        Property requiredField = new Property("required", DataType.STRING, false, false);
        Assert.assertFalse(requiredField.isPrimary());
        Assert.assertFalse(requiredField.isNullable());

        // Non-primary and nullable (optional field)
        Property optionalField = new Property("optional", DataType.STRING, false, true);
        Assert.assertFalse(optionalField.isPrimary());
        Assert.assertTrue(optionalField.isNullable());
    }

    @Test
    public void testPropertyWithEmptyName() {
        Property emptyNameProp = new Property("", DataType.STRING, false, true);
        Assert.assertEquals("", emptyNameProp.getName());
        Assert.assertEquals(DataType.STRING, emptyNameProp.getDataType());
    }

    @Test
    public void testPropertyWithNullName() {
        Property nullNameProp = new Property(null, DataType.STRING, false, true);
        Assert.assertNull(nullNameProp.getName());
        Assert.assertEquals(DataType.STRING, nullNameProp.getDataType());
    }

    @Test
    public void testPropertyWithNullDataType() {
        Property nullTypeProp = new Property("test", null, false, true);
        Assert.assertEquals("test", nullTypeProp.getName());
        Assert.assertNull(nullTypeProp.getDataType());
    }

    @Test
    public void testPropertyEquality() {
        Property prop1 = new Property("id", DataType.INT32, true, false);
        Property prop2 = new Property("id", DataType.INT32, true, false);
        Property prop3 = new Property("name", DataType.STRING, false, true);

        // Note: Property class doesn't override equals(), so this tests object identity
        Assert.assertNotEquals(prop1, prop2); // Different objects
        Assert.assertNotEquals(prop1, prop3); // Different properties

        // Same object reference
        Property sameRef = prop1;
        Assert.assertEquals(prop1, sameRef);
    }

    @Test
    public void testPropertyImmutability() {
        Property prop = new Property("test", DataType.INT32, true, false);

        // Properties should be immutable (final fields)
        Assert.assertEquals("test", prop.getName());
        Assert.assertEquals(DataType.INT32, prop.getDataType());
        Assert.assertTrue(prop.isPrimary());
        Assert.assertFalse(prop.isNullable());

        // These values should not change after construction
        Assert.assertEquals("test", prop.getName());
        Assert.assertEquals(DataType.INT32, prop.getDataType());
        Assert.assertTrue(prop.isPrimary());
        Assert.assertFalse(prop.isNullable());
    }
}
