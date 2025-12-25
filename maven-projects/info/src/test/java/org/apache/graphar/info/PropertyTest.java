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
import org.junit.Before;
import org.junit.Test;

public class PropertyTest {

    private Property idProperty;
    private Property nameProperty;
    private Property optionalProperty;

    @Before
    public void setUp() {
        idProperty = TestDataFactory.createIdProperty();
        nameProperty = TestDataFactory.createProperty("name", DataType.STRING, false, true);
        optionalProperty = TestDataFactory.createProperty("optional", DataType.STRING, false, true);
    }

    @Test
    public void testPropertyBasicConstruction() {
        Property prop = TestDataFactory.createProperty("id", DataType.INT32, true, false);

        TestVerificationUtils.verifyProperty(prop, "id", true, false);
        Assert.assertEquals(DataType.INT32, prop.getDataType());
    }

    @Test
    public void testIdProperty() {
        TestVerificationUtils.verifyProperty(idProperty, "id", true, false);
        Assert.assertEquals(DataType.INT64, idProperty.getDataType());
    }

    @Test
    public void testPropertyWithStringType() {
        TestVerificationUtils.verifyProperty(nameProperty, "name", false, true);
        Assert.assertEquals(DataType.STRING, nameProperty.getDataType());
    }

    @Test
    public void testPropertyWithAllDataTypes() {
        Property boolProp = TestDataFactory.createProperty("flag", DataType.BOOL, false, false);
        Assert.assertEquals(DataType.BOOL, boolProp.getDataType());
        TestVerificationUtils.verifyProperty(boolProp, "flag", false, false);

        Property int32Prop = TestDataFactory.createProperty("count", DataType.INT32, true, false);
        Assert.assertEquals(DataType.INT32, int32Prop.getDataType());
        TestVerificationUtils.verifyProperty(int32Prop, "count", true, false);

        Property int64Prop =
                TestDataFactory.createProperty("bigCount", DataType.INT64, false, false);
        Assert.assertEquals(DataType.INT64, int64Prop.getDataType());
        TestVerificationUtils.verifyProperty(int64Prop, "bigCount", false, false);

        Property floatProp = TestDataFactory.createProperty("score", DataType.FLOAT, false, true);
        Assert.assertEquals(DataType.FLOAT, floatProp.getDataType());
        TestVerificationUtils.verifyProperty(floatProp, "score", false, true);

        Property doubleProp =
                TestDataFactory.createProperty("precision", DataType.DOUBLE, false, true);
        Assert.assertEquals(DataType.DOUBLE, doubleProp.getDataType());
        TestVerificationUtils.verifyProperty(doubleProp, "precision", false, true);

        Property stringProp = TestDataFactory.createProperty("text", DataType.STRING, false, true);
        Assert.assertEquals(DataType.STRING, stringProp.getDataType());
        TestVerificationUtils.verifyProperty(stringProp, "text", false, true);

        Property listProp = TestDataFactory.createProperty("items", DataType.LIST, false, true);
        Assert.assertEquals(DataType.LIST, listProp.getDataType());
        TestVerificationUtils.verifyProperty(listProp, "items", false, true);

        // Newly added data types
        Property dateProp =
                TestDataFactory.createProperty("creationDate", DataType.DATE, false, true);
        Assert.assertEquals(DataType.DATE, dateProp.getDataType());
        TestVerificationUtils.verifyProperty(dateProp, "creationDate", false, true);

        Property timestampProp =
                TestDataFactory.createProperty("createdAt", DataType.TIMESTAMP, false, true);
        Assert.assertEquals(DataType.TIMESTAMP, timestampProp.getDataType());
        TestVerificationUtils.verifyProperty(timestampProp, "createdAt", false, true);
    }

    @Test
    public void testPropertyFlagCombinations() {
        // Primary and non-nullable (typical ID field)
        Property primaryNonNull = TestDataFactory.createProperty("id", DataType.INT64, true, false);
        TestVerificationUtils.verifyProperty(primaryNonNull, "id", true, false);

        // Primary and nullable (unusual but valid)
        Property primaryNull =
                TestDataFactory.createProperty("optionalId", DataType.INT64, true, true);
        TestVerificationUtils.verifyProperty(primaryNull, "optionalId", true, true);

        // Non-primary and non-nullable (required field)
        Property requiredField =
                TestDataFactory.createProperty("required", DataType.STRING, false, false);
        TestVerificationUtils.verifyProperty(requiredField, "required", false, false);

        // Non-primary and nullable (optional field)
        TestVerificationUtils.verifyProperty(optionalProperty, "optional", false, true);
    }

    @Test
    public void testPropertyWithEmptyName() {
        Property emptyNameProp = TestDataFactory.createProperty("", DataType.STRING, false, true);
        TestVerificationUtils.verifyProperty(emptyNameProp, "", false, true);
        Assert.assertEquals(DataType.STRING, emptyNameProp.getDataType());
    }

    @Test
    public void testPropertyWithNullName() {
        Property nullNameProp = TestDataFactory.createProperty(null, DataType.STRING, false, true);
        Assert.assertNull(nullNameProp.getName());
        Assert.assertEquals(DataType.STRING, nullNameProp.getDataType());
        Assert.assertFalse(nullNameProp.isPrimary());
        Assert.assertTrue(nullNameProp.isNullable());
    }

    @Test
    public void testPropertyWithNullDataType() {
        Property nullTypeProp = TestDataFactory.createProperty("test", null, false, true);
        TestVerificationUtils.verifyProperty(nullTypeProp, "test", false, true);
        Assert.assertNull(nullTypeProp.getDataType());
    }

    @Test
    public void testPropertyEquality() {
        Property prop1 = TestDataFactory.createProperty("id", DataType.INT32, true, false);
        Property prop2 = TestDataFactory.createProperty("id", DataType.INT32, true, false);
        Property prop3 = TestDataFactory.createProperty("name", DataType.STRING, false, true);

        // Note: Property class doesn't override equals(), so this tests object identity
        Assert.assertNotEquals(prop1, prop2); // Different objects
        Assert.assertNotEquals(prop1, prop3); // Different properties

        // Same object reference
        Property sameRef = prop1;
        Assert.assertEquals(prop1, sameRef);
    }

    @Test
    public void testPropertyImmutability() {
        Property prop = TestDataFactory.createProperty("test", DataType.INT32, true, false);

        // Properties should be immutable (final fields)
        TestVerificationUtils.verifyProperty(prop, "test", true, false);
        Assert.assertEquals(DataType.INT32, prop.getDataType());

        // These values should not change after construction
        TestVerificationUtils.verifyProperty(prop, "test", true, false);
        Assert.assertEquals(DataType.INT32, prop.getDataType());
    }
}
