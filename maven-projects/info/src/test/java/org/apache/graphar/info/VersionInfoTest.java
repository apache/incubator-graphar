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
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class VersionInfoTest {

    @Test
    public void testVersionInfoBasicConstruction() {
        VersionInfo versionInfo = new VersionInfo(1, null);

        Assert.assertEquals(1, versionInfo.getVersion());
        Assert.assertTrue(versionInfo.getUserDefinedTypes().isEmpty());
        Assert.assertEquals("gar/v1", versionInfo.toString());
    }

    @Test
    public void testVersionInfoWithEmptyUserDefinedTypes() {
        List<String> emptyTypes = Collections.emptyList();
        VersionInfo versionInfo = new VersionInfo(1, emptyTypes);

        Assert.assertEquals(1, versionInfo.getVersion());
        Assert.assertEquals(emptyTypes, versionInfo.getUserDefinedTypes());
        Assert.assertEquals("gar/v1", versionInfo.toString());
    }

    @Test
    public void testVersionInfoWithUserDefinedTypes() {
        List<String> userTypes = Arrays.asList("customType1", "customType2");
        VersionInfo versionInfo = new VersionInfo(1, userTypes);

        Assert.assertEquals(1, versionInfo.getVersion());
        Assert.assertEquals(userTypes, versionInfo.getUserDefinedTypes());
        Assert.assertEquals("gar/v1 (customType1,customType2)", versionInfo.toString());
    }

    @Test
    public void testVersionInfoWithSingleUserDefinedType() {
        List<String> userTypes = Arrays.asList("singleType");
        VersionInfo versionInfo = new VersionInfo(1, userTypes);

        Assert.assertEquals(1, versionInfo.getVersion());
        Assert.assertEquals(userTypes, versionInfo.getUserDefinedTypes());
        Assert.assertEquals("gar/v1 (singleType)", versionInfo.toString());
    }

    @Test
    public void testVersionInfoCheckTypeBuiltInTypes() {
        VersionInfo versionInfo = new VersionInfo(1, null);

        // Test built-in types for version 1
        Assert.assertTrue(versionInfo.checkType("bool"));
        Assert.assertTrue(versionInfo.checkType("int32"));
        Assert.assertTrue(versionInfo.checkType("int64"));
        Assert.assertTrue(versionInfo.checkType("float"));
        Assert.assertTrue(versionInfo.checkType("double"));
        Assert.assertTrue(versionInfo.checkType("string"));
    }

    @Test
    public void testVersionInfoCheckTypeNonExistentTypes() {
        VersionInfo versionInfo = new VersionInfo(1, null);

        // Test non-existent types
        Assert.assertFalse(versionInfo.checkType("date32"));
        Assert.assertFalse(versionInfo.checkType("timestamp"));
        Assert.assertFalse(versionInfo.checkType("binary"));
        Assert.assertFalse(versionInfo.checkType("customType"));
        Assert.assertFalse(versionInfo.checkType(""));
    }

    @Test
    public void testVersionInfoCheckTypeWithUserDefinedTypes() {
        List<String> userTypes = Arrays.asList("customType1", "customType2");
        VersionInfo versionInfo = new VersionInfo(1, userTypes);

        // Test built-in types still work
        Assert.assertTrue(versionInfo.checkType("int32"));
        Assert.assertTrue(versionInfo.checkType("string"));

        // Test user-defined types
        Assert.assertTrue(versionInfo.checkType("customType1"));
        Assert.assertTrue(versionInfo.checkType("customType2"));

        // Test non-existent types
        Assert.assertFalse(versionInfo.checkType("nonExistentType"));
        Assert.assertFalse(versionInfo.checkType("date32"));
    }

    @Test
    public void testVersionInfoCheckTypeWithNullType() {
        VersionInfo versionInfo = new VersionInfo(1, null);

        // The current implementation doesn't handle null gracefully, so this will throw NPE
        try {
            versionInfo.checkType(null);
            Assert.fail("Expected NullPointerException");
        } catch (NullPointerException expected) {
            // This is the current behavior - null parameter causes NPE
        }
    }

    @Test
    public void testVersionInfoCheckTypeWithUnsupportedVersionButUserTypes() {
        List<String> userTypes = Arrays.asList("customType");
        VersionInfo versionInfo = new VersionInfo(999, userTypes);

        // Unsupported version should not support built-in types
        Assert.assertFalse(versionInfo.checkType("int32"));
        Assert.assertFalse(versionInfo.checkType("string"));

        // Current implementation: unsupported version returns false for all types,
        // including user-defined types, due to early return in checkType()
        Assert.assertFalse(versionInfo.checkType("customType"));
    }

    @Test
    public void testVersionInfoToStringWithMultipleTypes() {
        List<String> userTypes = Arrays.asList("type1", "type2", "type3", "type4");
        VersionInfo versionInfo = new VersionInfo(1, userTypes);

        String expected = "gar/v1 (type1,type2,type3,type4)";
        Assert.assertEquals(expected, versionInfo.toString());
    }

    @Test
    public void testVersionInfoToStringWithDifferentVersions() {
        VersionInfo version1 = new VersionInfo(1, null);
        VersionInfo version2 = new VersionInfo(2, null);
        VersionInfo version10 = new VersionInfo(10, null);

        Assert.assertEquals("gar/v1", version1.toString());
        Assert.assertEquals("gar/v2", version2.toString());
        Assert.assertEquals("gar/v10", version10.toString());
    }

    @Test
    public void testVersionInfoToStringWithSpecialCharactersInTypes() {
        List<String> userTypes =
                Arrays.asList("type-with-dash", "type_with_underscore", "type.with.dots");
        VersionInfo versionInfo = new VersionInfo(1, userTypes);

        String expected = "gar/v1 (type-with-dash,type_with_underscore,type.with.dots)";
        Assert.assertEquals(expected, versionInfo.toString());
    }

    @Test
    public void testVersionInfoWithZeroVersion() {
        try {
            new VersionInfo(0, null);
            Assert.fail("Expected IllegalArgumentException for zero version");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("Error message should mention version", 
                e.getMessage().contains("Version must be a supported positive integer"));
        }
    }

    @Test
    public void testVersionInfoWithNegativeVersion() {
        try {
            new VersionInfo(-1, null);
            Assert.fail("Expected IllegalArgumentException for negative version");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("Error message should mention version", 
                e.getMessage().contains("Version must be a supported positive integer"));
        }
    }

    @Test
    public void testVersionInfoCaseSensitivity() {
        List<String> userTypes = Arrays.asList("MyType", "mytype", "MYTYPE");
        VersionInfo versionInfo = new VersionInfo(1, userTypes);

        // Should be case sensitive
        Assert.assertTrue(versionInfo.checkType("MyType"));
        Assert.assertTrue(versionInfo.checkType("mytype"));
        Assert.assertTrue(versionInfo.checkType("MYTYPE"));
        Assert.assertFalse(versionInfo.checkType("myType"));
        Assert.assertFalse(versionInfo.checkType("Mytype"));
    }

    @Test
    public void testVersionInfoBuiltInTypeCaseSensitivity() {
        VersionInfo versionInfo = new VersionInfo(1, null);

        // Built-in types should be case sensitive
        Assert.assertTrue(versionInfo.checkType("int32"));
        Assert.assertTrue(versionInfo.checkType("string"));
        Assert.assertFalse(versionInfo.checkType("INT32"));
        Assert.assertFalse(versionInfo.checkType("String"));
        Assert.assertFalse(versionInfo.checkType("INT64"));
    }
}
