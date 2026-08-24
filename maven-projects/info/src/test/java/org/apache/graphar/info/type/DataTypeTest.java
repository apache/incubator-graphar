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

package org.apache.graphar.info.type;

import org.apache.graphar.info.Property;
import org.apache.graphar.info.yaml.PropertyYaml;
import org.junit.Assert;
import org.junit.Test;

public class DataTypeTest {

    @Test
    public void testListTypeRoundTrip() {
        DataType listType = DataType.listOf(DataType.INT64);

        Assert.assertEquals("list<int64>", listType.toString());
        Assert.assertTrue(listType.isList());
        Assert.assertEquals(DataType.INT64, listType.getValueType());
        Assert.assertEquals(listType, DataType.fromString("list<int64>"));

        Property property = new Property("ids", listType, false, true);
        Property roundTripped = new Property(new PropertyYaml(property));
        Assert.assertEquals(listType, roundTripped.getDataType());
    }

    @Test
    public void testUnsupportedListTypesFailFast() {
        Assert.assertThrows(IllegalArgumentException.class, () -> DataType.fromString("list"));
        Assert.assertThrows(
                IllegalArgumentException.class, () -> DataType.fromString("list<bool>"));
        Assert.assertThrows(
                IllegalArgumentException.class, () -> DataType.fromString("list<list<int32>>"));
    }
}
