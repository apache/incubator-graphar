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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import org.apache.graphar.info.type.DataType;
import org.apache.graphar.info.type.FileType;
import org.junit.Assert;
import org.junit.Test;

public class VertexInfoTest {

    private VertexInfo.VertexInfoBuilder defaultBuilder =
            VertexInfo.builder()
                    .baseUri("test")
                    .type("test")
                    .chunkSize(24)
                    .version("gar/v1")
                    .propertyGroups(new PropertyGroups(List.of(TestUtil.pg3)));

    @Test
    public void testVertexInfoBasicBuilder() {
        VertexInfo v = defaultBuilder.build();
    }

    @Test
    public void testVertexInfoBuilderDoubleDeclaration() throws URISyntaxException {
        VertexInfo.VertexInfoBuilder doubleDefinitionBuilder = defaultBuilder.baseUri(new URI("world"));
        VertexInfo v = doubleDefinitionBuilder.build();

        Assert.assertEquals(new URI("world"), v.getBaseUri());
    }

    @Test(expected = RuntimeException.class)
    public void URInullTest() {
        VertexInfo v =
                VertexInfo.builder()
                        .type("test")
                        .chunkSize(24)
                        .version("gar/v1")
                        .propertyGroups(new PropertyGroups(List.of(TestUtil.pg3)))
                        .build();
    }

    @Test
    public void propertyGroupAppendTest() {
        VertexInfo.VertexInfoBuilder propertyGroupAppendBuilder = defaultBuilder.addPropertyGroup(TestUtil.pg2);
        VertexInfo v = propertyGroupAppendBuilder.build();

        Assert.assertEquals(2, v.getPropertyGroups().size());
    }

    @Test
    public void propertyGroupAddOnlyTest() {
        VertexInfo.VertexInfoBuilder propertyGroupAddOnlyBuilder = defaultBuilder.propertyGroups(null).addPropertyGroup(TestUtil.pg2);
        VertexInfo v =  propertyGroupAddOnlyBuilder.build();

        Assert.assertEquals(1, v.getPropertyGroups().size());
    }

    @Test
    public void invalidChunkSizeTest() {
        VertexInfo.VertexInfoBuilder invalidChunkSizeBuilder = defaultBuilder.chunkSize(-1);
        try {
            invalidChunkSizeBuilder.build();
            Assert.assertThrows(IllegalArgumentException.class, () -> invalidChunkSizeBuilder.chunkSize(-1));
        } catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void testBuildWithPrefix() {
        try {
            VertexInfo vertexInfo =
                    new VertexInfo(
                            "person", 100, Arrays.asList(TestUtil.pg1), "vertex/person/", "gar/v1");
            Assert.assertEquals(URI.create("vertex/person/"), vertexInfo.getBaseUri());
        } catch (Exception e) {
            Assert.fail("Should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void testIsValidated() {
        // Test valid vertex info
        VertexInfo validVertexInfo =
                new VertexInfo(
                        "person", 100, Arrays.asList(TestUtil.pg1), "vertex/person/", "gar/v1");
        Assert.assertTrue(validVertexInfo.isValidated());

        // Test invalid vertex info with empty type
        VertexInfo emptyTypeVertexInfo =
                new VertexInfo("", 100, Arrays.asList(TestUtil.pg1), "vertex/person/", "gar/v1");
        Assert.assertFalse(emptyTypeVertexInfo.isValidated());

        // Test invalid vertex info with zero chunk size
        VertexInfo zeroChunkSizeVertexInfo =
                new VertexInfo(
                        "person", 0, Arrays.asList(TestUtil.pg1), "vertex/person/", "gar/v1");
        Assert.assertFalse(zeroChunkSizeVertexInfo.isValidated());

        // Test invalid vertex info with null prefix
        VertexInfo nullPrefixVertexInfo =
                new VertexInfo("person", 100, Arrays.asList(TestUtil.pg1), (URI) null, "gar/v1");
        Assert.assertFalse(nullPrefixVertexInfo.isValidated());

        // Test invalid vertex info with invalid property group
        Property invalidProperty = new Property("", DataType.STRING, false, true);
        PropertyGroup invalidPropertyGroup =
                new PropertyGroup(Arrays.asList(invalidProperty), FileType.CSV, "invalid/");
        VertexInfo invalidPropertyGroupVertexInfo =
                new VertexInfo(
                        "person",
                        100,
                        Arrays.asList(invalidPropertyGroup),
                        "vertex/person/",
                        "gar/v1");
        Assert.assertFalse(invalidPropertyGroupVertexInfo.isValidated());
    }
}
