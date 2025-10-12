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
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class VertexInfoTest {

    private VertexInfo.VertexInfoBuilder b =
            VertexInfo.builder()
                    .baseUri("test")
                    .type("test")
                    .chunkSize(24)
                    .version("gar/v1")
                    .propertyGroups(new PropertyGroups(List.of(TestUtil.pg3)));

    @Test
    public void testVertexInfoBasicBuilder() {
        VertexInfo v = b.build();
    }

    @Test
    public void testVertexInfoBuilderDoubleDeclaration() throws URISyntaxException {
        VertexInfo v = b.baseUri(new URI("world")).build();

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
        VertexInfo v = b.addPropertyGroup(TestUtil.pg2).build();

        Assert.assertEquals(2, v.getPropertyGroups().size());
    }

    @Test
    public void propertyGroupAddOnlyTest() {
        VertexInfo v = b.propertyGroups(null).addPropertyGroup(TestUtil.pg2).build();

        Assert.assertEquals(1, v.getPropertyGroups().size());
    }

    @Test(expected = RuntimeException.class)
    public void invalidChunkSizeTest() {
        b.chunkSize(-1);
        b.build();
    }
}
