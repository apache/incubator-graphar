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
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class EdgeInfoTest {

    private final EdgeInfo.EdgeInfoBuilder e =
            EdgeInfo.builder()
                    .srcType("person")
                    .edgeType("knows")
                    .chunkSize(1024)
                    .srcChunkSize(100)
                    .dstChunkSize(100)
                    .directed(false)
                    .baseUri(URI.create("edge/person_knows_person/"))
                    .version("gar/v1");

    @Test
    public void testBuildWithPrefix() {
        EdgeInfo edgeInfo =
                EdgeInfo.builder()
                        .srcType("person")
                        .edgeType("knows")
                        .dstType("person")
                        .propertyGroups(new PropertyGroups(List.of(TestUtil.pg3)))
                        .adjacentLists(List.of(TestUtil.orderedBySource))
                        .chunkSize(1024)
                        .srcChunkSize(100)
                        .dstChunkSize(100)
                        .directed(false)
                        .prefix("edge/person_knows_person/")
                        .version("gar/v1")
                        .build();
        Assert.assertEquals(URI.create("edge/person_knows_person/"), edgeInfo.getBaseUri());
    }

    @Test
    public void testUriAndPrefixConflict() {
        try {
            EdgeInfo.builder()
                    .srcType("person")
                    .edgeType("knows")
                    .dstType("person")
                    .propertyGroups(new PropertyGroups(List.of(TestUtil.pg3)))
                    .adjacentLists(List.of(TestUtil.orderedBySource))
                    .chunkSize(1024)
                    .srcChunkSize(100)
                    .dstChunkSize(100)
                    .directed(false)
                    .prefix("edge/person_knows_person/")
                    .baseUri(URI.create("/person_knows_person/"))
                    .version("gar/v1")
                    .build();
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(
                    "baseUri and prefix conflict: baseUri=/person_knows_person/ prefix=edge/person_knows_person/",
                    e.getMessage());
        }
    }

    @Test
    public void testMissingUriAndPrefix() {
        try {
            EdgeInfo.builder()
                    .srcType("person")
                    .edgeType("knows")
                    .dstType("person")
                    .propertyGroups(new PropertyGroups(List.of(TestUtil.pg3)))
                    .adjacentLists(List.of(TestUtil.orderedBySource))
                    .chunkSize(1024)
                    .srcChunkSize(100)
                    .dstChunkSize(100)
                    .directed(false)
                    .version("gar/v1")
                    .build();
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("baseUri and prefix cannot be both null", e.getMessage());
        }
    }

    @Test
    public void erroneousTripletEdgeBuilderTest() {
        try {
            e.adjacentLists(List.of(TestUtil.orderedBySource, TestUtil.orderedByDest))
                    .addPropertyGroups(List.of(TestUtil.pg3))
                    .build();
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("Edge triplet is null", e.getMessage());
        }
    }

    @Test
    public void emptyAdjacentListEdgeBuilderTest() {
        try {
            e.dstType("person").addPropertyGroups(List.of(TestUtil.pg3)).build();
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("AdjacentLists is empty", e.getMessage());
        }
    }

    @Test
    public void emptyPropertyGroupsEdgeBuilderTest() {
        try {
            e.adjacentLists(List.of(TestUtil.orderedBySource, TestUtil.orderedByDest))
                    .dstType("person")
                    .build();
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("PropertyGroups is empty", e.getMessage());
        }
    }

    @Test
    public void addMethodsTest() {

        EdgeInfo edgeInfo =
                e.addPropertyGroup(TestUtil.pg3)
                        .addAdjacentList(TestUtil.orderedBySource)
                        .addAdjacentList(TestUtil.orderedByDest)
                        .dstType("person")
                        .build();

        Assert.assertEquals(2, edgeInfo.getAdjacentLists().size());
        Assert.assertEquals(1, edgeInfo.getPropertyGroups().size());
    }

    @Test
    public void appendMethodsTest() {
        EdgeInfo edgeInfo =
                e.propertyGroups(new PropertyGroups(List.of(TestUtil.pg3)))
                        .adjacentLists(List.of(TestUtil.orderedBySource))
                        .addAdjacentList(TestUtil.orderedByDest)
                        .addPropertyGroups(List.of(TestUtil.pg2))
                        .dstType("person")
                        .build();

        Assert.assertEquals(2, edgeInfo.getAdjacentLists().size());
        Assert.assertEquals(2, edgeInfo.getPropertyGroups().size());
    }
}
