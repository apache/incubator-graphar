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

import org.apache.graphar.info.type.AdjListType;
import org.apache.graphar.info.type.FileType;
import org.junit.Assert;
import org.junit.Test;

public class AdjacentListTest {

    @Test
    public void testAdjacentListBasicConstruction() {
        AdjacentList adjList =
                new AdjacentList(AdjListType.unordered_by_source, FileType.CSV, "adj_list/");

        Assert.assertEquals(AdjListType.unordered_by_source, adjList.getType());
        Assert.assertEquals(FileType.CSV, adjList.getFileType());
        Assert.assertEquals("adj_list/", adjList.getPrefix());
    }

    @Test
    public void testAdjacentListWithAllAdjListTypes() {
        AdjacentList unorderedBySource =
                new AdjacentList(AdjListType.unordered_by_source, FileType.CSV, "unordered_src/");
        Assert.assertEquals(AdjListType.unordered_by_source, unorderedBySource.getType());

        AdjacentList unorderedByDest =
                new AdjacentList(AdjListType.unordered_by_dest, FileType.CSV, "unordered_dst/");
        Assert.assertEquals(AdjListType.unordered_by_dest, unorderedByDest.getType());

        AdjacentList orderedBySource =
                new AdjacentList(AdjListType.ordered_by_source, FileType.CSV, "ordered_src/");
        Assert.assertEquals(AdjListType.ordered_by_source, orderedBySource.getType());

        AdjacentList orderedByDest =
                new AdjacentList(AdjListType.ordered_by_dest, FileType.CSV, "ordered_dst/");
        Assert.assertEquals(AdjListType.ordered_by_dest, orderedByDest.getType());
    }

    @Test
    public void testAdjacentListWithAllFileTypes() {
        AdjacentList csvList =
                new AdjacentList(AdjListType.unordered_by_source, FileType.CSV, "csv/");
        Assert.assertEquals(FileType.CSV, csvList.getFileType());

        AdjacentList parquetList =
                new AdjacentList(AdjListType.unordered_by_source, FileType.PARQUET, "parquet/");
        Assert.assertEquals(FileType.PARQUET, parquetList.getFileType());

        AdjacentList orcList =
                new AdjacentList(AdjListType.unordered_by_source, FileType.ORC, "orc/");
        Assert.assertEquals(FileType.ORC, orcList.getFileType());
    }

    @Test
    public void testAdjacentListTypeAndFormatCombinations() {
        // Test unordered by source with PARQUET (COO format)
        AdjacentList cooList =
                new AdjacentList(AdjListType.unordered_by_source, FileType.PARQUET, "coo/");
        Assert.assertEquals(AdjListType.unordered_by_source, cooList.getType());
        Assert.assertEquals(FileType.PARQUET, cooList.getFileType());

        // Test ordered by source with CSV (CSR format)
        AdjacentList csrList =
                new AdjacentList(AdjListType.ordered_by_source, FileType.CSV, "csr/");
        Assert.assertEquals(AdjListType.ordered_by_source, csrList.getType());
        Assert.assertEquals(FileType.CSV, csrList.getFileType());

        // Test ordered by dest with ORC (CSC format)
        AdjacentList cscList = new AdjacentList(AdjListType.ordered_by_dest, FileType.ORC, "csc/");
        Assert.assertEquals(AdjListType.ordered_by_dest, cscList.getType());
        Assert.assertEquals(FileType.ORC, cscList.getFileType());
    }

    @Test
    public void testAdjacentListWithNullPrefix() {
        AdjacentList adjList =
                new AdjacentList(AdjListType.unordered_by_source, FileType.CSV, (String) null);

        Assert.assertEquals(AdjListType.unordered_by_source, adjList.getType());
        Assert.assertEquals(FileType.CSV, adjList.getFileType());
        Assert.assertNull(adjList.getPrefix());
    }

    @Test
    public void testAdjacentListWithEmptyPrefix() {
        AdjacentList adjList =
                new AdjacentList(AdjListType.ordered_by_source, FileType.PARQUET, "");

        Assert.assertEquals(AdjListType.ordered_by_source, adjList.getType());
        Assert.assertEquals(FileType.PARQUET, adjList.getFileType());
        Assert.assertEquals("", adjList.getPrefix());
    }

    @Test
    public void testAdjacentListWithNullType() {
        AdjacentList adjList = new AdjacentList(null, FileType.CSV, "test/");

        Assert.assertNull(adjList.getType());
        Assert.assertEquals(FileType.CSV, adjList.getFileType());
        Assert.assertEquals("test/", adjList.getPrefix());
    }

    @Test
    public void testAdjacentListWithNullFileType() {
        AdjacentList adjList = new AdjacentList(AdjListType.unordered_by_source, null, "test/");

        Assert.assertEquals(AdjListType.unordered_by_source, adjList.getType());
        Assert.assertNull(adjList.getFileType());
        Assert.assertEquals("test/", adjList.getPrefix());
    }

    @Test
    public void testAdjacentListImmutability() {
        AdjListType originalType = AdjListType.ordered_by_source;
        FileType originalFileType = FileType.PARQUET;
        String originalPrefix = "original/";

        AdjacentList adjList = new AdjacentList(originalType, originalFileType, originalPrefix);

        // Values should remain the same after construction
        Assert.assertEquals(originalType, adjList.getType());
        Assert.assertEquals(originalFileType, adjList.getFileType());
        Assert.assertEquals(originalPrefix, adjList.getPrefix());

        // References should be the same (since they're immutable)
        Assert.assertSame(originalType, adjList.getType());
        Assert.assertSame(originalFileType, adjList.getFileType());
        Assert.assertEquals(originalPrefix, adjList.getPrefix()); // String comparison
    }

    @Test
    public void testAdjacentListEquality() {
        AdjacentList adjList1 =
                new AdjacentList(AdjListType.unordered_by_source, FileType.CSV, "test/");
        AdjacentList adjList2 =
                new AdjacentList(AdjListType.unordered_by_source, FileType.CSV, "test/");
        AdjacentList adjList3 =
                new AdjacentList(AdjListType.ordered_by_source, FileType.CSV, "test/");

        // Note: AdjacentList doesn't override equals(), so this tests object identity
        Assert.assertNotEquals(adjList1, adjList2); // Different objects
        Assert.assertNotEquals(adjList1, adjList3); // Different types

        // Same object reference
        AdjacentList sameRef = adjList1;
        Assert.assertEquals(adjList1, sameRef);
    }

    @Test
    public void testAdjacentListDefaultPrefixBehavior() {
        // Test behavior that might generate default prefixes based on type
        AdjacentList unorderedSrc =
                new AdjacentList(
                        AdjListType.unordered_by_source, FileType.CSV, "unordered_by_source/");
        Assert.assertEquals("unordered_by_source/", unorderedSrc.getPrefix());

        AdjacentList orderedSrc =
                new AdjacentList(
                        AdjListType.ordered_by_source, FileType.PARQUET, "ordered_by_source/");
        Assert.assertEquals("ordered_by_source/", orderedSrc.getPrefix());

        AdjacentList unorderedDst =
                new AdjacentList(AdjListType.unordered_by_dest, FileType.ORC, "unordered_by_dest/");
        Assert.assertEquals("unordered_by_dest/", unorderedDst.getPrefix());

        AdjacentList orderedDst =
                new AdjacentList(AdjListType.ordered_by_dest, FileType.CSV, "ordered_by_dest/");
        Assert.assertEquals("ordered_by_dest/", orderedDst.getPrefix());
    }

    @Test
    public void testAdjacentListFieldAccess() {
        AdjListType type = AdjListType.ordered_by_source;
        FileType fileType = FileType.PARQUET;
        String prefix = "edge_data/";

        AdjacentList adjList = new AdjacentList(type, fileType, prefix);

        // Test that getters return the correct values
        Assert.assertEquals(type, adjList.getType());
        Assert.assertEquals(fileType, adjList.getFileType());
        Assert.assertEquals(prefix, adjList.getPrefix());

        // Test multiple calls return consistent values
        Assert.assertEquals(type, adjList.getType());
        Assert.assertEquals(fileType, adjList.getFileType());
        Assert.assertEquals(prefix, adjList.getPrefix());
    }
}
