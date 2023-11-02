/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphar.edges;

import static com.alibaba.graphar.graphinfo.GraphInfoTest.root;

import com.alibaba.graphar.graphinfo.GraphInfo;
import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.types.AdjListType;
import org.junit.Assert;
import org.junit.Test;

public class EdgesCollectionTest {
    @Test
    public void test1() {
        String path = root + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
        StdString srcLabel = StdString.create("person");
        StdString edgeLabel = StdString.create("knows");
        StdString dstLabel = StdString.create("person");
        GraphInfo graphInfo = GraphInfo.load(path).value();

        // iterate edges of vertex chunk 0
        EdgesCollection edges =
                EdgesCollection.create(
                        graphInfo,
                        srcLabel.toJavaString(),
                        edgeLabel.toJavaString(),
                        dstLabel.toJavaString(),
                        AdjListType.ordered_by_source,
                        0,
                        1);
        EdgeIter it = edges.begin();
        long count = 0L;
        for (Edge edge : edges) {
            // access data through iterator directly
            System.out.print("src=" + it.source() + ", dst=" + it.destination() + " ");
            // access data through edge
            Assert.assertEquals(edge.source(), it.source());
            Assert.assertEquals(edge.destination(), it.destination());
            StdString creationDate = StdString.create("creationDate");
            System.out.println("creationDate=" + edge.property(creationDate, creationDate).value());
            count++;
            it.inc();
        }
        System.out.println("edge_count=" + count);
        Assert.assertEquals(count, edges.size());

        // iterate edges of vertex chunk [2, 4)
        EdgesCollection edges1 =
                EdgesCollection.create(
                        graphInfo,
                        srcLabel.toJavaString(),
                        edgeLabel.toJavaString(),
                        dstLabel.toJavaString(),
                        AdjListType.ordered_by_dest,
                        2,
                        4);
        EdgeIter end1 = edges1.end();
        long count1 = 0;
        for (Edge edge : edges1) {
            count1++;
        }
        System.out.println("edge_count=" + count1);
        Assert.assertEquals(count1, edges1.size());

        // iterate all edges
        EdgesCollection edges2 =
                EdgesCollection.create(
                        graphInfo,
                        srcLabel.toJavaString(),
                        edgeLabel.toJavaString(),
                        dstLabel.toJavaString(),
                        AdjListType.ordered_by_source);
        long count2 = 0;
        for (Edge edge : edges2) {
            System.out.println("src=" + edge.source() + ", dst=" + edge.destination());
            count2++;
        }
        System.out.println("edge_count=" + count2);
        Assert.assertEquals(count2, edges2.size());

        // empty collection
        EdgesCollection edges3 =
                EdgesCollection.create(
                        graphInfo,
                        srcLabel.toJavaString(),
                        edgeLabel.toJavaString(),
                        dstLabel.toJavaString(),
                        AdjListType.unordered_by_source,
                        5,
                        5);
        long count3 = 0;
        for (Edge edge : edges3) {
            count3++;
        }
        System.out.println("edge_count=" + count3);
        Assert.assertEquals(0, count3);
        Assert.assertEquals(0, edges3.size());
    }
}
