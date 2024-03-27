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

package com.alibaba.graphar.edges;

import static com.alibaba.graphar.graphinfo.GraphInfoTest.root;

import com.alibaba.graphar.graphinfo.GraphInfo;
import com.alibaba.graphar.stdcxx.StdSharedPtr;
import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.types.AdjListType;
import com.alibaba.graphar.util.GrapharStaticFunctions;
import com.alibaba.graphar.util.Result;
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
        Result<StdSharedPtr<EdgesCollection>> maybeEdges =
                GrapharStaticFunctions.INSTANCE.constructEdgesCollection(
                        graphInfo,
                        srcLabel,
                        edgeLabel,
                        dstLabel,
                        AdjListType.ordered_by_source,
                        0,
                        1);
        Assert.assertTrue(maybeEdges.status().ok());
        StdSharedPtr<EdgesCollection> edges = maybeEdges.value();
        EdgeIter it = edges.get().begin();
        long count = 0L;
        for (Edge edge : edges.get()) {
            // access data through edge
            Assert.assertEquals(edge.source(), it.source());
            Assert.assertEquals(edge.destination(), it.destination());
            StdString creationDate = StdString.create("creationDate");
            count++;
            it.inc();
        }
        Assert.assertEquals(count, edges.get().size());

        // iterate edges of vertex chunk [2, 4)
        Result<StdSharedPtr<EdgesCollection>> maybeEdges1 =
                GrapharStaticFunctions.INSTANCE.constructEdgesCollection(
                        graphInfo,
                        srcLabel,
                        edgeLabel,
                        dstLabel,
                        AdjListType.ordered_by_dest,
                        2,
                        4);
        Assert.assertTrue(maybeEdges.status().ok());
        StdSharedPtr<EdgesCollection> edges1 = maybeEdges1.value();
        EdgeIter end1 = edges1.get().end();
        long count1 = 0;
        for (Edge edge : edges1.get()) {
            count1++;
        }
        Assert.assertEquals(count1, edges1.get().size());

        // iterate all edges
        Result<StdSharedPtr<EdgesCollection>> maybeEdges2 =
                GrapharStaticFunctions.INSTANCE.constructEdgesCollection(
                        graphInfo, srcLabel, edgeLabel, dstLabel, AdjListType.ordered_by_source);
        Assert.assertTrue(maybeEdges.status().ok());
        StdSharedPtr<EdgesCollection> edges2 = maybeEdges2.value();
        long count2 = 0;
        for (Edge edge : edges2.get()) {
            count2++;
        }
        Assert.assertEquals(count2, edges2.get().size());

        // empty collection
        Result<StdSharedPtr<EdgesCollection>> maybeEdges3 =
                GrapharStaticFunctions.INSTANCE.constructEdgesCollection(
                        graphInfo,
                        srcLabel,
                        edgeLabel,
                        dstLabel,
                        AdjListType.unordered_by_source,
                        5,
                        5);
        Assert.assertTrue(maybeEdges3.status().ok());
        StdSharedPtr<EdgesCollection> edges3 = maybeEdges3.value();
        long count3 = 0;
        for (Edge edge : edges3.get()) {
            count3++;
        }
        Assert.assertEquals(0, count3);
        Assert.assertEquals(0, edges3.get().size());
    }
}
