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

package org.apache.graphar.graphinfo;

import com.alibaba.fastffi.CXXReference;
import org.apache.graphar.stdcxx.StdSharedPtr;
import org.apache.graphar.stdcxx.StdString;
import org.apache.graphar.stdcxx.StdVector;
import org.apache.graphar.types.AdjListType;
import org.apache.graphar.types.FileType;
import org.apache.graphar.util.GrapharStaticFunctions;
import org.apache.graphar.util.Result;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class GraphInfoTest {
    public static final String root = System.getenv("GAR_TEST_DATA") + "/java";

    @Test
    public void test1() {
        String graphName = "test_graph";

        StdVector.Factory<StdSharedPtr<VertexInfo>> vertexInfoVecFactory =
                StdVector.getStdVectorFactory("std::vector<std::shared_ptr<graphar::VertexInfo>>");
        StdVector<StdSharedPtr<VertexInfo>> vertexInfoVector = vertexInfoVecFactory.create();

        StdVector.Factory<StdSharedPtr<EdgeInfo>> edgeInfoVecFactory =
                StdVector.getStdVectorFactory("std::vector<std::shared_ptr<graphar::EdgeInfo>>");
        StdVector<StdSharedPtr<EdgeInfo>> edgeInfoVector = edgeInfoVecFactory.create();
        String prefix = "test_prefix";
        StdSharedPtr<GraphInfo> graphInfoStdSharedPtr =
                GrapharStaticFunctions.INSTANCE.createGraphInfo(
                        StdString.create(graphName),
                        vertexInfoVector,
                        edgeInfoVector,
                        StdString.create(prefix));
        GraphInfo graphInfo = graphInfoStdSharedPtr.get();
        Assert.assertEquals(graphName, graphInfo.getName().toJavaString());
        Assert.assertEquals(prefix, graphInfo.getPrefix().toJavaString());

        // test add vertex and get vertex info
        StdString vertexLabel = StdString.create("test_vertex");
        long vertexChunkSize = 100;
        StdString vertexPrefix = StdString.create("test_vertex_prefix");
        StdString vertexInfoPath = StdString.create("/tmp/test_vertex.vertex.yml");
        StdString unknownLabel = StdString.create("text_not_exist");
        StdVector.Factory<StdSharedPtr<PropertyGroup>> propertyGroupVecFactory =
                StdVector.getStdVectorFactory(
                        "std::vector<std::shared_ptr<graphar::PropertyGroup>>");
        StdVector<StdSharedPtr<PropertyGroup>> propertyGroupStdVector =
                propertyGroupVecFactory.create();
        StdSharedPtr<VertexInfo> vertexInfo =
                GrapharStaticFunctions.INSTANCE.createVertexInfo(
                        vertexLabel, vertexChunkSize, propertyGroupStdVector, vertexPrefix);
        Assert.assertEquals(0, graphInfo.getVertexInfos().size());
        Result<StdSharedPtr<GraphInfo>> addVertex = graphInfo.addVertex(vertexInfo);
        Assert.assertTrue(addVertex.status().ok());
        graphInfo = addVertex.value().get();
        Assert.assertTrue(graphInfo.addVertex(vertexInfo).hasError());
        Assert.assertEquals(1, graphInfo.getVertexInfos().size());
        StdSharedPtr<VertexInfo> maybeVertexInfo = graphInfo.getVertexInfo(vertexLabel);
        Assert.assertNotNull(maybeVertexInfo.get());
        Assert.assertTrue(vertexLabel.eq(maybeVertexInfo.get().getLabel()));
        Assert.assertTrue(vertexPrefix.eq(maybeVertexInfo.get().getPrefix()));
        Assert.assertNull(graphInfo.getVertexInfo(unknownLabel).get());
        // existed vertex info can't be added again
        Assert.assertTrue(graphInfo.addVertex(vertexInfo).status().isInvalid());

        // test add edge and get edge info
        StdString srcLabel = StdString.create("test_vertex");
        StdString edgeLabel = StdString.create("test_edge");
        StdString dstLabel = StdString.create("test_vertex");
        long edgeChunkSize = 1024;
        StdString edgeInfoPath = StdString.create("/tmp/test_edge.edge.yml");
        AdjListType adjListType = AdjListType.ordered_by_source;
        FileType fileType = FileType.PARQUET;
        StdSharedPtr<AdjacentList> adjacentList =
                GrapharStaticFunctions.INSTANCE.createAdjacentList(adjListType, fileType);
        StdVector.Factory<StdSharedPtr<AdjacentList>> adjancyListVecFactory =
                StdVector.getStdVectorFactory(
                        "std::vector<std::shared_ptr<graphar::AdjacentList>>");
        StdVector<StdSharedPtr<AdjacentList>> adjacentListStdVector =
                adjancyListVecFactory.create();
        adjacentListStdVector.push_back(adjacentList);
        StdSharedPtr<EdgeInfo> edgeInfo =
                GrapharStaticFunctions.INSTANCE.createEdgeInfo(
                        srcLabel,
                        edgeLabel,
                        dstLabel,
                        edgeChunkSize,
                        vertexChunkSize,
                        vertexChunkSize,
                        true,
                        adjacentListStdVector,
                        propertyGroupStdVector,
                        StdString.create(prefix));
        Assert.assertEquals(0, graphInfo.getEdgeInfos().size());
        Result<StdSharedPtr<GraphInfo>> addEdgeGraph = graphInfo.addEdge(edgeInfo);
        Assert.assertTrue(addEdgeGraph.status().ok());
        graphInfo = addEdgeGraph.value().get();
        Assert.assertEquals(1, graphInfo.getEdgeInfos().size());
        StdSharedPtr<EdgeInfo> edgeInfoPtr = graphInfo.getEdgeInfo(srcLabel, edgeLabel, dstLabel);
        Assert.assertNotNull(edgeInfoPtr.get());
        Assert.assertTrue(srcLabel.eq(edgeInfoPtr.get().getSrcLabel()));
        Assert.assertTrue(edgeLabel.eq(edgeInfoPtr.get().getEdgeLabel()));
        Assert.assertTrue(dstLabel.eq(edgeInfoPtr.get().getDstLabel()));
        Assert.assertNull(graphInfo.getEdgeInfo(unknownLabel, unknownLabel, unknownLabel).get());
        // existed edge info can't be added again
        Assert.assertTrue(graphInfo.addEdge(edgeInfo).status().isInvalid());
    }

    @Test
    public void testGraphInfoLoadFromFile() {
        String path = root + "/ldbc_sample/csv/ldbc_sample.graph.yml";
        Result<StdSharedPtr<GraphInfo>> graphInfoResult = GraphInfo.load(path);
        Assert.assertFalse(graphInfoResult.hasError());
        StdSharedPtr<GraphInfo> graphInfo = graphInfoResult.value();
        Assert.assertEquals("ldbc_sample", graphInfo.get().getName().toJavaString());
        Assert.assertEquals(root + "/ldbc_sample/csv/", graphInfo.get().getPrefix().toJavaString());
        StdVector<StdSharedPtr<@CXXReference VertexInfo>> vertexInfos =
                graphInfo.get().getVertexInfos();
        StdVector<StdSharedPtr<@CXXReference EdgeInfo>> edgeInfos = graphInfo.get().getEdgeInfos();
        Assert.assertEquals(1, vertexInfos.size());
        Assert.assertEquals(1, edgeInfos.size());
    }

    @Ignore(
            "Problem about arrow 12.0.0 with S3, see https://github.com/apache/incubator-graphar/issues/187")
    public void testGraphInfoLoadFromS3() {
        // arrow::fs::Fi
        // nalizeS3 was not called even though S3 was initialized.  This could lead to a
        // segmentation
        // fault at exit
        String path =
                "s3://graphar/ldbc/ldbc.graph.yml"
                        + "?endpoint_override=graphscope.oss-cn-beijing.aliyuncs.com";
        Result<StdSharedPtr<GraphInfo>> graphInfoResult = GraphInfo.load(path);
        Assert.assertFalse(graphInfoResult.hasError());
        StdSharedPtr<GraphInfo> graphInfo = graphInfoResult.value();
        Assert.assertEquals("ldbc", graphInfo.get().getName().toJavaString());
        StdVector<StdSharedPtr<@CXXReference VertexInfo>> vertexInfos =
                graphInfo.get().getVertexInfos();
        StdVector<StdSharedPtr<@CXXReference EdgeInfo>> edgeInfos = graphInfo.get().getEdgeInfos();
        Assert.assertEquals(8, vertexInfos.size());
        Assert.assertEquals(23, edgeInfos.size());
    }
}
