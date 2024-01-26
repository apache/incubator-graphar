/*
 * Copyright 2022-2023 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphar.graphinfo;

import com.alibaba.graphar.stdcxx.StdMap;
import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.util.InfoVersion;
import com.alibaba.graphar.util.Result;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class GraphInfoTest {
    public static final String root = System.getenv("GAR_TEST_DATA") + "/root";

    @Test
    public void test1() {
        String graphName = "test_graph";
        String prefix = "test_prefix";
        InfoVersion version = InfoVersion.create(1);
        GraphInfo graphInfo = GraphInfo.create(graphName, version, prefix);
        Assert.assertEquals(graphName, graphInfo.getName().toJavaString());
        Assert.assertEquals(prefix, graphInfo.getPrefix().toJavaString());
        Assert.assertTrue(version.eq(graphInfo.getInfoVersion()));

        // test add vertex and get vertex info
        StdString vertexLabel = StdString.create("test_vertex");
        long vertexChunkSize = 100;
        StdString vertexPrefix = StdString.create("test_vertex_prefix");
        StdString vertexInfoPath = StdString.create("/tmp/test_vertex.vertex.yml");
        StdString unknownLabel = StdString.create("text_not_exist");
        VertexInfo vertexInfo =
                VertexInfo.factory.create(vertexLabel, vertexChunkSize, version, vertexPrefix);
        Assert.assertEquals(0, graphInfo.getVertexInfos().size());
        Assert.assertTrue(graphInfo.addVertex(vertexInfo).ok());
        graphInfo.addVertexInfoPath(vertexInfoPath);
        Assert.assertEquals(1, graphInfo.getVertexInfos().size());
        Result<VertexInfo> maybeVertexInfo = graphInfo.getVertexInfo(vertexLabel);
        Assert.assertFalse(maybeVertexInfo.hasError());
        Assert.assertTrue(vertexLabel.eq(maybeVertexInfo.value().getLabel()));
        Assert.assertTrue(vertexPrefix.eq(maybeVertexInfo.value().getPrefix()));
        Assert.assertTrue(graphInfo.getVertexInfo(unknownLabel).status().isKeyError());
        // existed vertex info can't be added again
        Assert.assertTrue(graphInfo.addVertex(vertexInfo).isInvalid());

        // test add edge and get edge info
        StdString srcLabel = StdString.create("test_vertex");
        StdString edgeLabel = StdString.create("test_edge");
        StdString dstLabel = StdString.create("test_vertex");
        long edgeChunkSize = 1024;
        StdString edgeInfoPath = StdString.create("/tmp/test_edge.edge.yml");
        EdgeInfo edgeInfo =
                EdgeInfo.factory.create(
                        srcLabel,
                        edgeLabel,
                        dstLabel,
                        edgeChunkSize,
                        vertexChunkSize,
                        vertexChunkSize,
                        true,
                        version);
        Assert.assertEquals(0, graphInfo.getEdgeInfos().size());
        Assert.assertTrue(graphInfo.addEdge(edgeInfo).ok());
        graphInfo.addEdgeInfoPath(edgeInfoPath);
        Assert.assertEquals(1, graphInfo.getEdgeInfos().size());
        Result<EdgeInfo> maybeEdgeInfo = graphInfo.getEdgeInfo(srcLabel, edgeLabel, dstLabel);
        Assert.assertFalse(maybeEdgeInfo.hasError());
        Assert.assertTrue(srcLabel.eq(maybeEdgeInfo.value().getSrcLabel()));
        Assert.assertTrue(edgeLabel.eq(maybeEdgeInfo.value().getEdgeLabel()));
        Assert.assertTrue(dstLabel.eq(maybeEdgeInfo.value().getDstLabel()));
        Assert.assertTrue(
                graphInfo
                        .getEdgeInfo(unknownLabel, unknownLabel, unknownLabel)
                        .status()
                        .isKeyError());
        // existed edge info can't be added again
        Assert.assertTrue(graphInfo.addEdge(edgeInfo).isInvalid());

        // test version
        Assert.assertTrue(version.eq(graphInfo.getInfoVersion()));
    }

    @Test
    public void testGraphInfoLoadFromFile() {
        String path = root + "/ldbc_sample/csv/ldbc_sample.graph.yml";
        Result<GraphInfo> graphInfoResult = GraphInfo.load(path);
        Assert.assertFalse(graphInfoResult.hasError());
        GraphInfo graphInfo = graphInfoResult.value();
        Assert.assertEquals("ldbc_sample", graphInfo.getName().toJavaString());
        Assert.assertEquals(root + "/ldbc_sample/csv/", graphInfo.getPrefix().toJavaString());
        StdMap<StdString, VertexInfo> vertexInfos = graphInfo.getVertexInfos();
        StdMap<StdString, EdgeInfo> edgeInfos = graphInfo.getEdgeInfos();
        Assert.assertEquals(1, vertexInfos.size());
        Assert.assertEquals(1, edgeInfos.size());
    }

    @Ignore("Problem about arrow 12.0.0 with S3, see https://github.com/alibaba/GraphAr/issues/187")
    public void testGraphInfoLoadFromS3() {
        // arrow::fs::Fi
        // nalizeS3 was not called even though S3 was initialized.  This could lead to a
        // segmentation
        // fault at exit
        String path =
                "s3://graphar/ldbc/ldbc.graph.yml"
                        + "?endpoint_override=graphscope.oss-cn-beijing.aliyuncs.com";
        Result<GraphInfo> graphInfoResult = GraphInfo.load(path);
        Assert.assertFalse(graphInfoResult.hasError());
        GraphInfo graphInfo = graphInfoResult.value();
        Assert.assertEquals("ldbc", graphInfo.getName().toJavaString());
        StdMap<StdString, VertexInfo> vertexInfos = graphInfo.getVertexInfos();
        StdMap<StdString, EdgeInfo> edgeInfos = graphInfo.getEdgeInfos();
        Assert.assertEquals(8, vertexInfos.size());
        Assert.assertEquals(23, edgeInfos.size());
    }
}
