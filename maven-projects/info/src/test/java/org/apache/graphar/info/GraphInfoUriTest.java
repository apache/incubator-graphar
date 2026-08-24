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
import org.apache.graphar.info.type.AdjListType;
import org.apache.graphar.info.type.DataType;
import org.apache.graphar.info.type.FileType;
import org.apache.graphar.info.yaml.VertexYaml;
import org.junit.Assert;
import org.junit.Test;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class GraphInfoUriTest {

    @Test
    public void testBaseGraphInfo() {
        Yaml vertexYamlLoader = new Yaml(new Constructor(VertexYaml.class, new LoaderOptions()));
        VertexYaml vertexYaml = vertexYamlLoader.load(TestUtil.getBaseGraphInfoYaml());
        VertexInfo vertexInfo = TestUtil.buildVertexInfoFromYaml(vertexYaml);
        Assert.assertEquals(URI.create("vertex/person/"), vertexInfo.getBaseUri());
        // absolute paths
        Assert.assertEquals(
                URI.create("vertex/person/id/"),
                vertexInfo.getPropertyGroupUri(vertexInfo.getPropertyGroups().get(0)));
        // relative paths
        Assert.assertEquals(
                URI.create("/tmp/vertex/person/firstName_lastName_gender/chunk0"),
                vertexInfo.getPropertyGroupChunkUri(vertexInfo.getPropertyGroups().get(1), 0));
    }

    @Test
    public void testS3GraphInfo() {
        Yaml vertexYamlLoader = new Yaml(new Constructor(VertexYaml.class, new LoaderOptions()));
        VertexYaml vertexYaml = vertexYamlLoader.load(TestUtil.getS3GraphInfoYaml());
        VertexInfo vertexInfo = TestUtil.buildVertexInfoFromYaml(vertexYaml);
        Assert.assertEquals(URI.create("s3://graphar/vertex/person/"), vertexInfo.getBaseUri());
        // absolute paths
        Assert.assertEquals(
                URI.create("s3://graphar/vertex/person/id/"),
                vertexInfo.getPropertyGroupUri(vertexInfo.getPropertyGroups().get(0)));
        // relative paths
        Assert.assertEquals(
                URI.create("s3://tmp/vertex/person/firstName_lastName_gender/chunk0"),
                vertexInfo.getPropertyGroupChunkUri(vertexInfo.getPropertyGroups().get(1), 0));
    }

    @Test
    public void testHdfsGraphInfo() {
        Yaml vertexYamlLoader = new Yaml(new Constructor(VertexYaml.class, new LoaderOptions()));
        VertexYaml vertexYaml = vertexYamlLoader.load(TestUtil.getHdfsGraphInfoYaml());
        VertexInfo vertexInfo = TestUtil.buildVertexInfoFromYaml(vertexYaml);
        Assert.assertEquals(URI.create("hdfs://graphar/vertex/person/"), vertexInfo.getBaseUri());
        // absolute paths
        Assert.assertEquals(
                URI.create("hdfs://graphar/vertex/person/id/"),
                vertexInfo.getPropertyGroupUri(vertexInfo.getPropertyGroups().get(0)));
        // relative paths
        Assert.assertEquals(
                URI.create("hdfs://tmp/vertex/person/firstName_lastName_gender/chunk0"),
                vertexInfo.getPropertyGroupChunkUri(vertexInfo.getPropertyGroups().get(1), 0));
    }

    @Test
    public void testFileGraphInfo() {
        Yaml vertexYamlLoader = new Yaml(new Constructor(VertexYaml.class, new LoaderOptions()));
        VertexYaml vertexYaml = vertexYamlLoader.load(TestUtil.getFileGraphInfoYaml());
        VertexInfo vertexInfo = TestUtil.buildVertexInfoFromYaml(vertexYaml);
        Assert.assertEquals(URI.create("file:///graphar/vertex/person/"), vertexInfo.getBaseUri());
        // absolute paths
        Assert.assertEquals(
                URI.create("file:///graphar/vertex/person/id/"),
                vertexInfo.getPropertyGroupUri(vertexInfo.getPropertyGroups().get(0)));
        // relative paths
        Assert.assertEquals(
                URI.create("file:///tmp/vertex/person/firstName_lastName_gender/chunk0"),
                vertexInfo.getPropertyGroupChunkUri(vertexInfo.getPropertyGroups().get(1), 0));
    }

    @Test
    public void testEdgePathsNormalizePrefixesWithoutTrailingSlashes() {
        PropertyGroup propertyGroup =
                new PropertyGroup(
                        List.of(new Property("created", DataType.STRING, false, false)),
                        FileType.PARQUET,
                        "created");
        EdgeInfo edgeInfo =
                new EdgeInfo(
                        "person",
                        "knows",
                        "person",
                        1024,
                        100,
                        100,
                        false,
                        "edge/person_knows_person",
                        "gar/v1",
                        List.of(
                                new AdjacentList(
                                        AdjListType.ordered_by_source,
                                        FileType.PARQUET,
                                        "ordered_by_source")),
                        List.of(propertyGroup));

        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/edge_count2"),
                edgeInfo.getEdgesNumFileUri(AdjListType.ordered_by_source, 2));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/adj_list/part2/chunk1"),
                edgeInfo.getAdjacentListChunkUri(AdjListType.ordered_by_source, 2, 1));
        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/created/part2/chunk1"),
                edgeInfo.getPropertyGroupChunkUri(
                        propertyGroup, AdjListType.ordered_by_source, 2, 1));
    }

    @Test
    public void testEdgePathsRejectMissingAdjacentListPrefix() {
        EdgeInfo edgeInfo = edgeInfoWithAdjacentListPrefix(null);

        IllegalArgumentException failure =
                Assert.assertThrows(
                        IllegalArgumentException.class,
                        () -> edgeInfo.getAdjacentListUri(AdjListType.ordered_by_source));
        Assert.assertEquals("childPath must not be null", failure.getMessage());
    }

    @Test
    public void testEdgePathsKeepEscapedPrefixesUnchanged() {
        EdgeInfo edgeInfo = edgeInfoWithAdjacentListPrefix("ordered%20by%20source");

        Assert.assertEquals(
                URI.create("edge/person_knows_person/ordered%20by%20source/adj_list/part0/chunk0"),
                edgeInfo.getAdjacentListChunkUri(AdjListType.ordered_by_source, 0, 0));
    }

    private static EdgeInfo edgeInfoWithAdjacentListPrefix(String prefix) {
        return new EdgeInfo(
                "person",
                "knows",
                "person",
                1024,
                100,
                100,
                false,
                "edge/person_knows_person",
                "gar/v1",
                List.of(new AdjacentList(AdjListType.ordered_by_source, FileType.PARQUET, prefix)),
                List.of(
                        new PropertyGroup(
                                List.of(new Property("created", DataType.STRING, false, false)),
                                FileType.PARQUET,
                                "created")));
    }
}
