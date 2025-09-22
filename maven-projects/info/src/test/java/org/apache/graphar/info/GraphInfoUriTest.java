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
import org.apache.graphar.info.loader.BaseGraphInfoLoader;
import org.apache.graphar.info.loader.impl.LocalFileSystemStringGraphInfoLoader;
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
        BaseGraphInfoLoader baseGraphInfoLoader = new LocalFileSystemStringGraphInfoLoader();
        VertexInfo vertexInfo = baseGraphInfoLoader.buildVertexInfoFromGraphYaml(vertexYaml);
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
        BaseGraphInfoLoader baseGraphInfoLoader = new LocalFileSystemStringGraphInfoLoader();
        VertexInfo vertexInfo = baseGraphInfoLoader.buildVertexInfoFromGraphYaml(vertexYaml);
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
        BaseGraphInfoLoader baseGraphInfoLoader = new LocalFileSystemStringGraphInfoLoader();
        VertexInfo vertexInfo = baseGraphInfoLoader.buildVertexInfoFromGraphYaml(vertexYaml);
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
        BaseGraphInfoLoader baseGraphInfoLoader = new LocalFileSystemStringGraphInfoLoader();
        VertexInfo vertexInfo = baseGraphInfoLoader.buildVertexInfoFromGraphYaml(vertexYaml);
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
}
