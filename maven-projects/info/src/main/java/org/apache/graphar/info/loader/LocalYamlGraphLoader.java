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

package org.apache.graphar.info.loader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.info.VertexInfo;
import org.apache.graphar.info.yaml.EdgeYaml;
import org.apache.graphar.info.yaml.GraphYaml;
import org.apache.graphar.info.yaml.VertexYaml;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class LocalYamlGraphLoader implements GraphLoader {
    private static FileSystem fileSystem = null;

    public LocalYamlGraphLoader() {}

    @Override
    public GraphInfo load(String graphYamlPath) throws IOException {
        if (fileSystem == null) {
            fileSystem = FileSystem.get(new Configuration());
        }
        // load graph itself
        final Path path = new Path(graphYamlPath);
        final FSDataInputStream inputStream = fileSystem.open(path);
        final Yaml yamlLoader = new Yaml(new Constructor(GraphYaml.class, new LoaderOptions()));
        final GraphYaml graphYaml = yamlLoader.load(inputStream);
        // load vertices
        final String ABSOLUTE_PREFIX = path.getParent().toString();
        List<VertexInfo> vertexInfos = new ArrayList<>(graphYaml.getVertices().size());
        for (String vertexYamlName : graphYaml.getVertices()) {
            vertexInfos.add(loadVertex(ABSOLUTE_PREFIX + "/" + vertexYamlName));
        }
        // load edges
        List<EdgeInfo> edgeInfos = new ArrayList<>(graphYaml.getEdges().size());
        for (String edgeYamlName : graphYaml.getEdges()) {
            edgeInfos.add(loadEdge(ABSOLUTE_PREFIX + "/" + edgeYamlName));
        }
        return new GraphInfo(graphYaml.getName(), vertexInfos, edgeInfos, graphYaml.getPrefix());
    }

    private VertexInfo loadVertex(String path) throws IOException {
        FSDataInputStream inputStream = fileSystem.open(new Path(path));
        Yaml vertexYamlLoader = new Yaml(new Constructor(VertexYaml.class, new LoaderOptions()));
        VertexYaml vertexYaml = vertexYamlLoader.load(inputStream);
        return vertexYaml.toVertexInfo();
    }

    private EdgeInfo loadEdge(String path) throws IOException {
        FSDataInputStream inputStream = fileSystem.open(new Path(path));
        Yaml edgeYamlLoader = new Yaml(new Constructor(EdgeYaml.class, new LoaderOptions()));
        EdgeYaml edgeYaml = edgeYamlLoader.load(inputStream);
        return edgeYaml.toEdgeInfo();
    }
}
