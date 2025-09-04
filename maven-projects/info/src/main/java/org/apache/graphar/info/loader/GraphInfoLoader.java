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

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.info.VertexInfo;
import org.apache.graphar.info.yaml.GraphYaml;
import org.apache.graphar.util.PathUtil;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class GraphInfoLoader {

    public static GraphInfo load(String graphYamlPath) throws IOException {
        return load(graphYamlPath, (path -> Files.readString(Path.of(path))));
    }

    public static GraphInfo load(String graphYamlPath, YamlReader yamlReader) throws IOException {
        final Path path = FileSystems.getDefault().getPath(graphYamlPath);
        // load graph itself
        String yaml = yamlReader.readYaml(graphYamlPath);
        DataInputStream dataInputStream;
        Yaml GraphYamlLoader = new Yaml(new Constructor(GraphYaml.class, new LoaderOptions()));
        GraphYaml graphYaml = GraphYamlLoader.load(yaml);
        //init prefix default value
        String prefix = PathUtil.pathToDirectory(graphYamlPath);
        if (graphYaml.getPrefix() != null && !graphYaml.getPrefix().isEmpty()) {
            prefix = graphYaml.getPrefix();
        }

        // load vertices
        List<VertexInfo> vertexInfos = new ArrayList<>(graphYaml.getVertices().size());
        for (String vertexYamlPath : graphYaml.getVertices()) {
            String vertexInfoPath = PathUtil.resolvePath(graphYamlPath, vertexYamlPath);
            vertexInfos.add(VertexInfoLoader.load(vertexInfoPath, yamlReader));
        }
        // load edges
        List<EdgeInfo> edgeInfos = new ArrayList<>(graphYaml.getEdges().size());
        for (String edgeYamlPath : graphYaml.getEdges()) {
            String EdgeInfoPath = PathUtil.resolvePath(graphYamlPath, edgeYamlPath);
            edgeInfos.add(EdgeInfoLoader.load(EdgeInfoPath, yamlReader));
        }
        return new GraphInfo(
                graphYaml.getName(), vertexInfos, edgeInfos, prefix, graphYaml.getVersion());
    }
}
