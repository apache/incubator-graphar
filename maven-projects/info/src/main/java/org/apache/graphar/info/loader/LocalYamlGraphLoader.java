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

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.info.VertexInfo;
import org.apache.graphar.info.yaml.EdgeYaml;
import org.apache.graphar.info.yaml.GraphYaml;
import org.apache.graphar.info.yaml.VertexYaml;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class LocalYamlGraphLoader implements GraphLoader {
    public LocalYamlGraphLoader() {}

    private String ensureTrailingSlash(String pathStr) {
        if (pathStr == null) {
            return null; // Or handle as an error, or return "" or "/"
        }
        return pathStr.endsWith("/") ? pathStr : pathStr + "/";
    }

    @Override
    public GraphInfo load(String graphYamlPath) throws IOException {
        final Path path =
                FileSystems.getDefault()
                        .getPath(graphYamlPath)
                        .toAbsolutePath(); // Ensure path is absolute for reliable parent
        // load graph itself
        final BufferedReader reader = Files.newBufferedReader(path);
        final Yaml yamlLoader = new Yaml(new Constructor(GraphYaml.class, new LoaderOptions()));
        final GraphYaml graphYaml = yamlLoader.load(reader);
        reader.close();

        // Determine effective prefix for GraphInfo
        String yamlPrefix = graphYaml.getPrefix();
        Path baseDir = path.getParent();
        if (baseDir == null) { // Should not happen for a non-root file path
            baseDir = FileSystems.getDefault().getPath("");
        }
        String effectiveGraphPrefix;
        if (yamlPrefix == null || yamlPrefix.isEmpty()) {
            effectiveGraphPrefix = ensureTrailingSlash(baseDir.toString());
        } else {
            Path resolvedYamlPrefixPath = baseDir.resolve(yamlPrefix).normalize();
            effectiveGraphPrefix = ensureTrailingSlash(resolvedYamlPrefixPath.toString());
        }

        // load vertices - paths for vertex/edge YAMLs should be relative to the graph.yml's
        // directory (baseDir)
        // The ABSOLUTE_PREFIX used for loading vertex/edge YAMLs should be baseDir.
        String vertexEdgeYamlBaseDir = ensureTrailingSlash(baseDir.toString());
        List<VertexInfo> vertexInfos = new ArrayList<>(graphYaml.getVertices().size());
        for (String vertexYamlName : graphYaml.getVertices()) {
            vertexInfos.add(loadVertex(vertexEdgeYamlBaseDir + vertexYamlName));
        }
        // load edges
        List<EdgeInfo> edgeInfos = new ArrayList<>(graphYaml.getEdges().size());
        for (String edgeYamlName : graphYaml.getEdges()) {
            edgeInfos.add(loadEdge(vertexEdgeYamlBaseDir + edgeYamlName));
        }
        // Use effectiveGraphPrefix for GraphInfo constructor
        return new GraphInfo(graphYaml.getName(), vertexInfos, edgeInfos, effectiveGraphPrefix);
    }

    private VertexInfo loadVertex(String path) throws IOException {
        final Path vertexPath = FileSystems.getDefault().getPath(path);
        final BufferedReader reader = Files.newBufferedReader(vertexPath);
        Yaml vertexYamlLoader = new Yaml(new Constructor(VertexYaml.class, new LoaderOptions()));
        VertexYaml vertexYaml = vertexYamlLoader.load(reader);
        reader.close();
        return vertexYaml.toVertexInfo();
    }

    private EdgeInfo loadEdge(String path) throws IOException {
        final Path edgePath = FileSystems.getDefault().getPath(path);
        final BufferedReader reader = Files.newBufferedReader(edgePath);
        Yaml edgeYamlLoader = new Yaml(new Constructor(EdgeYaml.class, new LoaderOptions()));
        EdgeYaml edgeYaml = edgeYamlLoader.load(reader);
        reader.close();
        return edgeYaml.toEdgeInfo();
    }
}
