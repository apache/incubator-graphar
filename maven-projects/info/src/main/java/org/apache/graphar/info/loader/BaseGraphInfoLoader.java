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
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.info.VertexInfo;
import org.apache.graphar.info.yaml.AdjacentListYaml;
import org.apache.graphar.info.yaml.EdgeYaml;
import org.apache.graphar.info.yaml.GraphYaml;
import org.apache.graphar.info.yaml.PropertyGroupYaml;
import org.apache.graphar.info.yaml.VertexYaml;

public abstract class BaseGraphInfoLoader implements GraphInfoLoader {

    public abstract GraphInfo loadGraphInfo(URI graphYamlUri) throws IOException;

    public abstract VertexInfo loadVertexInfo(URI vertexYamlUri) throws IOException;

    public abstract EdgeInfo loadEdgeInfo(URI edgeYamlUri) throws IOException;

    public GraphInfo buildGraphInfoFromGraphYaml(URI baseUri, GraphYaml graphYaml)
            throws IOException {

        URI defaultBaseUri = baseUri;
        if (graphYaml.getPrefix() != null && !graphYaml.getPrefix().isEmpty()) {
            defaultBaseUri = baseUri.resolve(graphYaml.getPrefix());
        }

        // load vertices
        List<VertexInfo> vertexInfos = new ArrayList<>(graphYaml.getVertices().size());
        for (String vertexYamlPath : graphYaml.getVertices()) {
            URI vertexInfoUri = baseUri.resolve(vertexYamlPath);
            vertexInfos.add(loadVertexInfo(vertexInfoUri));
        }
        // load edges
        List<EdgeInfo> edgeInfos = new ArrayList<>(graphYaml.getEdges().size());
        for (String edgeYamlPath : graphYaml.getEdges()) {
            URI edgeInfoUri = baseUri.resolve(edgeYamlPath);
            edgeInfos.add(loadEdgeInfo(edgeInfoUri));
        }
        return new GraphInfo(
                graphYaml.getName(), vertexInfos, edgeInfos, defaultBaseUri, graphYaml.getVersion());
    }

    public VertexInfo buildVertexInfoFromGraphYaml(VertexYaml vertexYaml) {
        return new VertexInfo(
                vertexYaml.getType(),
                vertexYaml.getChunk_size(),
                vertexYaml.getProperty_groups().stream()
                        .map(PropertyGroupYaml::toPropertyGroup)
                        .collect(Collectors.toList()),
                vertexYaml.getPrefix(),
                vertexYaml.getVersion());
    }

    public EdgeInfo buildEdgeInfoFromGraphYaml(EdgeYaml edgeYaml) {
        return new EdgeInfo(
                edgeYaml.getSrc_type(),
                edgeYaml.getEdge_type(),
                edgeYaml.getDst_type(),
                edgeYaml.getChunk_size(),
                edgeYaml.getSrc_chunk_size(),
                edgeYaml.getDst_chunk_size(),
                edgeYaml.isDirected(),
                edgeYaml.getPrefix(),
                edgeYaml.getVersion(),
                edgeYaml.getAdj_lists().stream()
                        .map(AdjacentListYaml::toAdjacentList)
                        .collect(Collectors.toUnmodifiableList()),
                edgeYaml.getProperty_groups().stream()
                        .map(PropertyGroupYaml::toPropertyGroup)
                        .collect(Collectors.toList()));
    }
}
