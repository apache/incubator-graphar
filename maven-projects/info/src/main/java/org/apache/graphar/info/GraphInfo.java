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
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.graphar.info.yaml.GraphYaml;
import org.yaml.snakeyaml.Yaml;

public class GraphInfo {
    private final String name;
    private final List<VertexInfo> vertexInfos;
    private final List<EdgeInfo> edgeInfos;
    private final URI baseUri;
    private final Map<String, VertexInfo> vertexType2VertexInfo;
    private final Map<String, EdgeInfo> edgeConcat2EdgeInfo;
    private final VersionInfo version;

    public GraphInfo(
            String name,
            List<VertexInfo> vertexInfos,
            List<EdgeInfo> edgeInfos,
            String prefix,
            String version) {
        this(name, vertexInfos, edgeInfos, prefix == null ? null : URI.create(prefix), version);
    }

    public GraphInfo(
            String name,
            List<VertexInfo> vertexInfos,
            List<EdgeInfo> edgeInfos,
            URI baseUri,
            String version) {
        this.name = name;
        this.vertexInfos = List.copyOf(vertexInfos);
        this.edgeInfos = List.copyOf(edgeInfos);
        this.baseUri = baseUri;
        this.version = VersionParser.getVersion(version);
        this.vertexType2VertexInfo =
                vertexInfos.stream()
                        .collect(
                                Collectors.toUnmodifiableMap(
                                        VertexInfo::getType, Function.identity()));
        this.edgeConcat2EdgeInfo =
                edgeInfos.stream()
                        .collect(
                                Collectors.toUnmodifiableMap(
                                        EdgeInfo::getConcat, Function.identity()));
    }

    private GraphInfo(
            String name,
            List<VertexInfo> vertexInfos,
            List<EdgeInfo> edgeInfos,
            URI baseUri,
            String version,
            Map<String, VertexInfo> vertexType2VertexInfo,
            Map<String, EdgeInfo> edgeConcat2EdgeInfo) {
        this(
                name,
                vertexInfos,
                edgeInfos,
                baseUri,
                VersionParser.getVersion(version),
                vertexType2VertexInfo,
                edgeConcat2EdgeInfo);
    }

    private GraphInfo(
            String name,
            List<VertexInfo> vertexInfos,
            List<EdgeInfo> edgeInfos,
            URI baseUri,
            VersionInfo version,
            Map<String, VertexInfo> vertexType2VertexInfo,
            Map<String, EdgeInfo> edgeConcat2EdgeInfo) {
        this.name = name;
        this.vertexInfos = vertexInfos;
        this.edgeInfos = edgeInfos;
        this.baseUri = baseUri;
        this.version = version;
        this.vertexType2VertexInfo = vertexType2VertexInfo;
        this.edgeConcat2EdgeInfo = edgeConcat2EdgeInfo;
    }

    public String dump() {
        Yaml yaml = new Yaml(GraphYaml.getRepresenter(), GraphYaml.getDumperOptions());
        GraphYaml graphYaml = new GraphYaml(this);
        return yaml.dump(graphYaml);
    }

    public Optional<GraphInfo> addVertexAsNew(VertexInfo vertexInfo) {
        if (vertexInfo == null || hasVertexInfo(vertexInfo.getType())) {
            return Optional.empty();
        }
        List<VertexInfo> newVertexInfos =
                Stream.concat(vertexInfos.stream(), Stream.of(vertexInfo))
                        .collect(Collectors.toList());
        Map<String, VertexInfo> newVertexType2VertexInfo =
                Stream.concat(
                                vertexType2VertexInfo.entrySet().stream(),
                                Stream.of(Map.entry(vertexInfo.getType(), vertexInfo)))
                        .collect(
                                Collectors.toUnmodifiableMap(
                                        Map.Entry::getKey, Map.Entry::getValue));
        return Optional.of(
                new GraphInfo(
                        name,
                        newVertexInfos,
                        edgeInfos,
                        baseUri,
                        version,
                        newVertexType2VertexInfo,
                        edgeConcat2EdgeInfo));
    }

    public Optional<GraphInfo> removeVertex(VertexInfo vertexInfo) {

        if (vertexInfo == null || !hasVertexInfo(vertexInfo.getType())) {
            return Optional.empty();
        }

        final List<VertexInfo> newVertexInfoList =
                vertexInfos.stream()
                        .filter(v -> !v.getType().equals(vertexInfo.getType()))
                        .collect(Collectors.toList());

        final Map<String, VertexInfo> newVertexInfoMap =
                vertexType2VertexInfo.entrySet().stream()
                        .filter(v -> !v.getKey().equals(vertexInfo.getType()))
                        .collect(
                                Collectors.toUnmodifiableMap(
                                        Map.Entry::getKey, Map.Entry::getValue));
        return Optional.of(
                new GraphInfo(
                        name,
                        newVertexInfoList,
                        edgeInfos,
                        baseUri,
                        version,
                        newVertexInfoMap,
                        edgeConcat2EdgeInfo));
    }

    public Optional<GraphInfo> addEdgeAsNew(EdgeInfo edgeInfo) {
        if (edgeInfo == null
                || hasEdgeInfo(
                        edgeInfo.getSrcType(), edgeInfo.getEdgeType(), edgeInfo.getDstType())) {
            return Optional.empty();
        }
        List<EdgeInfo> newEdgeInfos =
                Stream.concat(edgeInfos.stream(), Stream.of(edgeInfo)).collect(Collectors.toList());
        Map<String, EdgeInfo> newEdgeConcat2EdgeInfo =
                Stream.concat(
                                edgeConcat2EdgeInfo.entrySet().stream(),
                                Stream.of(Map.entry(edgeInfo.getConcat(), edgeInfo)))
                        .collect(
                                Collectors.toUnmodifiableMap(
                                        Map.Entry::getKey, Map.Entry::getValue));
        return Optional.of(
                new GraphInfo(
                        name,
                        vertexInfos,
                        newEdgeInfos,
                        baseUri,
                        version,
                        vertexType2VertexInfo,
                        newEdgeConcat2EdgeInfo));
    }

    public Optional<GraphInfo> removeEdge(EdgeInfo edgeInfo) {
        if (edgeInfo == null
                || !hasEdgeInfo(
                        edgeInfo.getSrcType(), edgeInfo.getEdgeType(), edgeInfo.getDstType())) {
            return Optional.empty();
        }
        final List<EdgeInfo> newEdgeInfos =
                edgeInfos.stream()
                        .filter(
                                e ->
                                        !(e.getSrcType().equals(edgeInfo.getSrcType())
                                                && e.getDstType().equals(edgeInfo.getDstType())
                                                && e.getEdgeType().equals(edgeInfo.getEdgeType())))
                        .collect(Collectors.toList());

        final Map<String, EdgeInfo> newEdgeConcat2EdgeInfo =
                edgeConcat2EdgeInfo.entrySet().stream()
                        .filter(e -> !e.getKey().equals(edgeInfo.getConcat()))
                        .collect(
                                Collectors.toUnmodifiableMap(
                                        Map.Entry::getKey, Map.Entry::getValue));
        return Optional.of(
                new GraphInfo(
                        name,
                        vertexInfos,
                        newEdgeInfos,
                        baseUri,
                        version,
                        vertexType2VertexInfo,
                        newEdgeConcat2EdgeInfo));
    }

    public boolean hasVertexInfo(String type) {
        return vertexType2VertexInfo.containsKey(type);
    }

    public boolean hasEdgeInfo(String srcType, String edgeType, String dstType) {
        return edgeConcat2EdgeInfo.containsKey(EdgeInfo.concat(srcType, edgeType, dstType));
    }

    public VertexInfo getVertexInfo(String type) {
        checkVertexExist(type);
        return vertexType2VertexInfo.get(type);
    }

    public EdgeInfo getEdgeInfo(String srcType, String edgeType, String dstType) {
        checkEdgeExist(srcType, edgeType, dstType);
        return edgeConcat2EdgeInfo.get(EdgeInfo.concat(srcType, edgeType, dstType));
    }

    public int getVertexInfoNum() {
        return vertexInfos.size();
    }

    public int getEdgeInfoNum() {
        return edgeInfos.size();
    }

    public String getName() {
        return name;
    }

    public List<VertexInfo> getVertexInfos() {
        return vertexInfos;
    }

    public List<EdgeInfo> getEdgeInfos() {
        return edgeInfos;
    }

    public String getPrefix() {
        return baseUri == null ? null : baseUri.toString();
    }

    public URI getBaseUri() {
        return baseUri;
    }

    public VersionInfo getVersion() {
        return version;
    }

    public boolean isValidated() {
        // Check if name is not empty and base URI is not null
        if (name == null || name.isEmpty() || baseUri == null) {
            return false;
        }

        // Check if all vertex infos are valid
        for (VertexInfo vertexInfo : vertexInfos) {
            if (vertexInfo == null || !vertexInfo.isValidated()) {
                return false;
            }
        }

        // Check if all edge infos are valid
        for (EdgeInfo edgeInfo : edgeInfos) {
            if (edgeInfo == null || !edgeInfo.isValidated()) {
                return false;
            }
        }

        // Check if vertex/edge infos size matches vertex/edge type to index map size
        if (vertexInfos.size() != vertexType2VertexInfo.size()
                || edgeInfos.size() != edgeConcat2EdgeInfo.size()) {
            return false;
        }

        return true;
    }

    private void checkVertexExist(String type) {
        if (!hasVertexInfo(type)) {
            throw new IllegalArgumentException(
                    "Vertex type " + type + " not exist in graph " + getName());
        }
    }

    private void checkEdgeExist(String srcType, String dstType, String edgeType) {
        if (!hasEdgeInfo(srcType, dstType, edgeType)) {
            throw new IllegalArgumentException(
                    "Edge type "
                            + EdgeInfo.concat(srcType, dstType, edgeType)
                            + " not exist in graph "
                            + getName());
        }
    }
}
