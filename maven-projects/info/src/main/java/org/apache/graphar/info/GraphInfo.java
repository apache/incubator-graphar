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
import java.util.*;
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
    private final Map<String, URI> types2StoreUri;

    public GraphInfo(
            String name,
            Map<URI, VertexInfo> vertexInfos,
            Map<URI, EdgeInfo> edgeInfos,
            URI uri,
            String version) {
        this(
                name,
                new ArrayList<>(vertexInfos.values()),
                new ArrayList<>(edgeInfos.values()),
                uri,
                version);
        vertexInfos.forEach((key, value) -> types2StoreUri.put(value.getType() + ".vertex", key));
        edgeInfos.forEach((key, value) -> types2StoreUri.put(value.getConcat() + ".edge", key));
    }

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
        this.types2StoreUri = new HashMap<>();
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
        this.types2StoreUri = new HashMap<>();
    }

    public String dump(URI storeUri) {
        Yaml yaml = new Yaml(GraphYaml.getRepresenter(), GraphYaml.getDumperOptions());
        GraphYaml graphYaml = new GraphYaml(storeUri, this);
        return yaml.dump(graphYaml);
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

    public boolean hasVertexInfo(String type) {
        return vertexType2VertexInfo.containsKey(type);
    }

    public boolean hasEdgeInfo(String srcType, String edgeType, String dstType) {
        return edgeConcat2EdgeInfo.containsKey(EdgeInfo.concat(srcType, dstType, edgeType));
    }

    public VertexInfo getVertexInfo(String type) {
        checkVertexExist(type);
        return vertexType2VertexInfo.get(type);
    }

    public EdgeInfo getEdgeInfo(String srcType, String edgeType, String dstType) {
        checkEdgeExist(srcType, edgeType, dstType);
        return edgeConcat2EdgeInfo.get(EdgeInfo.concat(srcType, dstType, edgeType));
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

    public void setStoreUri(VertexInfo vertexInfo, URI storeUri) {
        this.types2StoreUri.put(vertexInfo.getType() + ".vertex", storeUri);
    }

    public void setStoreUri(EdgeInfo edgeInfo, URI storeUri) {
        this.types2StoreUri.put(edgeInfo.getConcat() + ".edge", storeUri);
    }

    public URI getStoreUri(VertexInfo vertexInfo) {
        String type = vertexInfo.getType() + ".vertex";
        if (types2StoreUri.containsKey(type)) {
            return types2StoreUri.get(type);
        }
        return URI.create(type + ".yaml");
    }

    public URI getStoreUri(EdgeInfo edgeInfo) {
        String type = edgeInfo.getConcat() + ".edge";
        if (types2StoreUri.containsKey(type)) {
            return types2StoreUri.get(type);
        }
        return URI.create(type + ".yaml");
    }

    public Map<String, URI> getTypes2Uri() {
        return types2StoreUri;
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
