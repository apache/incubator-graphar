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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.graphar.info.yaml.GraphYaml;
import org.apache.graphar.util.GeneralParams;
import org.yaml.snakeyaml.Yaml;

public class GraphInfo {
    private final org.apache.graphar.proto.GraphInfo protoGraphInfo;
    private final List<VertexInfo> cachedVertexInfoList;
    private final List<EdgeInfo> cachedEdgeInfoList;
    private final Map<String, VertexInfo> cachedVertexInfoMap;
    private final Map<String, EdgeInfo> cachedEdgeInfoMap;

    public GraphInfo(
            String name, List<VertexInfo> vertexInfos, List<EdgeInfo> edgeInfos, String prefix) {
        this.cachedVertexInfoList = List.copyOf(vertexInfos);
        this.cachedEdgeInfoList = List.copyOf(edgeInfos);
        this.cachedVertexInfoMap =
                vertexInfos.stream()
                        .collect(
                                Collectors.toUnmodifiableMap(
                                        VertexInfo::getType, Function.identity()));
        this.cachedEdgeInfoMap =
                edgeInfos.stream()
                        .collect(
                                Collectors.toUnmodifiableMap(
                                        EdgeInfo::getConcat, Function.identity()));
        this.protoGraphInfo =
                org.apache.graphar.proto.GraphInfo.newBuilder()
                        .setName(name)
                        .addAllVertices(
                                vertexInfos.stream()
                                        .map(VertexInfo::getVertexPath)
                                        .collect(Collectors.toList()))
                        .addAllEdges(
                                edgeInfos.stream()
                                        .map(EdgeInfo::getEdgePath)
                                        .collect(Collectors.toList()))
                        .setPrefix(prefix)
                        .build();
    }

    private GraphInfo(
            org.apache.graphar.proto.GraphInfo protoGraphInfo,
            List<VertexInfo> cachedVertexInfoList,
            List<EdgeInfo> cachedEdgeInfoList,
            Map<String, VertexInfo> cachedVertexInfoMap,
            Map<String, EdgeInfo> cachedEdgeInfoMap) {

        this.protoGraphInfo = protoGraphInfo;
        this.cachedVertexInfoList = cachedVertexInfoList;
        this.cachedEdgeInfoList = cachedEdgeInfoList;
        this.cachedVertexInfoMap = cachedVertexInfoMap;
        this.cachedEdgeInfoMap = cachedEdgeInfoMap;
    }

    public String dump() {
        Yaml yaml = new Yaml(GraphYaml.getDumperOptions());
        GraphYaml graphYaml = new GraphYaml(this);
        return yaml.dump(graphYaml);
    }

    public Optional<GraphInfo> addVertexAsNew(VertexInfo vertexInfo) {
        if (vertexInfo == null || hasVertexInfo(vertexInfo.getType())) {
            return Optional.empty();
        }
        final org.apache.graphar.proto.GraphInfo newProtoGraphInfo =
                org.apache.graphar.proto.GraphInfo.newBuilder(protoGraphInfo)
                        .addVertices(vertexInfo.getVertexPath())
                        .build();
        final List<VertexInfo> newVertexInfoList =
                Stream.concat(cachedVertexInfoList.stream(), Stream.of(vertexInfo))
                        .collect(Collectors.toList());
        final Map<String, VertexInfo> newVertexInfoMap =
                Stream.concat(
                        cachedVertexInfoMap.entrySet().stream(),
                        Stream.of(Map.entry(vertexInfo.getType(), vertexInfo)))
                        .collect(
                                Collectors.toUnmodifiableMap(
                                        Map.Entry::getKey, Map.Entry::getValue));
        return Optional.of(
                new GraphInfo(
                        newProtoGraphInfo,
                        newVertexInfoList,
                        cachedEdgeInfoList,
                        newVertexInfoMap,
                        cachedEdgeInfoMap));
    }

    public Optional<GraphInfo> removeEdgeAsNew(String srcLabel, String edgeLabel, String dstLabel) {
        if (edgeInfo == null
                || hasEdgeInfo(
                srcLabel, edgeLabel, dstLabel)) {
            return Optional.empty();
        }
        String edgePath = getPrefix() + "/" + EdgeInfo.concat(srcLabel, edgeLabel, dstLabel) + ".edge.yaml";
        final org.apache.graphar.proto.GraphInfo newProtoGraphInfo =
                org.apache.graphar.proto.GraphInfo.newBuilder(protoGraphInfo)
                        .removeEdges(edgePath)
                        .build();
        final List<EdgeInfo> newEdgeInfos = Stream.concat(cachedEdgeInfoList.stream().filter(e ->
                !e.getSrcLabel().equals(srcLabel)
                        && !e.getDstLabel().equals(dstLabel)
                        && !e.getEdgeLabel().equals(edgeLabel)))
                .collect(Collectors.toList());


        List<VertexInfo> newCachedVertexInfoList = newEdgeInfos.stream()
                // flatMap is used because for each edge, we might generate a stream of multiple vertices
                .flatMap(e -> {
                    Stream<VertexInfo> vertexStream = Stream.empty(); // Start with an empty stream
                    if (e.getSrcLabel().equals("")) {
                        // If srcLabel is empty, filter vertices where dstLabel doesn't match
                        vertexStream = Stream.concat(vertexStream,
                                cachedVertexInfoList.stream()
                                        .filter(vertex -> !e.getDstLabel().equals(vertex.getType())));
                    }
                    if (e.getDstLabel().equals("")) {
                        // If dstLabel is empty, filter vertices where srcLabel doesn't match
                        vertexStream = Stream.concat(vertexStream,
                                cachedVertexInfoList.stream()
                                        .filter(vertex -> !e.getSrcLabel().equals(vertex.getType())));
                    }
                    return vertexStream; // Return the combined stream of vertices for this edge
                })
                .distinct()
                .collect(Collectors.toList());

        final Map<String, EdgeInfo> newEdgeConcat2EdgeInfo =
                cachedEdgeInfoMap.entrySet().stream()
                        .filter(e -> !e.getConcat().equals(edgeInfo.getConcat()))
                        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
        // TODO if logic is correct create newCachedVertexInfoMap as well
        return Optional.of(
                new GraphInfo(
                        newProtoGraphInfo,
                        newCachedVertexInfoList,
                        newEdgeInfos,
                        cachedVertexInfoMap,
                        newEdgeConcat2EdgeInfo));
    }

    public Optional<GraphInfo> addEdgeAsNew(EdgeInfo edgeInfo) {
        if (edgeInfo == null
                || hasEdgeInfo(
                edgeInfo.getSrcLabel(), edgeInfo.getEdgeLabel(), edgeInfo.getDstLabel())) {
            return Optional.empty();
        }
        final org.apache.graphar.proto.GraphInfo newProtoGraphInfo =
                org.apache.graphar.proto.GraphInfo.newBuilder(protoGraphInfo)
                        .addEdges(edgeInfo.getEdgePath())
                        .build();
        final List<EdgeInfo> newEdgeInfos =
                Stream.concat(cachedEdgeInfoList.stream(), Stream.of(edgeInfo))
                        .collect(Collectors.toList());
        final Map<String, EdgeInfo> newEdgeConcat2EdgeInfo =
                Stream.concat(
                        cachedEdgeInfoMap.entrySet().stream(),
                        Stream.of(Map.entry(edgeInfo.getConcat(), edgeInfo)))
                        .collect(
                                Collectors.toUnmodifiableMap(
                                        Map.Entry::getKey, Map.Entry::getValue));
        return Optional.of(
                new GraphInfo(
                        newProtoGraphInfo,
                        cachedVertexInfoList,
                        newEdgeInfos,
                        cachedVertexInfoMap,
                        newEdgeConcat2EdgeInfo));
    }

    public boolean hasVertexInfo(String label) {
        return cachedVertexInfoMap.containsKey(label);
    }

    public boolean hasEdgeInfo(String srcLabel, String edgeLabel, String dstLabel) {
        return cachedEdgeInfoMap.containsKey(EdgeInfo.concat(srcLabel, edgeLabel, dstLabel));
    }

    public VertexInfo getVertexInfo(String label) {
        checkVertexExist(label);
        return cachedVertexInfoMap.get(label);
    }

    public EdgeInfo getEdgeInfo(String srcLabel, String edgeLabel, String dstLabel) {
        checkEdgeExist(srcLabel, edgeLabel, dstLabel);
        return cachedEdgeInfoMap.get(EdgeInfo.concat(srcLabel, edgeLabel, dstLabel));
    }

    public int getVertexInfoNum() {
        return cachedVertexInfoList.size();
    }

    public int getEdgeInfoNum() {
        return cachedEdgeInfoList.size();
    }

    public String getName() {
        return protoGraphInfo.getName();
    }

    public List<VertexInfo> getVertexInfos() {
        return cachedVertexInfoList;
    }

    public List<EdgeInfo> getEdgeInfos() {
        return cachedEdgeInfoList;
    }

    public String getPrefix() {
        return protoGraphInfo.getPrefix();
    }
    

    private void checkVertexExist(String label) {
        if (!hasVertexInfo(label)) {
            throw new IllegalArgumentException(
                    "Vertex label " + label + " not exist in graph " + getName());
        }
    }

    private void checkEdgeExist(String srcLabel, String dstLabel, String edgeLabel) {
        if (!hasEdgeInfo(srcLabel, dstLabel, edgeLabel)) {
            throw new IllegalArgumentException(
                    "Edge label"
                            + srcLabel
                            + GeneralParams.regularSeparator
                            + GeneralParams.regularSeparator
                            + edgeLabel
                            + GeneralParams.regularSeparator
                            + dstLabel
                            + " not exist in graph "
                            + getName());
        }
    }
}
