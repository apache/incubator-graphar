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
import org.apache.graphar.info.yaml.EdgeYaml;
import org.apache.graphar.info.yaml.GraphYaml;
import org.apache.graphar.proto.AdjListType;
import org.apache.graphar.proto.DataType;
import org.apache.graphar.util.GeneralParams;
import org.yaml.snakeyaml.Yaml;

public class EdgeInfo {
    private final org.apache.graphar.proto.EdgeInfo protoEdgeInfo;
    private final Map<AdjListType, AdjacentList> cachedAdjacentLists;
    private final PropertyGroups cachedPropertyGroups;

    public EdgeInfo(
            String srcLabel,
            String edgeLabel,
            String dstLabel,
            long chunkSize,
            long srcChunkSize,
            long dstChunkSize,
            boolean directed,
            String prefix,
            String version,
            List<AdjacentList> adjacentListsAsList,
            List<PropertyGroup> propertyGroupsAsList) {
        this.cachedAdjacentLists =
                adjacentListsAsList.stream()
                        .collect(
                                Collectors.toUnmodifiableMap(
                                        AdjacentList::getType, Function.identity()));
        this.cachedPropertyGroups = new PropertyGroups(propertyGroupsAsList);
        this.protoEdgeInfo =
                org.apache.graphar.proto.EdgeInfo.newBuilder()
                        .setSourceVertexType(srcLabel)
                        .setType(edgeLabel)
                        .setDestinationVertexChunkSize(dstChunkSize)
                        .setChunkSize(chunkSize)
                        .setSourceVertexChunkSize(srcChunkSize)
                        .setDestinationVertexType(dstLabel)
                        .setIsDirected(directed)
                        .setPrefix(prefix)
                        .setVersion(version)
                        .addAllAdjacentList(
                                adjacentListsAsList.stream()
                                        .map(AdjacentList::getProto)
                                        .collect(Collectors.toUnmodifiableList()))
                        .addAllProperties(
                                propertyGroupsAsList.stream()
                                        .map(PropertyGroup::getProto)
                                        .collect(Collectors.toUnmodifiableList()))
                        .build();
    }

    public EdgeInfo(org.apache.graphar.proto.EdgeInfo protoEdgeInfo) {
        this.protoEdgeInfo = protoEdgeInfo;
        this.cachedAdjacentLists =
                protoEdgeInfo.getAdjacentListList().stream()
                        .map(AdjacentList::ofProto)
                        .collect(
                                Collectors.toUnmodifiableMap(
                                        AdjacentList::getType, Function.identity()));
        this.cachedPropertyGroups = PropertyGroups.ofProto(protoEdgeInfo.getPropertiesList());
    }

    private EdgeInfo(
            org.apache.graphar.proto.EdgeInfo protoEdgeInfo,
            Map<AdjListType, AdjacentList> cachedAdjacentLists,
            PropertyGroups cachedPropertyGroups) {
        this.protoEdgeInfo = protoEdgeInfo;
        this.cachedAdjacentLists = cachedAdjacentLists;
        this.cachedPropertyGroups = cachedPropertyGroups;
    }

    org.apache.graphar.proto.EdgeInfo getProto() {
        return protoEdgeInfo;
    }

    public static String concat(String srcLabel, String edgeLabel, String dstLabel) {
        return srcLabel
                + GeneralParams.regularSeparator
                + edgeLabel
                + GeneralParams.regularSeparator
                + dstLabel;
    }

    public Optional<EdgeInfo> addAdjacentListAsNew(AdjacentList adjacentList) {
        if (adjacentList == null || cachedAdjacentLists.containsKey(adjacentList.getType())) {
            return Optional.empty();
        }
        Map<AdjListType, AdjacentList> newAdjacentLists =
                Stream.concat(
                                cachedAdjacentLists.entrySet().stream(),
                                Map.of(adjacentList.getType(), adjacentList).entrySet().stream())
                        .collect(
                                Collectors.toUnmodifiableMap(
                                        Map.Entry::getKey, Map.Entry::getValue));
        return Optional.of(new EdgeInfo(protoEdgeInfo, newAdjacentLists, cachedPropertyGroups));
    }

    public Optional<EdgeInfo> addPropertyGroupAsNew(PropertyGroup propertyGroup) {
        // do not need check property group exist, because PropertyGroups will check it
        return cachedPropertyGroups
                .addPropertyGroupAsNew(propertyGroup)
                .map(
                        newPropertyGroups ->
                                new EdgeInfo(
                                        protoEdgeInfo, cachedAdjacentLists, newPropertyGroups));
    }

    public boolean hasAdjListType(AdjListType adjListType) {
        return cachedAdjacentLists.containsKey(adjListType);
    }

    public boolean hasProperty(String propertyName) {
        return cachedPropertyGroups.hasProperty(propertyName);
    }

    public boolean hasPropertyGroup(PropertyGroup propertyGroup) {
        return cachedPropertyGroups.hasPropertyGroup(propertyGroup);
    }

    public AdjacentList getAdjacentList(AdjListType adjListType) {
        // AdjListType will be checked in this method,
        // other methods which get adjacent list in this class should call this method first,
        // so we don't check AdjListType in other methods.
        checkAdjListTypeExist(adjListType);
        return cachedAdjacentLists.get(adjListType);
    }

    public int getPropertyGroupNum() {
        return cachedPropertyGroups.getPropertyGroupNum();
    }

    public PropertyGroup getPropertyGroup(String property) {
        return cachedPropertyGroups.getPropertyGroup(property);
    }

    public String getPropertyGroupPrefix(PropertyGroup propertyGroup) {
        checkPropertyGroupExist(propertyGroup);
        return getPrefix() + propertyGroup.getPrefix();
    }

    public String getPropertyGroupChunkPath(PropertyGroup propertyGroup, long chunkIndex) {
        // PropertyGroup will be checked in getPropertyGroupPrefix
        return getPropertyGroupPrefix(propertyGroup) + "chunk" + chunkIndex;
    }

    public String getAdjacentListPrefix(AdjListType adjListType) {
        return getPrefix() + getAdjacentList(adjListType).getPrefix() + "adj_list/";
    }

    public String getAdjacentListChunkPath(AdjListType adjListType, long vertexChunkIndex) {
        return getAdjacentListPrefix(adjListType) + "chunk" + vertexChunkIndex;
    }

    public String getOffsetPrefix(AdjListType adjListType) {
        return getAdjacentListPrefix(adjListType) + "offset/";
    }

    public String getOffsetChunkPath(AdjListType adjListType, long vertexChunkIndex) {
        return getOffsetPrefix(adjListType) + "chunk" + vertexChunkIndex;
    }

    public String getVerticesNumFilePath(AdjListType adjListType) {
        return getAdjacentListPrefix(adjListType) + "vertex_count";
    }

    public String getEdgesNumFilePath(AdjListType adjListType, long vertexChunkIndex) {
        return getAdjacentListPrefix(adjListType) + "edge_count" + vertexChunkIndex;
    }

    public DataType getPropertyType(String propertyName) {
        return cachedPropertyGroups.getPropertyType(propertyName);
    }

    public boolean isPrimaryKey(String propertyName) {
        return cachedPropertyGroups.isPrimaryKey(propertyName);
    }

    public boolean isNullableKey(String propertyName) {
        return cachedPropertyGroups.isNullableKey(propertyName);
    }

    public String dump() {
        Yaml yaml = new Yaml(GraphYaml.getRepresenter(), GraphYaml.getDumperOptions());
        EdgeYaml edgeYaml = new EdgeYaml(this);
        return yaml.dump(edgeYaml);
    }

    public String getConcat() {
        return concat(getSrcLabel(), getEdgeLabel(), getDstLabel());
    }

    public String getSrcLabel() {
        return protoEdgeInfo.getSourceVertexType();
    }

    public String getEdgeLabel() {
        return protoEdgeInfo.getType();
    }

    public String getDstLabel() {
        return protoEdgeInfo.getDestinationVertexType();
    }

    public long getChunkSize() {
        return protoEdgeInfo.getChunkSize();
    }

    public long getSrcChunkSize() {
        return protoEdgeInfo.getSourceVertexChunkSize();
    }

    public long getDstChunkSize() {
        return protoEdgeInfo.getDestinationVertexChunkSize();
    }

    public boolean isDirected() {
        return protoEdgeInfo.getIsDirected();
    }

    public String getPrefix() {
        return protoEdgeInfo.getPrefix();
    }

    public String getEdgePath() {
        return getPrefix() + getConcat() + ".edge.yaml";
    }

    public VersionInfo getVersion() {
        return VersionParser.getVersion(protoEdgeInfo.getVersion());
    }

    public Map<AdjListType, AdjacentList> getAdjacentLists() {
        return cachedAdjacentLists;
    }

    public List<PropertyGroup> getPropertyGroups() {
        return cachedPropertyGroups.getPropertyGroupList();
    }

    private void checkAdjListTypeExist(AdjListType adjListType) {
        if (adjListType == null) {
            throw new IllegalArgumentException("The adjacency list type is null");
        }
        if (!cachedAdjacentLists.containsKey(adjListType)) {
            throw new IllegalArgumentException(
                    "The adjacency list type "
                            + adjListType
                            + " does not exist in the edge info "
                            + getConcat());
        }
    }

    private void checkPropertyGroupExist(PropertyGroup propertyGroup) {
        if (propertyGroup == null) {
            throw new IllegalArgumentException("Property group is null");
        }
        if (!hasPropertyGroup(propertyGroup)) {
            throw new IllegalArgumentException(
                    "Property group "
                            + propertyGroup
                            + " does not exist in the edge "
                            + getConcat());
        }
    }
}
