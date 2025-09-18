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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.graphar.info.type.AdjListType;
import org.apache.graphar.info.type.DataType;
import org.apache.graphar.info.yaml.EdgeYaml;
import org.apache.graphar.info.yaml.GraphYaml;
import org.apache.graphar.util.GeneralParams;
import org.yaml.snakeyaml.Yaml;

public class EdgeInfo {
    private final EdgeTriplet edgeTriplet;
    private final long chunkSize;
    private final long srcChunkSize;
    private final long dstChunkSize;
    private final boolean directed;
    private final URI baseUri;
    private final Map<AdjListType, AdjacentList> adjacentLists;
    private final PropertyGroups propertyGroups;
    private final VersionInfo version;

    public static EdgeInfoBuilder builder() {
        return new EdgeInfoBuilder();
    }

    public static final class EdgeInfoBuilder {
        private EdgeTriplet edgeTriplet;
        private long chunkSize;
        private long srcChunkSize;
        private long dstChunkSize;
        private boolean directed;
        private URI baseUri;
        private String prefix;
        private Map<AdjListType, AdjacentList> adjacentLists;
        private PropertyGroups propertyGroups;
        private VersionInfo version;

        private List<AdjacentList> adjacentListsAsListTemp;
        private List<PropertyGroup> propertyGroupsAsListTemp;

        private String srcType;
        private String edgeType;
        private String dstType;

        private EdgeInfoBuilder() {}

        public EdgeInfoBuilder srcType(String srcType) {
            this.srcType = srcType;
            return this;
        }

        public EdgeInfoBuilder edgeType(String edgeType) {
            this.edgeType = edgeType;
            return this;
        }

        public EdgeInfoBuilder dstType(String dstType) {
            this.dstType = dstType;
            return this;
        }

        public EdgeInfoBuilder edgeTriplet(String srcType, String edgeType, String dstType) {
            this.edgeTriplet = new EdgeTriplet(srcType, edgeType, dstType);
            return this;
        }

        public EdgeInfoBuilder edgeTriplet(EdgeTriplet edgeTriplet) {
            this.edgeTriplet = edgeTriplet;
            return this;
        }

        public EdgeInfoBuilder chunkSize(long chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }

        public EdgeInfoBuilder srcChunkSize(long srcChunkSize) {
            this.srcChunkSize = srcChunkSize;
            return this;
        }

        public EdgeInfoBuilder dstChunkSize(long dstChunkSize) {
            this.dstChunkSize = dstChunkSize;
            return this;
        }

        public EdgeInfoBuilder directed(boolean directed) {
            this.directed = directed;
            return this;
        }

        public EdgeInfoBuilder baseUri(URI baseUri) {
            this.baseUri = baseUri;
            return this;
        }

        public EdgeInfoBuilder prefix(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public EdgeInfoBuilder addAdjacentList(AdjacentList adjacentList) {
            if (adjacentListsAsListTemp == null) {
                adjacentListsAsListTemp = new ArrayList<>();
            }
            adjacentListsAsListTemp.add(adjacentList);
            return this;
        }

        public EdgeInfoBuilder adjacentLists(List<AdjacentList> adjacentListsAsList) {
            if (adjacentListsAsListTemp == null) {
                adjacentListsAsListTemp = new ArrayList<>();
            }
            this.adjacentListsAsListTemp.addAll(adjacentListsAsList);
            return this;
        }

        public EdgeInfoBuilder adjacentLists(Map<AdjListType, AdjacentList> adjacentLists) {
            this.adjacentLists = adjacentLists;
            return this;
        }

        public EdgeInfoBuilder addPropertyGroup(PropertyGroup propertyGroup) {
            if (propertyGroupsAsListTemp == null) propertyGroupsAsListTemp = new ArrayList<>();
            propertyGroupsAsListTemp.add(propertyGroup);
            return this;
        }

        public EdgeInfoBuilder addPropertyGroups(List<PropertyGroup> propertyGroups) {
            if (propertyGroupsAsListTemp == null) propertyGroupsAsListTemp = new ArrayList<>();
            propertyGroupsAsListTemp.addAll(propertyGroups);
            return this;
        }

        public EdgeInfoBuilder propertyGroups(PropertyGroups propertyGroups) {
            this.propertyGroups = propertyGroups;
            return this;
        }

        public EdgeInfoBuilder version(String version) {
            this.version = VersionParser.getVersion(version);
            return this;
        }

        public EdgeInfoBuilder version(VersionInfo version) {
            this.version = version;
            return this;
        }

        public EdgeInfo build() {
            if (adjacentLists == null) {
                adjacentLists = new HashMap<>();
            }

            if (adjacentListsAsListTemp != null) {
                adjacentLists.putAll(
                        adjacentListsAsListTemp.stream()
                                .collect(
                                        Collectors.toUnmodifiableMap(
                                                AdjacentList::getType, Function.identity())));
            }

            if (propertyGroups == null && propertyGroupsAsListTemp != null) {
                propertyGroups = new PropertyGroups(propertyGroupsAsListTemp);
            } else if (propertyGroupsAsListTemp != null) {
                propertyGroups =
                        propertyGroupsAsListTemp.stream()
                                .map(propertyGroups::addPropertyGroupAsNew)
                                .filter(Optional::isPresent)
                                .map(Optional::get)
                                .reduce((first, second) -> second)
                                .orElse(new PropertyGroups(new ArrayList<>()));
            }

            if (edgeTriplet == null && srcType != null && edgeType != null && dstType != null) {
                edgeTriplet = new EdgeTriplet(srcType, edgeType, dstType);
            }

            if (edgeTriplet == null) {
                throw new IllegalArgumentException("Edge triplet is null");
            }

            if (propertyGroups == null) {
                throw new IllegalArgumentException("PropertyGroups is empty");
            }

            if (adjacentLists.isEmpty()) {
                throw new IllegalArgumentException("AdjacentLists is empty");
            }

            if (baseUri == null && prefix == null) {
                throw new IllegalArgumentException("baseUri and prefix cannot be both null");
            }

            if (baseUri != null && prefix != null && !URI.create(prefix).equals(baseUri)) {
                throw new IllegalArgumentException(
                        "baseUri and prefix conflict: baseUri="
                                + baseUri.toString()
                                + " prefix="
                                + prefix);
            }

            if (baseUri == null) {
                baseUri = URI.create(prefix);
            }

            return new EdgeInfo(this);
        }
    }

    private EdgeInfo(EdgeInfoBuilder builder) {
        this.edgeTriplet = builder.edgeTriplet;
        this.chunkSize = builder.chunkSize;
        this.srcChunkSize = builder.srcChunkSize;
        this.dstChunkSize = builder.dstChunkSize;
        this.directed = builder.directed;
        this.baseUri = builder.baseUri;
        this.adjacentLists = builder.adjacentLists;
        this.propertyGroups = builder.propertyGroups;
        this.version = builder.version;
    }

    public EdgeInfo(
            String srcType,
            String edgeType,
            String dstType,
            long chunkSize,
            long srcChunkSize,
            long dstChunkSize,
            boolean directed,
            URI baseUri,
            String version,
            List<AdjacentList> adjacentListsAsList,
            List<PropertyGroup> propertyGroupsAsList) {
        this(
                srcType,
                edgeType,
                dstType,
                chunkSize,
                srcChunkSize,
                dstChunkSize,
                directed,
                baseUri,
                VersionParser.getVersion(version),
                adjacentListsAsList,
                propertyGroupsAsList);
    }

    public EdgeInfo(
            String srcType,
            String edgeType,
            String dstType,
            long chunkSize,
            long srcChunkSize,
            long dstChunkSize,
            boolean directed,
            String prefix,
            String version,
            List<AdjacentList> adjacentListsAsList,
            List<PropertyGroup> propertyGroupsAsList) {
        this(
                srcType,
                edgeType,
                dstType,
                chunkSize,
                srcChunkSize,
                dstChunkSize,
                directed,
                URI.create(prefix),
                version,
                adjacentListsAsList,
                propertyGroupsAsList);
    }

    public EdgeInfo(
            String srcType,
            String edgeType,
            String dstType,
            long chunkSize,
            long srcChunkSize,
            long dstChunkSize,
            boolean directed,
            URI baseUri,
            VersionInfo version,
            List<AdjacentList> adjacentListsAsList,
            List<PropertyGroup> propertyGroupsAsList) {
        this.edgeTriplet = new EdgeTriplet(srcType, edgeType, dstType);
        this.chunkSize = chunkSize;
        this.srcChunkSize = srcChunkSize;
        this.dstChunkSize = dstChunkSize;
        this.directed = directed;
        this.baseUri = baseUri;
        this.adjacentLists =
                adjacentListsAsList.stream()
                        .collect(
                                Collectors.toUnmodifiableMap(
                                        AdjacentList::getType, Function.identity()));
        this.propertyGroups = new PropertyGroups(propertyGroupsAsList);
        this.version = version;
    }

    private EdgeInfo(
            EdgeTriplet edgeTriplet,
            long chunkSize,
            long srcChunkSize,
            long dstChunkSize,
            boolean directed,
            URI baseUri,
            String version,
            Map<AdjListType, AdjacentList> adjacentLists,
            PropertyGroups propertyGroups) {
        this(
                edgeTriplet,
                chunkSize,
                srcChunkSize,
                dstChunkSize,
                directed,
                baseUri,
                VersionParser.getVersion(version),
                adjacentLists,
                propertyGroups);
    }

    private EdgeInfo(
            EdgeTriplet edgeTriplet,
            long chunkSize,
            long srcChunkSize,
            long dstChunkSize,
            boolean directed,
            URI baseUri,
            VersionInfo version,
            Map<AdjListType, AdjacentList> adjacentLists,
            PropertyGroups propertyGroups) {
        this.edgeTriplet = edgeTriplet;
        this.chunkSize = chunkSize;
        this.srcChunkSize = srcChunkSize;
        this.dstChunkSize = dstChunkSize;
        this.directed = directed;
        this.baseUri = baseUri;
        this.adjacentLists = adjacentLists;
        this.propertyGroups = propertyGroups;
        this.version = version;
    }

    public static String concat(String srcLabel, String edgeLabel, String dstLabel) {
        return srcLabel
                + GeneralParams.regularSeparator
                + edgeLabel
                + GeneralParams.regularSeparator
                + dstLabel;
    }

    public Optional<EdgeInfo> addAdjacentListAsNew(AdjacentList adjacentList) {
        if (adjacentList == null || adjacentLists.containsKey(adjacentList.getType())) {
            return Optional.empty();
        }
        Map<AdjListType, AdjacentList> newAdjacentLists =
                Stream.concat(
                                adjacentLists.entrySet().stream(),
                                Map.of(adjacentList.getType(), adjacentList).entrySet().stream())
                        .collect(
                                Collectors.toUnmodifiableMap(
                                        Map.Entry::getKey, Map.Entry::getValue));
        return Optional.of(
                new EdgeInfo(
                        edgeTriplet,
                        chunkSize,
                        srcChunkSize,
                        dstChunkSize,
                        directed,
                        baseUri,
                        version,
                        newAdjacentLists,
                        propertyGroups));
    }

    public Optional<EdgeInfo> addPropertyGroupAsNew(PropertyGroup propertyGroup) {
        return propertyGroups
                .addPropertyGroupAsNew(propertyGroup)
                .map(
                        newPropertyGroups ->
                                new EdgeInfo(
                                        edgeTriplet,
                                        chunkSize,
                                        srcChunkSize,
                                        dstChunkSize,
                                        directed,
                                        baseUri,
                                        version,
                                        adjacentLists,
                                        newPropertyGroups));
    }

    public boolean hasAdjListType(AdjListType adjListType) {
        return adjacentLists.containsKey(adjListType);
    }

    public boolean hasProperty(String propertyName) {
        return propertyGroups.hasProperty(propertyName);
    }

    public boolean hasPropertyGroup(PropertyGroup propertyGroup) {
        return propertyGroups.hasPropertyGroup(propertyGroup);
    }

    public AdjacentList getAdjacentList(AdjListType adjListType) {
        // AdjListType will be checked in this method,
        // other methods which get adjacent list in this class should call this method first,
        // so we don't check AdjListType in other methods.
        checkAdjListTypeExist(adjListType);
        return adjacentLists.get(adjListType);
    }

    public int getPropertyGroupNum() {
        return propertyGroups.getPropertyGroupNum();
    }

    public PropertyGroup getPropertyGroup(String property) {
        return propertyGroups.getPropertyGroup(property);
    }

    public URI getPropertyGroupUri(PropertyGroup propertyGroup) {
        checkPropertyGroupExist(propertyGroup);
        return getBaseUri().resolve(propertyGroup.getBaseUri());
    }

    public URI getPropertyGroupChunkUri(PropertyGroup propertyGroup, long chunkIndex) {
        // PropertyGroup will be checked in getPropertyGroupPrefix
        return getPropertyGroupUri(propertyGroup).resolve("chunk" + chunkIndex);
    }

    public URI getAdjacentListUri(AdjListType adjListType) {
        return getBaseUri().resolve(getAdjacentList(adjListType).getBaseUri()).resolve("adj_list/");
    }

    public URI getAdjacentListChunkUri(AdjListType adjListType, long vertexChunkIndex) {
        return getAdjacentListUri(adjListType).resolve("chunk" + vertexChunkIndex);
    }

    public URI getOffsetUri(AdjListType adjListType) {
        return getAdjacentListUri(adjListType).resolve("offset/");
    }

    public URI getOffsetChunkUri(AdjListType adjListType, long vertexChunkIndex) {
        return getOffsetUri(adjListType).resolve("chunk" + vertexChunkIndex);
    }

    public URI getVerticesNumFileUri(AdjListType adjListType) {
        return getAdjacentListUri(adjListType).resolve("vertex_count");
    }

    public URI getEdgesNumFileUri(AdjListType adjListType, long vertexChunkIndex) {
        return getAdjacentListUri(adjListType).resolve("edge_count" + vertexChunkIndex);
    }

    public DataType getPropertyType(String propertyName) {
        return propertyGroups.getPropertyType(propertyName);
    }

    public boolean isPrimaryKey(String propertyName) {
        return propertyGroups.isPrimaryKey(propertyName);
    }

    public boolean isNullableKey(String propertyName) {
        return propertyGroups.isNullableKey(propertyName);
    }

    public String dump() {
        Yaml yaml = new Yaml(GraphYaml.getRepresenter(), GraphYaml.getDumperOptions());
        EdgeYaml edgeYaml = new EdgeYaml(this);
        return yaml.dump(edgeYaml);
    }

    public String getConcat() {
        return edgeTriplet.getConcat();
    }

    public String getSrcType() {
        return edgeTriplet.getSrcType();
    }

    public String getEdgeType() {
        return edgeTriplet.getEdgeType();
    }

    public String getDstType() {
        return edgeTriplet.getDstType();
    }

    public long getChunkSize() {
        return chunkSize;
    }

    public long getSrcChunkSize() {
        return srcChunkSize;
    }

    public long getDstChunkSize() {
        return dstChunkSize;
    }

    public boolean isDirected() {
        return directed;
    }

    public String getPrefix() {
        return baseUri.toString();
    }

    public URI getBaseUri() {
        return baseUri;
    }

    public VersionInfo getVersion() {
        return version;
    }

    public Map<AdjListType, AdjacentList> getAdjacentLists() {
        return adjacentLists;
    }

    public List<PropertyGroup> getPropertyGroups() {
        return propertyGroups.getPropertyGroupList();
    }

    private void checkAdjListTypeExist(AdjListType adjListType) {
        if (adjListType == null) {
            throw new IllegalArgumentException("The adjacency list type is null");
        }
        if (!adjacentLists.containsKey(adjListType)) {
            throw new IllegalArgumentException(
                    "The adjacency list type "
                            + adjListType
                            + " does not exist in the edge info "
                            + this.edgeTriplet.getConcat());
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

    public boolean isValidated() {
        // Check if source type, edge type, or destination type is empty
        if (getSrcType() == null
                || getSrcType().isEmpty()
                || getEdgeType() == null
                || getEdgeType().isEmpty()
                || getDstType() == null
                || getDstType().isEmpty()) {
            return false;
        }

        // Check if chunk sizes are positive
        if (chunkSize <= 0 || srcChunkSize <= 0 || dstChunkSize <= 0) {
            return false;
        }

        // Check if prefix is null or empty
        if (baseUri == null || baseUri.toString().isEmpty()) {
            return false;
        }

        // Check if adjacent lists are empty
        if (adjacentLists.isEmpty()) {
            return false;
        }

        // Check if all adjacent lists are valid
        for (AdjacentList adjacentList : adjacentLists.values()) {
            if (adjacentList == null || !adjacentList.isValidated()) {
                return false;
            }
        }

        // Check if all property groups are valid and property names are unique
        Set<String> propertyNameSet = new HashSet<>();
        for (PropertyGroup pg : propertyGroups.getPropertyGroupList()) {
            if (pg == null || !pg.isValidated()) {
                return false;
            }

            for (Property p : pg.getPropertyList()) {
                if (propertyNameSet.contains(p.getName())) {
                    return false;
                }
                propertyNameSet.add(p.getName());
            }
        }
        return true;
    }

    private static class EdgeTriplet {
        private final String srcType;
        private final String edgeType;
        private final String dstType;

        public EdgeTriplet(String srcType, String edgeType, String dstType) {
            this.srcType = srcType;
            this.edgeType = edgeType;
            this.dstType = dstType;
        }

        public String getConcat() {
            return EdgeInfo.concat(srcType, edgeType, dstType);
        }

        @Override
        public String toString() {
            return getConcat();
        }

        public String getSrcType() {
            return srcType;
        }

        public String getEdgeType() {
            return edgeType;
        }

        public String getDstType() {
            return dstType;
        }
    }
}
