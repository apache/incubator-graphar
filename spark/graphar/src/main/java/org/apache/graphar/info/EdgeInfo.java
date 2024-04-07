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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.graphar.info.type.AdjListType;
import org.apache.graphar.info.type.DataType;
import org.apache.graphar.info.yaml.EdgeYamlParser;
import org.apache.graphar.info.yaml.GraphYamlParser;
import org.apache.graphar.util.GeneralParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class EdgeInfo {
    private final EdgeTriplet edgeTriplet;
    private final long chunkSize;
    private final long srcChunkSize;
    private final long dstChunkSize;
    private final boolean directed;
    private final String prefix;
    private final Map<AdjListType, AdjacentList> adjacentLists;
    private final PropertyGroups propertyGroups;
    private final String version;

    public EdgeInfo(
            String srcLabel,
            String edgeLabel,
            String dstLabel,
            long srcChunkSize,
            long chunkSize,
            long dstChunkSize,
            boolean directed,
            String prefix,
            List<AdjacentList> adjacentListsAsList,
            List<PropertyGroup> propertyGroupsAsList,
            String version) {
        this.edgeTriplet = new EdgeTriplet(srcLabel, edgeLabel, dstLabel);
        this.chunkSize = chunkSize;
        this.srcChunkSize = srcChunkSize;
        this.dstChunkSize = dstChunkSize;
        this.directed = directed;
        this.prefix = prefix;
        this.adjacentLists =
                adjacentListsAsList.stream()
                        .collect(
                                Collectors.toUnmodifiableMap(
                                        AdjacentList::getType, Function.identity()));
        this.propertyGroups = new PropertyGroups(propertyGroupsAsList);
        this.version = version;
    }

    private EdgeInfo(EdgeYamlParser yamlParser) {
        this(
                yamlParser.getSrc_label(),
                yamlParser.getEdge_label(),
                yamlParser.getDst_label(),
                yamlParser.getChunk_size(),
                yamlParser.getSrc_chunk_size(),
                yamlParser.getDst_chunk_size(),
                yamlParser.isDirected(),
                yamlParser.getPrefix(),
                yamlParser.getAdjacent_lists().stream()
                        .map(AdjacentList::new)
                        .collect(Collectors.toUnmodifiableList()),
                yamlParser.getProperty_groups().stream()
                        .map(PropertyGroup::new)
                        .collect(Collectors.toUnmodifiableList()),
                yamlParser.getVersion());
    }

    private EdgeInfo(
            EdgeTriplet edgeTriplet,
            long chunkSize,
            long srcChunkSize,
            long dstChunkSize,
            boolean directed,
            String prefix,
            Map<AdjListType, AdjacentList> adjacentLists,
            PropertyGroups propertyGroups,
            String version) {
        this.edgeTriplet = edgeTriplet;
        this.chunkSize = chunkSize;
        this.srcChunkSize = srcChunkSize;
        this.dstChunkSize = dstChunkSize;
        this.directed = directed;
        this.prefix = prefix;
        this.adjacentLists = adjacentLists;
        this.propertyGroups = propertyGroups;
        this.version = version;
    }

    public static EdgeInfo load(String edgeInfoPath) throws IOException {
        return load(edgeInfoPath, new Configuration());
    }

    public static EdgeInfo load(String edgeInfoPath, Configuration conf) throws IOException {
        if (conf == null) {
            conf = new Configuration();
        }
        return load(edgeInfoPath, FileSystem.get(conf));
    }

    public static EdgeInfo load(String edgeInfoPath, FileSystem fileSystem) throws IOException {
        if (fileSystem == null) {
            fileSystem = FileSystem.get(new Configuration());
        }
        FSDataInputStream inputStream = fileSystem.open(new Path(edgeInfoPath));
        Yaml edgeInfoYamlLoader =
                new Yaml(new Constructor(EdgeYamlParser.class, new LoaderOptions()));
        EdgeYamlParser edgeInfoYaml = edgeInfoYamlLoader.load(inputStream);
        return new EdgeInfo(edgeInfoYaml);
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
                        prefix,
                        newAdjacentLists,
                        propertyGroups,
                        version));
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
                                        prefix,
                                        adjacentLists,
                                        newPropertyGroups,
                                        version));
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

    public String getPropertyGroupPrefix(PropertyGroup propertyGroup) {
        checkPropertyGroupExist(propertyGroup);
        return getPrefix() + "/" + propertyGroup.getPrefix();
    }

    public String getPropertyGroupChunkPath(PropertyGroup propertyGroup, long chunkIndex) {
        // PropertyGroup will be checked in getPropertyGroupPrefix
        return getPropertyGroupPrefix(propertyGroup) + "/chunk" + chunkIndex;
    }

    public String getAdjacentListPrefix(AdjListType adjListType) {
        return getPrefix() + "/" + getAdjacentList(adjListType).getPrefix() + "/adj_list";
    }

    public String getAdjacentListChunkPath(AdjListType adjListType, long vertexChunkIndex) {
        return getAdjacentListPrefix(adjListType) + "/chunk" + vertexChunkIndex;
    }

    public String getOffsetPrefix(AdjListType adjListType) {
        return getAdjacentListPrefix(adjListType) + "/offset";
    }

    public String getOffsetChunkPath(AdjListType adjListType, long vertexChunkIndex) {
        return getOffsetPrefix(adjListType) + "/chunk" + vertexChunkIndex;
    }

    public String getVerticesNumFilePath(AdjListType adjListType) {
        return getAdjacentListPrefix(adjListType) + "/vertex_count";
    }

    public String getEdgesNumFilePath(AdjListType adjListType, long vertexChunkIndex) {
        return getAdjacentListPrefix(adjListType) + "/edge_count" + vertexChunkIndex;
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

    public void save(String filePath, Configuration conf) throws IOException {
        if (conf == null) {
            conf = new Configuration();
        }
        save(filePath, FileSystem.get(conf));
    }

    public void save(String fileName, FileSystem fileSystem) throws IOException {
        if (fileSystem == null) {
            fileSystem = FileSystem.get(new Configuration());
        }
        FSDataOutputStream outputStream = fileSystem.create(new Path(fileName));
        outputStream.writeBytes(dump());
        outputStream.close();
    }

    public String dump() {
        Yaml yaml = new Yaml(GraphYamlParser.getDumperOptions());
        EdgeYamlParser edgeYaml = new EdgeYamlParser(this);
        return yaml.dump(edgeYaml);
    }

    public String getConcat() {
        return edgeTriplet.getConcat();
    }

    public String getSrcLabel() {
        return edgeTriplet.getSrcLabel();
    }

    public String getEdgeLabel() {
        return edgeTriplet.getEdgeLabel();
    }

    public String getDstLabel() {
        return edgeTriplet.getDstLabel();
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
        return prefix;
    }

    public Map<AdjListType, AdjacentList> getAdjacentLists() {
        return adjacentLists;
    }

    public List<PropertyGroup> getPropertyGroups() {
        return propertyGroups.getPropertyGroupList();
    }

    public String getVersion() {
        return version;
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

    private static class EdgeTriplet {
        private final String srcLabel;
        private final String edgeLabel;
        private final String dstLabel;

        public EdgeTriplet(String srcLabel, String edgeLabel, String dstLabel) {
            this.srcLabel = srcLabel;
            this.edgeLabel = edgeLabel;
            this.dstLabel = dstLabel;
        }

        public String getConcat() {
            return srcLabel
                    + GeneralParams.regularSeparator
                    + edgeLabel
                    + GeneralParams.regularSeparator
                    + dstLabel;
        }

        @Override
        public String toString() {
            return getConcat();
        }

        public String getSrcLabel() {
            return srcLabel;
        }

        public String getEdgeLabel() {
            return edgeLabel;
        }

        public String getDstLabel() {
            return dstLabel;
        }
    }
}
