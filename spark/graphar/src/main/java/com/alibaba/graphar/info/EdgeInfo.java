/*
 * Copyright 2022-2023 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphar.info;

import com.alibaba.graphar.info.type.AdjListType;
import com.alibaba.graphar.info.type.DataType;
import com.alibaba.graphar.info.yaml.EdgeYamlParser;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class EdgeInfo {
    private final EdgeTriple edgeTriple;
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
            long chunkSize,
            long srcChunkSize,
            long dstChunkSize,
            boolean directed,
            String prefix,
            List<AdjacentList> adjacentListsAsList,
            List<PropertyGroup> propertyGroupsAsList,
            String version) {
        this.edgeTriple = new EdgeTriple(srcLabel, edgeLabel, dstLabel);
        this.chunkSize = chunkSize;
        this.srcChunkSize = srcChunkSize;
        this.dstChunkSize = dstChunkSize;
        this.directed = directed;
        this.prefix = prefix;
        this.adjacentLists =
                adjacentListsAsList.stream()
                        .collect(Collectors.toMap(AdjacentList::getType, Function.identity()));
        this.propertyGroups = new PropertyGroups(propertyGroupsAsList);
        this.version = version;
    }

    EdgeInfo(EdgeYamlParser yamlParser) {
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
                        .collect(Collectors.toList()),
                yamlParser.getProperty_groups().stream()
                        .map(PropertyGroup::new)
                        .collect(Collectors.toList()),
                yamlParser.getVersion());
    }

    private EdgeInfo(
            EdgeTriple edgeTriple,
            long chunkSize,
            long srcChunkSize,
            long dstChunkSize,
            boolean directed,
            String prefix,
            Map<AdjListType, AdjacentList> adjacentLists,
            PropertyGroups propertyGroups,
            String version) {
        this.edgeTriple = edgeTriple;
        this.chunkSize = chunkSize;
        this.srcChunkSize = srcChunkSize;
        this.dstChunkSize = dstChunkSize;
        this.directed = directed;
        this.prefix = prefix;
        this.adjacentLists = adjacentLists;
        this.propertyGroups = propertyGroups;
        this.version = version;
    }

    public static EdgeInfo load(String edgeInfoPath, Configuration conf) throws IOException {
        if (conf == null) {
            conf = new Configuration();
        }
        Path path = new Path(edgeInfoPath);
        FileSystem fileSystem = path.getFileSystem(conf);
        FSDataInputStream inputStream = fileSystem.open(path);
        Yaml edgeInfoYamlLoader =
                new Yaml(new Constructor(EdgeYamlParser.class, new LoaderOptions()));
        EdgeYamlParser edgeInfoYaml = edgeInfoYamlLoader.load(inputStream);
        return new EdgeInfo(edgeInfoYaml);
    }

    public EdgeInfo addAdjacentList(AdjacentList adjacentList) {
        Map<AdjListType, AdjacentList> newAdjacentLists = new HashMap<>(adjacentLists);
        newAdjacentLists.put(adjacentList.getType(), adjacentList);
        return new EdgeInfo(
                edgeTriple,
                chunkSize,
                srcChunkSize,
                dstChunkSize,
                directed,
                prefix,
                newAdjacentLists,
                propertyGroups,
                version);
    }

    public EdgeInfo addPropertyGroup(PropertyGroup propertyGroup) {
        return new EdgeInfo(
                edgeTriple,
                chunkSize,
                srcChunkSize,
                dstChunkSize,
                directed,
                prefix,
                adjacentLists,
                propertyGroups.addPropertyGroup(propertyGroup),
                version);
    }

    public boolean hasAdjacentListType(AdjListType adjListType) {
        return adjacentLists.containsKey(adjListType);
    }

    public boolean hasProperty(String propertyName) {
        return propertyGroups.hasProperty(propertyName);
    }

    public boolean hasPropertyGroup(PropertyGroup propertyGroup) {
        return propertyGroups.hasPropertyGroup(propertyGroup);
    }

    public AdjacentList getAdjacentList(AdjListType adjListType) {
        return adjacentLists.get(adjListType);
    }

    public int getPropertyGroupNum() {
        return propertyGroups.getPropertyGroupNum();
    }

    public PropertyGroup getPropertyGroup(String property) {
        return propertyGroups.getPropertyGroup(property);
    }

    // TODO(@Thespica): Implement file path get methods

    //    public String getVerticesNumFilePath(AdjListType adjListType) {
    //    }
    //
    //    public String getEdgesNumFilePath(long vertexChunkIndex, AdjListType adjListType) {
    //    }
    //
    //    public String getAdjListFilePath(long vertexChunkIndex, long edgeChunkIndex, AdjListType
    // adjListType) {
    //    }
    //
    //    public String getAdjListPathPrefix(AdjListType adjListType) {
    //    }
    //
    //    /**
    //     * Get the adjacency list offset chunk file path of vertex chunk
    //     * the offset chunks is aligned with the vertex chunks
    //     *
    //     * @param vertexChunkIndex index of vertex chunk
    //     * @param adjListType      The adjacency list type.
    //     */
    //    public String getAdjListOffsetFilePath(long vertexChunkIndex, AdjListType adjListType) {
    //
    //    }
    //
    //    /**
    //     * Get the path prefix of the adjacency list offset chunk for the given
    //     * adjacency list type.
    //     *
    //     * @param adjListType The adjacency list type.
    //     * @return A Result object containing the path prefix, or a Status object
    //     * indicating an error.
    //     */
    //    public String getOffsetPathPrefix(AdjListType adjListType) {
    //
    //    }
    //
    //    public String getPropertyFilePath(
    //            PropertyGroup propertyGroup,
    //            AdjListType adjListType, long vertexChunkIndex,
    //            long edgeChunkIndex) {
    //
    //    }
    //
    //    /**
    //     * Get the path prefix of the property group chunk for the given
    //     * adjacency list type.
    //     *
    //     * @param propertyGroup property group.
    //     * @param adjListType   The adjacency list type.
    //     * @return A Result object containing the path prefix, or a Status object
    //     * indicating an error.
    //     */
    //    public String getPropertyGroupPathPrefix(
    //            PropertyGroup propertyGroup,
    //            AdjListType adjListType) {
    //
    //    }

    DataType getPropertyType(String propertyName) {
        return propertyGroups.getProperty(propertyName).getDataType();
    }

    boolean isPrimaryKey(String propertyName) {
        return propertyGroups.isPrimaryKey(propertyName);
    }

    boolean isNullableKey(String propertyName) {
        return propertyGroups.isNullableKey(propertyName);
    }

    // TODO(@Thespica): Implement save and dump methods
    //    /**
    //     * Saves the edge info to a YAML file.
    //     *
    //     * @param fileName The name of the file to save to.
    //     * @return A Status object indicating success or failure.
    //     */
    //    void save(String fileName) {
    //
    //    }
    //
    //    /**
    //     * Returns the edge info as a YAML formatted string.
    //     *
    //     * @return A Result object containing the YAML string, or a Status object
    //     * indicating an error.
    //     */
    //    public String dump() {
    //
    //    }

    public EdgeTriple getEdgeTriple() {
        return edgeTriple;
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

    public String getPrefix() {
        return prefix;
    }

    public Map<AdjListType, AdjacentList> getAdjacentLists() {
        return adjacentLists;
    }

    public List<PropertyGroup> getPropertyGroups() {
        return propertyGroups.toList();
    }

    public String getVersion() {
        return version;
    }
}
