/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphar.info;

import com.alibaba.graphar.info.yaml.EdgeYamlParser;
import java.io.IOException;
import java.util.List;
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
    private final String prefix;
    private final List<AdjacentList> adjacentLists;
    private final List<PropertyGroup> propertyGroups;
    private final String version;

    public EdgeInfo(
            EdgeTriple edgeTriple,
            long chunkSize,
            long srcChunkSize,
            long dstChunkSize,
            String prefix,
            List<AdjacentList> adjacentLists,
            List<PropertyGroup> propertyGroups,
            String version) {
        this.edgeTriple = edgeTriple;
        this.chunkSize = chunkSize;
        this.srcChunkSize = srcChunkSize;
        this.dstChunkSize = dstChunkSize;
        this.prefix = prefix;
        this.adjacentLists = adjacentLists;
        this.propertyGroups = propertyGroups;
        this.version = version;
    }

    public EdgeInfo(
            String srcLabel,
            String edgeLabel,
            String dstLabel,
            long chunkSize,
            long srcChunkSize,
            long dstChunkSize,
            String prefix,
            List<AdjacentList> adjacentLists,
            List<PropertyGroup> propertyGroups,
            String version) {
        this(
                new EdgeTriple(srcLabel, edgeLabel, dstLabel),
                chunkSize,
                srcChunkSize,
                dstChunkSize,
                prefix,
                adjacentLists,
                propertyGroups,
                version);
    }

    EdgeInfo(EdgeYamlParser yamlParser) {
        this(
                yamlParser.getSrc_label(),
                yamlParser.getEdge_label(),
                yamlParser.getDst_label(),
                yamlParser.getChunk_size(),
                yamlParser.getSrc_chunk_size(),
                yamlParser.getDst_chunk_size(),
                yamlParser.getPrefix(),
                yamlParser.getAdjacent_lists().stream()
                        .map(AdjacentList::new)
                        .collect(Collectors.toList()),
                yamlParser.getProperty_groups().stream()
                        .map(PropertyGroup::new)
                        .collect(Collectors.toList()),
                yamlParser.getVersion());
    }

    public static EdgeInfo load(String edgeInfoPath, Configuration conf) throws IOException {
        if (conf == null) {
            conf = new Configuration();
        }
        Path path = new Path(edgeInfoPath);
        FileSystem fileSystem = path.getFileSystem(conf);
        FSDataInputStream inputStream = fileSystem.open(path);
        EdgeInfo ret =
                new Yaml(new Constructor(EdgeYamlParser.class, new LoaderOptions()))
                        .load(inputStream);
        return ret;
    }

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

    public List<AdjacentList> getAdjacentLists() {
        return adjacentLists;
    }

    public List<PropertyGroup> getPropertyGroups() {
        return propertyGroups;
    }

    public String getVersion() {
        return version;
    }
}
