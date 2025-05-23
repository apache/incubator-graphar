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

package org.apache.graphar.info.yaml;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.graphar.info.EdgeInfo;

public class EdgeYaml {
    private String src_type;
    private String edge_type;
    private String dst_type;
    private long chunk_size;
    private long src_chunk_size;
    private long dst_chunk_size;
    private boolean directed;
    private String prefix;
    private List<AdjacentListYaml> adj_lists;
    private List<PropertyGroupYaml> property_groups;
    private String version;

    public EdgeYaml() {
        this.src_type = "";
        this.edge_type = "";
        this.dst_type = "";
        this.chunk_size = 0;
        this.src_chunk_size = 0;
        this.dst_chunk_size = 0;
        this.directed = false;
        this.prefix = "";
        this.adj_lists = new ArrayList<>();
        this.property_groups = new ArrayList<>();
        this.version = "v1";
    }

    public EdgeYaml(EdgeInfo edgeInfo) {
        this.src_type = edgeInfo.getSrcLabel();
        this.edge_type = edgeInfo.getSrcLabel();
        this.dst_type = edgeInfo.getDstLabel();
        this.chunk_size = edgeInfo.getChunkSize();
        this.src_chunk_size = edgeInfo.getSrcChunkSize();
        this.dst_chunk_size = edgeInfo.getDstChunkSize();
        this.directed = edgeInfo.isDirected();
        this.prefix = edgeInfo.getPrefix();
        this.adj_lists =
                edgeInfo.getAdjacentLists().values().stream()
                        .map(AdjacentListYaml::new)
                        .collect(Collectors.toList());
        this.property_groups =
                edgeInfo.getPropertyGroups().stream()
                        .map(PropertyGroupYaml::new)
                        .collect(Collectors.toList());
    }

    public EdgeInfo toEdgeInfo() {
        return new EdgeInfo(
                src_type,
                edge_type,
                dst_type,
                chunk_size,
                src_chunk_size,
                dst_chunk_size,
                directed,
                prefix,
                adj_lists.stream()
                        .map(AdjacentListYaml::toAdjacentList)
                        .collect(Collectors.toUnmodifiableList()),
                property_groups.stream()
                        .map(PropertyGroupYaml::toPropertyGroup)
                        .collect(Collectors.toList()),
                version); // Pass the version from EdgeYaml
    }

    public String getSrc_type() {
        return src_type;
    }

    public void setSrc_type(String src_type) {
        this.src_type = src_type;
    }

    public String getEdge_type() {
        return edge_type;
    }

    public void setEdge_type(String edge_type) {
        this.edge_type = edge_type;
    }

    public String getDst_type() {
        return dst_type;
    }

    public void setDst_type(String dst_type) {
        this.dst_type = dst_type;
    }

    public boolean isDirected() {
        return directed;
    }

    public void setDirected(boolean directed) {
        this.directed = directed;
    }

    public long getChunk_size() {
        return chunk_size;
    }

    public void setChunk_size(long chunk_size) {
        this.chunk_size = chunk_size;
    }

    public long getSrc_chunk_size() {
        return src_chunk_size;
    }

    public void setSrc_chunk_size(long src_chunk_size) {
        this.src_chunk_size = src_chunk_size;
    }

    public long getDst_chunk_size() {
        return dst_chunk_size;
    }

    public void setDst_chunk_size(long dst_chunk_size) {
        this.dst_chunk_size = dst_chunk_size;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public List<AdjacentListYaml> getAdj_lists() {
        return adj_lists;
    }

    public void setAdj_lists(List<AdjacentListYaml> adj_lists) {
        this.adj_lists = adj_lists;
    }

    public List<PropertyGroupYaml> getProperty_groups() {
        return property_groups;
    }

    public void setProperty_groups(List<PropertyGroupYaml> property_groups) {
        this.property_groups = property_groups;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}
