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
import org.apache.graphar.info.VertexInfo;

public class VertexYaml {
    private String type;
    private long chunk_size;
    private List<PropertyGroupYaml> property_groups;
    private String prefix;
    private String version;

    public VertexYaml() {
        this.type = "";
        this.chunk_size = 0;
        this.property_groups = new ArrayList<>();
        this.prefix = "";
        this.version = "";
    }

    public VertexYaml(VertexInfo vertexInfo) {
        this.type = vertexInfo.getLabel();
        this.chunk_size = vertexInfo.getChunkSize();
        this.property_groups =
                vertexInfo.getPropertyGroups().stream()
                        .map(PropertyGroupYaml::new)
                        .collect(Collectors.toList());
        this.prefix = vertexInfo.getPrefix();
        this.version = vertexInfo.getVersion();
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getChunk_size() {
        return chunk_size;
    }

    public void setChunk_size(long chunk_size) {
        this.chunk_size = chunk_size;
    }

    public List<PropertyGroupYaml> getProperty_groups() {
        return property_groups;
    }

    public void setProperty_groups(List<PropertyGroupYaml> property_groups) {
        this.property_groups = property_groups;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}
