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
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.graphar.info.type.DataType;
import org.apache.graphar.info.yaml.GraphYaml;
import org.apache.graphar.info.yaml.VertexYaml;
import org.yaml.snakeyaml.Yaml;

public class VertexInfo {
    private final String type;
    private final long chunkSize;
    private final PropertyGroups propertyGroups;
    private final String prefix;
    private final VersionInfo version;

    public VertexInfo(
            String type,
            long chunkSize,
            List<PropertyGroup> propertyGroups,
            String prefix,
            String version) {
        this(type, chunkSize, propertyGroups, prefix, VersionParser.getVersion(version));
    }

    public VertexInfo(
            String type,
            long chunkSize,
            List<PropertyGroup> propertyGroups,
            String prefix,
            VersionInfo version) {
        this.type = type;
        this.chunkSize = chunkSize;
        this.propertyGroups = new PropertyGroups(propertyGroups);
        this.prefix = prefix;
        this.version = version;
    }

    private VertexInfo(VertexYaml parser) {
        this(
                parser.getType(),
                parser.getChunk_size(),
                parser.getProperty_groups().stream()
                        .map(PropertyGroup::new)
                        .collect(Collectors.toUnmodifiableList()),
                parser.getPrefix(),
                parser.getVersion());
    }

    public Optional<VertexInfo> addPropertyGroupAsNew(PropertyGroup propertyGroup) {
        return propertyGroups
                .addPropertyGroupAsNew(propertyGroup)
                .map(PropertyGroups::getPropertyGroupList)
                .map(
                        newPropertyGroups ->
                                new VertexInfo(
                                        type, chunkSize, newPropertyGroups, prefix, version));
    }

    public int propertyGroupNum() {
        return propertyGroups.getPropertyGroupNum();
    }

    public DataType getPropertyType(String propertyName) {
        return propertyGroups.getPropertyType(propertyName);
    }

    public boolean hasProperty(String propertyName) {
        return propertyGroups.hasProperty(propertyName);
    }

    public boolean isPrimaryKey(String propertyName) {
        return propertyGroups.isPrimaryKey(propertyName);
    }

    public boolean isNullableKey(String propertyName) {
        return propertyGroups.isNullableKey(propertyName);
    }

    public boolean hasPropertyGroup(PropertyGroup propertyGroup) {
        return propertyGroups.hasPropertyGroup(propertyGroup);
    }

    public String getPropertyGroupPrefix(PropertyGroup propertyGroup) {
        checkPropertyGroupExist(propertyGroup);
        return getPrefix() + propertyGroup.getPrefix();
    }

    public String getPropertyGroupChunkPath(PropertyGroup propertyGroup, long chunkIndex) {
        // PropertyGroup will be checked in getPropertyGroupPrefix
        return getPropertyGroupPrefix(propertyGroup) + "chunk" + chunkIndex;
    }

    public String getVerticesNumFilePath() {
        return getPrefix() + "vertex_count";
    }

    public String dump() {
        Yaml yaml = new Yaml(GraphYaml.getRepresenter(), GraphYaml.getDumperOptions());
        VertexYaml vertexYaml = new VertexYaml(this);
        return yaml.dump(vertexYaml);
    }

    public String getType() {
        return type;
    }

    public long getChunkSize() {
        return chunkSize;
    }

    public List<PropertyGroup> getPropertyGroups() {
        return propertyGroups.getPropertyGroupList();
    }

    public String getPrefix() {
        return prefix;
    }

    public VersionInfo getVersion() {
        return version;
    }

    public String getVertexPath() {
        return getPrefix() + getType() + ".vertex.yaml";
    }

    private void checkPropertyGroupExist(PropertyGroup propertyGroup) {
        if (propertyGroup == null) {
            throw new IllegalArgumentException("Property group is null");
        }
        if (!hasPropertyGroup(propertyGroup)) {
            throw new IllegalArgumentException(
                    "Property group "
                            + propertyGroup
                            + " does not exist in the vertex "
                            + getType());
        }
    }
}
