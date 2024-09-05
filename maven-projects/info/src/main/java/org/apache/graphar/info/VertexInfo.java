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
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.graphar.info.yaml.GraphYamlParser;
import org.apache.graphar.info.yaml.VertexYamlParser;
import org.apache.graphar.proto.DataType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class VertexInfo {
    private final org.apache.graphar.proto.VertexInfo protoVertexInfo;
    private final PropertyGroups cachedPropertyGroups;

    public VertexInfo(
            String type, long chunkSize, List<PropertyGroup> propertyGroups, String prefix) {
        this.cachedPropertyGroups = new PropertyGroups(propertyGroups);
        this.protoVertexInfo =
                org.apache.graphar.proto.VertexInfo.newBuilder()
                        .setType(type)
                        .setChunkSize(chunkSize)
                        .addAllProperties(
                                propertyGroups.stream()
                                        .map(PropertyGroup::getProto)
                                        .collect(Collectors.toList()))
                        .setPrefix(prefix)
                        .build();
    }

    public VertexInfo(org.apache.graphar.proto.VertexInfo protoVertexInfo) {
        this.protoVertexInfo = protoVertexInfo;
        this.cachedPropertyGroups = new PropertyGroups(protoVertexInfo.getPropertiesList());
    }

    org.apache.graphar.proto.VertexInfo getProto() {
        return protoVertexInfo;
    }

    public Optional<VertexInfo> addPropertyGroupAsNew(PropertyGroup propertyGroup) {
        // do not need check property group exist, because PropertyGroups will check it
        return cachedPropertyGroups
                .addPropertyGroupAsNew(propertyGroup)
                .map(
                        newPropertyGroups ->
                                new VertexInfo(
                                        protoVertexInfo.getType(),
                                        protoVertexInfo.getChunkSize(),
                                        newPropertyGroups.getPropertyGroupList(),
                                        protoVertexInfo.getPrefix()));
    }

    public int propertyGroupNum() {
        return cachedPropertyGroups.getPropertyGroupNum();
    }

    public DataType getPropertyType(String propertyName) {
        return cachedPropertyGroups.getPropertyType(propertyName);
    }

    public boolean hasProperty(String propertyName) {
        return cachedPropertyGroups.hasProperty(propertyName);
    }

    public boolean isPrimaryKey(String propertyName) {
        return cachedPropertyGroups.isPrimaryKey(propertyName);
    }

    public boolean isNullableKey(String propertyName) {
        return cachedPropertyGroups.isNullableKey(propertyName);
    }

    public boolean hasPropertyGroup(PropertyGroup propertyGroup) {
        return cachedPropertyGroups.hasPropertyGroup(propertyGroup);
    }

    public String getPropertyGroupPrefix(PropertyGroup propertyGroup) {
        checkPropertyGroupExist(propertyGroup);
        return getPrefix() + "/" + propertyGroup.getPrefix();
    }

    public String getPropertyGroupChunkPath(PropertyGroup propertyGroup, long chunkIndex) {
        // PropertyGroup will be checked in getPropertyGroupPrefix
        return getPropertyGroupPrefix(propertyGroup) + "/chunk" + chunkIndex;
    }

    public String getVerticesNumFilePath() {
        return getPrefix() + "/vertex_count";
    }

    public String dump() {
        Yaml yaml = new Yaml(GraphYamlParser.getDumperOptions());
        VertexYamlParser vertexYaml = new VertexYamlParser(this);
        return yaml.dump(vertexYaml);
    }

    public String getType() {
        return protoVertexInfo.getType();
    }

    public long getChunkSize() {
        return protoVertexInfo.getChunkSize();
    }

    public List<PropertyGroup> getPropertyGroups() {
        return cachedPropertyGroups.getPropertyGroupList();
    }

    public String getPrefix() {
        return protoVertexInfo.getPrefix();
    }

    public String getVertexPath() {
        return getPrefix() + "/" + getType() + ".vertex.yaml";
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
