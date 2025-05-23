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
import org.apache.graphar.info.yaml.GraphYaml;
import org.apache.graphar.info.yaml.VertexYaml;
import org.apache.graphar.proto.DataType;
import org.yaml.snakeyaml.Yaml;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class VertexInfo {
    private final org.apache.graphar.proto.VertexInfo protoVertexInfo;
    private final PropertyGroups cachedPropertyGroups;
    private final String version; // Added version field

    public VertexInfo(
            String type,
            long chunkSize,
            List<PropertyGroup> propertyGroups,
            String prefix,
            String version) { // Added version parameter
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
        this.version = version; // Store version
    }

    // This constructor is likely used by ofProto, ensure it handles version if necessary
    // Or, if ofProto is not meant to be used with YAML loading path, it might not need version.
    // For now, let's assume ofProto creates a VertexInfo without a GAR YAML version,
    // or it should also somehow receive version information if it's constructing from a proto
    // that might have originated from a versioned YAML.
    // Given the task, focus is on loadVertexInfo path.
    private VertexInfo(org.apache.graphar.proto.VertexInfo protoVertexInfo, String version) {
        this.protoVertexInfo = protoVertexInfo;
        this.cachedPropertyGroups = PropertyGroups.ofProto(protoVertexInfo.getPropertiesList());
        this.version = version;
    }

    // Adjust ofProto if it needs to handle versioning from proto.
    // For now, this might create a VertexInfo with null version if called directly.
    // Or we might need a version string here too if this path is important for versioned objects.
    // Let's assume this path is less relevant for the current task or version is implicitly "unknown".
    public static VertexInfo ofProto(org.apache.graphar.proto.VertexInfo protoVertexInfo) {
        return new VertexInfo(protoVertexInfo, null); // Or handle version appropriately
    }


    public static VertexInfo loadVertexInfo(String yamlPath) throws IOException {
        String yamlContent = Files.readString(Paths.get(yamlPath));
        Yaml yaml = new Yaml();
        VertexYaml vertexYaml = yaml.loadAs(yamlContent, VertexYaml.class);
        // toVertexInfo in VertexYaml needs to be updated to pass the version to VertexInfo constructor
        return vertexYaml.toVertexInfo();
    }

    public String getVersion() { // Added getVersion method
        return version;
    }

    public PropertyGroup getPropertyGroup(String propertyName) {
        for (PropertyGroup pg : cachedPropertyGroups.getPropertyGroupList()) {
            if (pg.getProperties().stream().anyMatch(p -> p.getName().equals(propertyName))) {
                return pg;
            }
        }
        return null; // Or throw an exception if not found
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
                                        protoVertexInfo.getPrefix(),
                                        this.version)); // pass version along
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
        Yaml yaml = new Yaml(GraphYaml.getDumperOptions());
        VertexYaml vertexYaml = new VertexYaml(this);
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

    // Make sure this constructor is updated if it's used by external code, or make it private
    // For now, assuming the main public constructor is the one to be used.
    // This constructor is problematic if used to create versioned VertexInfo without version.
    // Let's remove it or make it private if the main one is sufficient.
    // For now, keeping the change minimal to the main constructor.
    // The constructor `public VertexInfo(String type, long chunkSize, List<PropertyGroup> propertyGroups, String prefix)`
    // was changed. If there was an implicit one or another one with these exact params, it's now shadowed.

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
