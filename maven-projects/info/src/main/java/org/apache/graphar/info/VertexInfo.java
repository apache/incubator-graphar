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
import java.util.List;
import java.util.Optional;
import org.apache.graphar.info.type.DataType;
import org.apache.graphar.info.yaml.GraphYaml;
import org.apache.graphar.info.yaml.VertexYaml;
import org.yaml.snakeyaml.Yaml;

public class VertexInfo {
    private final String type;
    private final long chunkSize;
    private final PropertyGroups propertyGroups;
    private final URI baseUri;
    private final VersionInfo version;

    public static VertexInfoBuilder builder() {
        return new VertexInfoBuilder();
    }

    public static class VertexInfoBuilder {
        private String type;
        private long chunkSize;
        private PropertyGroups propertyGroups;
        private URI baseUri;
        private VersionInfo version;

        private VertexInfoBuilder() {}

        public VertexInfoBuilder type(String type) {
            this.type = type;
            return this;
        }

        public VertexInfoBuilder chunkSize(long chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }

        public VertexInfoBuilder propertyGroups(PropertyGroups propertyGroups) {
            this.propertyGroups = propertyGroups;
            return this;
        }

        public VertexInfoBuilder baseUri(URI baseUri) {
            this.baseUri = baseUri;

            return this;
        }

        public VertexInfoBuilder baseUri(String baseUri) {
            this.baseUri = URI.create(baseUri);

            return this;
        }

        public VertexInfoBuilder version(VersionInfo version) {
            this.version = version;

            return this;
        }

        public VertexInfoBuilder version(String version) {
            this.version = VersionParser.getVersion(version);

            return this;
        }

        public VertexInfo build() {
            if (chunkSize < 0) {
                throw new IllegalArgumentException("Chunk size cannot be negative: " + chunkSize);
            }
            if (baseUri == null) {
                throw new IllegalArgumentException("Base URI cannot be null");
            }
            return new VertexInfo(this);
        }
    }

    private VertexInfo(VertexInfoBuilder builder) {
        this.type = builder.type;
        this.chunkSize = builder.chunkSize;
        this.propertyGroups = builder.propertyGroups;
        this.baseUri = builder.baseUri;
        this.version = builder.version;
    }

    public VertexInfo(
            String type,
            long chunkSize,
            List<PropertyGroup> propertyGroups,
            String prefix,
            String version) {
        this(type, chunkSize, propertyGroups, URI.create(prefix), version);
    }

    public VertexInfo(
            String type,
            long chunkSize,
            List<PropertyGroup> propertyGroups,
            URI baseUri,
            String version) {
        this(type, chunkSize, propertyGroups, baseUri, VersionParser.getVersion(version));
    }

    public VertexInfo(
            String type,
            long chunkSize,
            List<PropertyGroup> propertyGroups,
            URI baseUri,
            VersionInfo version) {
        if (chunkSize < 0) {
            throw new IllegalArgumentException("Chunk size cannot be negative: " + chunkSize);
        }
        this.type = type;
        this.chunkSize = chunkSize;
        this.propertyGroups = new PropertyGroups(propertyGroups);
        this.baseUri = baseUri;
        this.version = version;
    }

    public Optional<VertexInfo> addPropertyGroupAsNew(PropertyGroup propertyGroup) {
        return propertyGroups
                .addPropertyGroupAsNew(propertyGroup)
                .map(PropertyGroups::getPropertyGroupList)
                .map(
                        newPropertyGroups ->
                                new VertexInfo(
                                        type, chunkSize, newPropertyGroups, baseUri, version));
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

    public URI getPropertyGroupPrefix(PropertyGroup propertyGroup) {
        checkPropertyGroupExist(propertyGroup);
        return getBaseUri().resolve(propertyGroup.getBaseUri());
    }

    public URI getPropertyGroupChunkPath(PropertyGroup propertyGroup, long chunkIndex) {
        // PropertyGroup will be checked in getPropertyGroupPrefix
        return getPropertyGroupPrefix(propertyGroup).resolve("chunk" + chunkIndex);
    }

    public URI getVerticesNumFilePath() {
        return getBaseUri().resolve("vertex_count");
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
        return baseUri.toString();
    }

    public URI getBaseUri() {
        return baseUri;
    }

    public VersionInfo getVersion() {
        return version;
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
