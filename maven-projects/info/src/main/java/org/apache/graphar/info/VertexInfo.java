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
import org.apache.graphar.info.type.DataType;
import org.apache.graphar.info.yaml.GraphYamlParser;
import org.apache.graphar.info.yaml.VertexYamlParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class VertexInfo {
    private final String label;
    private final long chunkSize;
    private final PropertyGroups propertyGroups;
    private final String prefix;
    private final String version;

    public VertexInfo(
            String label,
            long chunkSize,
            List<PropertyGroup> propertyGroups,
            String prefix,
            String version) {
        this.label = label;
        this.chunkSize = chunkSize;
        this.propertyGroups = new PropertyGroups(propertyGroups);
        this.prefix = prefix;
        this.version = version;
    }

    private VertexInfo(VertexYamlParser parser) {
        this(
                parser.getLabel(),
                parser.getChunk_size(),
                parser.getProperty_groups().stream()
                        .map(PropertyGroup::new)
                        .collect(Collectors.toUnmodifiableList()),
                parser.getPrefix(),
                parser.getVersion());
    }

    private VertexInfo(
            String label,
            long chunkSize,
            PropertyGroups propertyGroups,
            String prefix,
            String version) {
        this.label = label;
        this.chunkSize = chunkSize;
        this.propertyGroups = propertyGroups;
        this.prefix = prefix;
        this.version = version;
    }

    public static VertexInfo load(String vertexInfoPath) throws IOException {
        return load(vertexInfoPath, new Configuration());
    }

    public static VertexInfo load(String vertexInfoPath, Configuration conf) throws IOException {
        if (conf == null) {
            throw new IllegalArgumentException("Configuration is null");
        }
        return load(vertexInfoPath, FileSystem.get(conf));
    }

    public static VertexInfo load(String vertexInfoPath, FileSystem fileSystem) throws IOException {
        if (fileSystem == null) {
            throw new IllegalArgumentException("FileSystem is null");
        }
        FSDataInputStream inputStream = fileSystem.open(new Path(vertexInfoPath));
        Yaml vertexInfoYamlLoader =
                new Yaml(new Constructor(VertexYamlParser.class, new LoaderOptions()));
        VertexYamlParser vertexInfoYaml = vertexInfoYamlLoader.load(inputStream);
        return new VertexInfo(vertexInfoYaml);
    }

    public Optional<VertexInfo> addPropertyGroupAsNew(PropertyGroup propertyGroup) {
        return propertyGroups
                .addPropertyGroupAsNew(propertyGroup)
                .map(
                        newPropertyGroups ->
                                new VertexInfo(
                                        label, chunkSize, newPropertyGroups, prefix, version));
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
        return getPrefix() + "/" + propertyGroup.getPrefix();
    }

    public String getPropertyGroupChunkPath(PropertyGroup propertyGroup, long chunkIndex) {
        // PropertyGroup will be checked in getPropertyGroupPrefix
        return getPropertyGroupPrefix(propertyGroup) + "/chunk" + chunkIndex;
    }

    public String getVerticesNumFilePath() {
        return getPrefix() + "/vertex_count";
    }

    public void save(String filePath) throws IOException {
        save(filePath, new Configuration());
    }

    public void save(String filePath, Configuration conf) throws IOException {
        if (conf == null) {
            throw new IllegalArgumentException("Configuration is null");
        }
        save(filePath, FileSystem.get(conf));
    }

    public void save(String fileName, FileSystem fileSystem) throws IOException {
        if (fileSystem == null) {
            throw new IllegalArgumentException("FileSystem is null");
        }
        FSDataOutputStream outputStream = fileSystem.create(new Path(fileName));
        outputStream.writeBytes(dump());
        outputStream.close();
    }

    public String dump() {
        Yaml yaml = new Yaml(GraphYamlParser.getDumperOptions());
        VertexYamlParser vertexYaml = new VertexYamlParser(this);
        return yaml.dump(vertexYaml);
    }

    public String getLabel() {
        return label;
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

    public String getVersion() {
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
                            + getLabel());
        }
    }
}
