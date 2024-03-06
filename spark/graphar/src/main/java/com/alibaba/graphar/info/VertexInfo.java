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

import com.alibaba.graphar.info.yaml.VertexYamlParser;
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

public class VertexInfo {
    private final String label;
    private final long chunkSize;
    private final List<PropertyGroup> propertyGroups;
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
        this.propertyGroups = propertyGroups;
        this.prefix = prefix;
        this.version = version;
    }

    VertexInfo(VertexYamlParser parser) {
        this(
                parser.getLabel(),
                parser.getChunk_size(),
                parser.getProperty_groups().stream()
                        .map(PropertyGroup::new)
                        .collect(Collectors.toList()),
                parser.getPrefix(),
                parser.getVersion());
    }

    public static VertexInfo load(String vertexInfoPath, Configuration conf) throws IOException {
        if (conf == null) {
            conf = new Configuration();
        }
        Path path = new Path(vertexInfoPath);
        FileSystem fileSystem = path.getFileSystem(conf);
        FSDataInputStream inputStream = fileSystem.open(path);
        VertexInfo ret =
                new Yaml(new Constructor(VertexInfo.class, new LoaderOptions())).load(inputStream);
        return ret;
    }

    public String getLabel() {
        return label;
    }

    public long getChunkSize() {
        return chunkSize;
    }

    public List<PropertyGroup> getPropertyGroups() {
        return propertyGroups;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getVersion() {
        return version;
    }
}
