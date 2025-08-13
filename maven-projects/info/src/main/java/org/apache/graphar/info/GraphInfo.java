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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.graphar.info.yaml.GraphYaml;
import org.apache.graphar.util.GeneralParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class GraphInfo {
    private final String name;
    private final List<VertexInfo> vertexInfos;
    private final List<EdgeInfo> edgeInfos;
    private final String prefix;
    private final Map<String, VertexInfo> vertexType2VertexInfo;
    private final Map<String, EdgeInfo> edgeConcat2EdgeInfo;
    private final VersionInfo version;

    public GraphInfo(
            String name,
            List<VertexInfo> vertexInfos,
            List<EdgeInfo> edgeInfos,
            String prefix,
            String version) {
        this.name = name;
        this.vertexInfos = List.copyOf(vertexInfos);
        this.edgeInfos = List.copyOf(edgeInfos);
        this.prefix = prefix;
        this.version = VersionParser.getVersion(version);
        this.vertexType2VertexInfo =
                vertexInfos.stream()
                        .collect(
                                Collectors.toUnmodifiableMap(
                                        VertexInfo::getType, Function.identity()));
        this.edgeConcat2EdgeInfo =
                edgeInfos.stream()
                        .collect(
                                Collectors.toUnmodifiableMap(
                                        EdgeInfo::getConcat, Function.identity()));
    }

    private GraphInfo(GraphYaml graphYaml, Configuration conf) throws IOException {
        this(
                graphYaml.getName(),
                vertexFileNames2VertexInfos(graphYaml.getVertices(), conf),
                edgeFileNames2EdgeInfos(graphYaml.getEdges(), conf),
                graphYaml.getPrefix(),
                graphYaml.getVersion());
    }

    private GraphInfo(
            String name,
            List<VertexInfo> vertexInfos,
            List<EdgeInfo> edgeInfos,
            String prefix,
            String version,
            Map<String, VertexInfo> vertexType2VertexInfo,
            Map<String, EdgeInfo> edgeConcat2EdgeInfo) {
        this(name, vertexInfos, edgeInfos, prefix, VersionParser.getVersion(version), vertexType2VertexInfo, edgeConcat2EdgeInfo);
    }

    private GraphInfo(
            String name,
            List<VertexInfo> vertexInfos,
            List<EdgeInfo> edgeInfos,
            String prefix,
            VersionInfo version,
            Map<String, VertexInfo> vertexType2VertexInfo,
            Map<String, EdgeInfo> edgeConcat2EdgeInfo) {
        this.name = name;
        this.vertexInfos = vertexInfos;
        this.edgeInfos = edgeInfos;
        this.prefix = prefix;
        this.version = version;
        this.vertexType2VertexInfo = vertexType2VertexInfo;
        this.edgeConcat2EdgeInfo = edgeConcat2EdgeInfo;
    }

    public static GraphInfo load(String graphPath) throws IOException {
        return load(graphPath, new Configuration());
    }

    public static GraphInfo load(String graphPath, FileSystem fileSystem) throws IOException {
        if (fileSystem == null) {
            throw new IllegalArgumentException("FileSystem is null");
        }
        return load(graphPath, fileSystem.getConf());
    }

    public static GraphInfo load(String graphPath, Configuration conf) throws IOException {
        if (conf == null) {
            throw new IllegalArgumentException("Configuration is null");
        }
        Path path = new Path(graphPath);
        FileSystem fileSystem = path.getFileSystem(conf);
        FSDataInputStream inputStream = fileSystem.open(path);
        Yaml graphYamlLoader = new Yaml(new Constructor(GraphYaml.class, new LoaderOptions()));
        GraphYaml graphYaml = graphYamlLoader.load(inputStream);
        return new GraphInfo(graphYaml, conf);
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
        Yaml yaml = new Yaml(GraphYaml.getRepresenter(), GraphYaml.getDumperOptions());
        GraphYaml graphYaml = new GraphYaml(this);
        return yaml.dump(graphYaml);
    }

    public Optional<GraphInfo> addVertexAsNew(VertexInfo vertexInfo) {
        if (vertexInfo == null || hasVertexInfo(vertexInfo.getType())) {
            return Optional.empty();
        }
        List<VertexInfo> newVertexInfos =
                Stream.concat(vertexInfos.stream(), Stream.of(vertexInfo))
                        .collect(Collectors.toList());
        Map<String, VertexInfo> newVertexType2VertexInfo =
                Stream.concat(
                                vertexType2VertexInfo.entrySet().stream(),
                                Stream.of(Map.entry(vertexInfo.getType(), vertexInfo)))
                        .collect(
                                Collectors.toUnmodifiableMap(
                                        Map.Entry::getKey, Map.Entry::getValue));
        return Optional.of(
                new GraphInfo(
                        name,
                        newVertexInfos,
                        edgeInfos,
                        prefix,
                        version,
                        newVertexType2VertexInfo,
                        edgeConcat2EdgeInfo));
    }

    public Optional<GraphInfo> addEdgeAsNew(EdgeInfo edgeInfo) {
        if (edgeInfo == null
                || hasEdgeInfo(
                edgeInfo.getSrcType(), edgeInfo.getEdgeType(), edgeInfo.getDstType())) {
            return Optional.empty();
        }
        List<EdgeInfo> newEdgeInfos =
                Stream.concat(edgeInfos.stream(), Stream.of(edgeInfo)).collect(Collectors.toList());
        Map<String, EdgeInfo> newEdgeConcat2EdgeInfo =
                Stream.concat(
                                edgeConcat2EdgeInfo.entrySet().stream(),
                                Stream.of(Map.entry(edgeInfo.getConcat(), edgeInfo)))
                        .collect(
                                Collectors.toUnmodifiableMap(
                                        Map.Entry::getKey, Map.Entry::getValue));
        return Optional.of(
                new GraphInfo(
                        name,
                        vertexInfos,
                        newEdgeInfos,
                        prefix,
                        version,
                        vertexType2VertexInfo,
                        newEdgeConcat2EdgeInfo));
    }

    public boolean hasVertexInfo(String type) {
        return vertexType2VertexInfo.containsKey(type);
    }

    public boolean hasEdgeInfo(String srcType, String edgeType, String dstType) {
        return edgeConcat2EdgeInfo.containsKey(srcType + dstType + edgeType);
    }

    public VertexInfo getVertexInfo(String type) {
        checkVertexExist(type);
        return vertexType2VertexInfo.get(type);
    }

    public EdgeInfo getEdgeInfo(String srcType, String edgeType, String dstType) {
        checkEdgeExist(srcType, edgeType, dstType);
        return edgeConcat2EdgeInfo.get(srcType + edgeType + dstType);
    }

    public int getVertexInfoNum() {
        return vertexInfos.size();
    }

    public int getEdgeInfoNum() {
        return edgeInfos.size();
    }

    public String getName() {
        return name;
    }

    public List<VertexInfo> getVertexInfos() {
        return vertexInfos;
    }

    public List<EdgeInfo> getEdgeInfos() {
        return edgeInfos;
    }

    public String getPrefix() {
        return prefix;
    }

    public VersionInfo getVersion() {
        return version;
    }

    private static List<VertexInfo> vertexFileNames2VertexInfos(
            List<String> vertexFileNames, Configuration conf) throws IOException {
        ArrayList<VertexInfo> tempVertices = new ArrayList<>(vertexFileNames.size());
        for (String vertexFileName : vertexFileNames) {
            tempVertices.add(VertexInfo.load(vertexFileName, conf));
        }
        return List.copyOf(tempVertices);
    }

    private static List<EdgeInfo> edgeFileNames2EdgeInfos(
            List<String> edgeFileNames, Configuration conf) throws IOException {
        ArrayList<EdgeInfo> tempEdges = new ArrayList<>(edgeFileNames.size());
        for (String edgeFileName : edgeFileNames) {
            tempEdges.add(EdgeInfo.load(edgeFileName, conf));
        }
        return List.copyOf(tempEdges);
    }

    private void checkVertexExist(String type) {
        if (!hasVertexInfo(type)) {
            throw new IllegalArgumentException(
                    "Vertex type " + type + " not exist in graph " + getName());
        }
    }

    private void checkEdgeExist(String srcType, String dstType, String edgeType) {
        if (!hasEdgeInfo(srcType, dstType, edgeType)) {
            throw new IllegalArgumentException(
                    "Edge type"
                            + srcType
                            + GeneralParams.regularSeparator
                            + GeneralParams.regularSeparator
                            + edgeType
                            + GeneralParams.regularSeparator
                            + dstType
                            + " not exist in graph "
                            + getName());
        }
    }
}
