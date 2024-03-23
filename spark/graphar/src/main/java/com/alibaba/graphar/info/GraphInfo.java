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

import com.alibaba.graphar.info.yaml.GraphYamlParser;
import com.alibaba.graphar.util.GeneralParams;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class GraphInfo {
    private final String name;
    private final ImmutableList<VertexInfo> vertexInfos;
    private final ImmutableList<EdgeInfo> edgeInfos;
    private final String prefix;
    private final ImmutableMap<String, VertexInfo> vertexLabel2VertexInfo;
    private final ImmutableMap<String, EdgeInfo> edgeConcat2EdgeInfo;
    private final String version;

    public GraphInfo(
            String name,
            List<VertexInfo> vertexInfos,
            List<EdgeInfo> edgeInfos,
            String prefix,
            String version) {
        this.name = name;
        this.vertexInfos =
                vertexInfos instanceof ImmutableList
                        ? (ImmutableList<VertexInfo>) vertexInfos
                        : ImmutableList.copyOf(vertexInfos);
        this.edgeInfos =
                edgeInfos instanceof ImmutableList
                        ? (ImmutableList<EdgeInfo>) edgeInfos
                        : ImmutableList.copyOf(edgeInfos);
        this.prefix = prefix;
        this.version = version;
        this.vertexLabel2VertexInfo =
                vertexInfos.stream()
                        .collect(
                                ImmutableMap.toImmutableMap(
                                        VertexInfo::getLabel, Function.identity()));
        this.edgeConcat2EdgeInfo =
                edgeInfos.stream()
                        .collect(
                                ImmutableMap.toImmutableMap(
                                        EdgeInfo::getConcat, Function.identity()));
    }

    private GraphInfo(GraphYamlParser graphYaml, Configuration conf) throws IOException {
        this(
                graphYaml.getName(),
                vertexFileNames2VertexInfos(graphYaml.getVertices(), conf),
                edgeFileNames2EdgeInfos(graphYaml.getEdges(), conf),
                graphYaml.getPrefix(),
                graphYaml.getVersion());
    }

    private GraphInfo(
            String name,
            ImmutableList<VertexInfo> vertexInfos,
            ImmutableList<EdgeInfo> edgeInfos,
            String prefix,
            ImmutableMap<String, VertexInfo> vertexLabel2VertexInfo,
            ImmutableMap<String, EdgeInfo> edgeConcat2EdgeInfo,
            String version) {
        this.name = name;
        this.vertexInfos = vertexInfos;
        this.edgeInfos = edgeInfos;
        this.prefix = prefix;
        this.vertexLabel2VertexInfo = vertexLabel2VertexInfo;
        this.edgeConcat2EdgeInfo = edgeConcat2EdgeInfo;
        this.version = version;
    }

    public static GraphInfo load(String graphPath) throws IOException {
        return load(graphPath, null);
    }

    public static GraphInfo load(String graphPath, Configuration conf) throws IOException {
        if (graphPath == null) {
            conf = new Configuration();
        }
        Path path = new Path(graphPath);
        FileSystem fileSystem = path.getFileSystem(conf);
        FSDataInputStream inputStream = fileSystem.open(path);
        Yaml graphYamlLoader =
                new Yaml(new Constructor(GraphYamlParser.class, new LoaderOptions()));
        GraphYamlParser graphYaml = graphYamlLoader.load(inputStream);
        return new GraphInfo(graphYaml, conf);
    }

    // TODO(@Thespica): Implement save and dump methods
    //    public void save(String path) {
    //
    //    }
    //
    //    public String dump() {
    //
    //    }

    Optional<GraphInfo> addVertexAsNew(VertexInfo vertexInfo) {
        if (vertexInfo == null || hasVertexInfo(vertexInfo.getLabel())) {
            return Optional.empty();
        }
        ImmutableList<VertexInfo> newVertexInfos =
                ImmutableList.<VertexInfo>builder().addAll(vertexInfos).add(vertexInfo).build();
        ImmutableMap<String, VertexInfo> newVertexLabel2VertexInfo =
                ImmutableMap.<String, VertexInfo>builder()
                        .putAll(vertexLabel2VertexInfo)
                        .put(vertexInfo.getLabel(), vertexInfo)
                        .build();
        return Optional.of(
                new GraphInfo(
                        name,
                        newVertexInfos,
                        edgeInfos,
                        prefix,
                        newVertexLabel2VertexInfo,
                        edgeConcat2EdgeInfo,
                        version));
    }

    Optional<GraphInfo> addEdgeAsNew(EdgeInfo edgeInfo) {
        if (edgeInfo == null
                || hasEdgeInfo(
                        edgeInfo.getSrcLabel(), edgeInfo.getEdgeLabel(), edgeInfo.getDstLabel())) {
            return Optional.empty();
        }
        ImmutableList<EdgeInfo> newEdgeInfos =
                ImmutableList.<EdgeInfo>builder().addAll(edgeInfos).add(edgeInfo).build();
        ImmutableMap<String, EdgeInfo> newEdgeConcat2EdgeInfo =
                ImmutableMap.<String, EdgeInfo>builder()
                        .putAll(edgeConcat2EdgeInfo)
                        .put(edgeInfo.getConcat(), edgeInfo)
                        .build();
        return Optional.of(
                new GraphInfo(
                        name,
                        vertexInfos,
                        newEdgeInfos,
                        prefix,
                        vertexLabel2VertexInfo,
                        newEdgeConcat2EdgeInfo,
                        version));
    }

    public boolean hasVertexInfo(String label) {
        return vertexLabel2VertexInfo.containsKey(label);
    }

    public boolean hasEdgeInfo(String srcLabel, String edgeLabel, String dstLabel) {
        return edgeConcat2EdgeInfo.containsKey(srcLabel + dstLabel + edgeLabel);
    }

    public VertexInfo getVertexInfo(String label) {
        checkVertexExist(label);
        return vertexLabel2VertexInfo.get(label);
    }

    public EdgeInfo getEdgeInfo(String srcLabel, String edgeLabel, String dstLabel) {
        checkEdgeExist(srcLabel, edgeLabel, dstLabel);
        return edgeConcat2EdgeInfo.get(srcLabel + edgeLabel + dstLabel);
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

    public ImmutableList<VertexInfo> getVertexInfos() {
        return vertexInfos;
    }

    public ImmutableList<EdgeInfo> getEdgeInfos() {
        return edgeInfos;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getVersion() {
        return version;
    }

    private static ImmutableList<VertexInfo> vertexFileNames2VertexInfos(
            List<String> vertexFileNames, Configuration conf) throws IOException {
        ImmutableList.Builder<VertexInfo> verticesBuilder = ImmutableList.builder();
        for (String vertexFileName : vertexFileNames) {
            verticesBuilder.add(VertexInfo.load(vertexFileName, conf));
        }
        return verticesBuilder.build();
    }

    private static ImmutableList<EdgeInfo> edgeFileNames2EdgeInfos(
            List<String> edgeFileNames, Configuration conf) throws IOException {
        ImmutableList.Builder<EdgeInfo> edgesBuilder = ImmutableList.builder();
        for (String edgeFileName : edgeFileNames) {
            edgesBuilder.add(EdgeInfo.load(edgeFileName, conf));
        }
        return edgesBuilder.build();
    }

    private void checkVertexExist(String label) {
        if (!hasVertexInfo(label)) {
            throw new IllegalArgumentException(
                    "Vertex label " + label + " not exist in graph " + getName());
        }
    }

    private void checkEdgeExist(String srcLabel, String dstLabel, String edgeLabel) {
        if (!hasEdgeInfo(srcLabel, dstLabel, edgeLabel)) {
            throw new IllegalArgumentException(
                    "Edge label"
                            + srcLabel
                            + GeneralParams.regularSeparator
                            + GeneralParams.regularSeparator
                            + edgeLabel
                            + GeneralParams.regularSeparator
                            + dstLabel
                            + " not exist in graph "
                            + getName());
        }
    }
}
