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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.info.VertexInfo;
import org.apache.hadoop.conf.Configuration;
import org.yaml.snakeyaml.DumperOptions;

public class GraphYaml {
    private String name;
    private String prefix;
    private List<String> vertices;
    private List<String> edges;
    private String version;
    private static final DumperOptions dumperOption;

    static {
        dumperOption = new DumperOptions();
        dumperOption.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        dumperOption.setIndent(4);
        dumperOption.setIndicatorIndent(2);
        dumperOption.setPrettyFlow(true);
    }

    public GraphYaml() {
        this.name = "";
        this.prefix = "";
        this.vertices = new ArrayList<>();
        this.edges = new ArrayList<>();
        this.version = "";
    }

    public GraphYaml(GraphInfo graphInfo) {
        this.name = graphInfo.getName();
        this.prefix = graphInfo.getPrefix();
        this.vertices =
                graphInfo.getVertexInfos().stream()
                        .map(vertexInfo -> vertexInfo.getType() + ".vertex.yaml")
                        .collect(Collectors.toList());
        this.edges =
                graphInfo.getEdgeInfos().stream()
                        .map(edgeInfo -> edgeInfo.getConcat() + ".edge.yaml")
                        .collect(Collectors.toList());
    }

    public GraphInfo toGraphInfo(Configuration conf) throws IOException {
        List<VertexInfo> vertexInfos = new ArrayList<>(vertices.size());
        for (String vertex : vertices) {
            vertexInfos.add(VertexInfo.load(vertex, conf));
        }
        List<EdgeInfo> edgeInfos = new ArrayList<>(edges.size());
        for (String edge : edges) {
            edgeInfos.add(EdgeInfo.load(edge, conf));
        }
        return new GraphInfo(
                name,
                vertexInfos,
                edgeInfos,
                prefix
        );
    }

    public static DumperOptions getDumperOptions() {
        return dumperOption;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public List<String> getVertices() {
        return vertices;
    }

    public void setVertices(List<String> vertices) {
        this.vertices = vertices;
    }

    public List<String> getEdges() {
        return edges;
    }

    public void setEdges(List<String> edges) {
        this.edges = edges;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}
