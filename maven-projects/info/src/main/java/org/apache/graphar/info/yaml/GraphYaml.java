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

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.info.VersionInfo;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

public class GraphYaml {
    private String name;
    private String prefix;
    private List<String> vertices;
    private List<String> edges;
    private String version;
    private static final DumperOptions dumperOption;
    private static Representer representer;

    static {
        dumperOption = new DumperOptions();
        dumperOption.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        dumperOption.setIndent(4);
        dumperOption.setIndicatorIndent(2);
        dumperOption.setPrettyFlow(true);
        representer =
                new Representer(dumperOption) {
                    @Override
                    protected NodeTuple representJavaBeanProperty(
                            Object javaBean,
                            Property property,
                            Object propertyValue,
                            Tag customTag) {
                        // if value of property is null, ignore it.
                        if (propertyValue == null) {
                            return null;
                        } else {
                            return super.representJavaBeanProperty(
                                    javaBean, property, propertyValue, customTag);
                        }
                    }
                };
        representer.addClassTag(GraphYaml.class, Tag.MAP);
        representer.addClassTag(VertexYaml.class, Tag.MAP);
        representer.addClassTag(EdgeYaml.class, Tag.MAP);
    }

    public GraphYaml() {
        this.name = "";
        this.prefix = "";
        this.vertices = new ArrayList<>();
        this.edges = new ArrayList<>();
        this.version = "";
    }

    public GraphYaml(GraphInfo graphInfo) {
        this(null, graphInfo);
    }

    public GraphYaml(URI graphInfoStoreUri, GraphInfo graphInfo) {
        this.name = graphInfo.getName();
        this.prefix = graphInfo.getPrefix();
        this.version =
                Optional.of(graphInfo)
                        .map(GraphInfo::getVersion)
                        .map(VersionInfo::toString)
                        .orElse(null);
        this.vertices =
                graphInfo.getVertexInfos().stream()
                        .map(
                                vertexInfo -> {
                                    URI storeUri = graphInfo.getStoreUri(vertexInfo);
                                    if (graphInfoStoreUri != null) {
                                        storeUri =
                                                graphInfoStoreUri.resolve(".").relativize(storeUri);
                                    }
                                    return storeUri.toString();
                                })
                        .collect(Collectors.toList());
        this.edges =
                graphInfo.getEdgeInfos().stream()
                        .map(
                                edgeInfo -> {
                                    URI storeUri = graphInfo.getStoreUri(edgeInfo);
                                    if (graphInfoStoreUri != null) {
                                        storeUri =
                                                graphInfoStoreUri.resolve(".").relativize(storeUri);
                                    }
                                    return storeUri.toString();
                                })
                        .collect(Collectors.toList());
        this.version =
                Optional.of(graphInfo)
                        .map(GraphInfo::getVersion)
                        .map(VersionInfo::toString)
                        .orElse(null);
    }

    public static DumperOptions getDumperOptions() {
        return dumperOption;
    }

    public static Representer getRepresenter() {
        return representer;
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
