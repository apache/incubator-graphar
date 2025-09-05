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

package org.apache.graphar.info.loader;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.info.VertexInfo;
import org.apache.graphar.info.yaml.EdgeYaml;
import org.apache.graphar.info.yaml.GraphYaml;
import org.apache.graphar.info.yaml.VertexYaml;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public abstract class StreamGraphInfoLoader extends BaseGraphInfoLoader {

    public abstract InputStream readYaml(URI uri) throws IOException;

    @Override
    public GraphInfo loadGraphInfo(URI graphYamluri) throws IOException {
        // load graph itself
        InputStream yaml = readYaml(graphYamluri);
        Yaml GraphYamlLoader = new Yaml(new Constructor(GraphYaml.class, new LoaderOptions()));
        GraphYaml graphYaml = GraphYamlLoader.load(yaml);
        return buildGraphInfoFromGraphYaml(graphYamluri, graphYaml);
    }

    @Override
    public VertexInfo loadVertexInfo(URI vertexYamlUri) throws IOException {
        InputStream yaml = readYaml(vertexYamlUri);
        Yaml edgeYamlLoader = new Yaml(new Constructor(VertexYaml.class, new LoaderOptions()));
        VertexYaml edgeYaml = edgeYamlLoader.load(yaml);
        return buildVertexInfoFromGraphYaml(edgeYaml);
    }

    @Override
    public EdgeInfo loadEdgeInfo(URI edgeYamlUri) throws IOException {
        InputStream yaml = readYaml(edgeYamlUri);
        Yaml edgeYamlLoader = new Yaml(new Constructor(EdgeYaml.class, new LoaderOptions()));
        EdgeYaml edgeYaml = edgeYamlLoader.load(yaml);
        return buildEdgeInfoFromGraphYaml(edgeYaml);
    }
}
