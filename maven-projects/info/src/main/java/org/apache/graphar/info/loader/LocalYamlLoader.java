/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.graphar.info.loader;

import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.info.VertexInfo;
import org.apache.graphar.info.yaml.EdgeYamlParser;
import org.apache.graphar.info.yaml.GraphYamlParser;
import org.apache.graphar.info.yaml.VertexYamlParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.IOException;

public class LocalYamlLoader implements Loader {
    private static final FileSystem fileSystem;

    static {
        try {
            fileSystem = FileSystem.get(new Configuration());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public LocalYamlLoader() {}

    @Override
    public GraphInfo loadGraph(String path) throws IOException {
        FSDataInputStream inputStream = fileSystem.open(new Path(path));
        Yaml graphYamlLoader =
                new Yaml(new Constructor(GraphYamlParser.class, new LoaderOptions()));
        GraphYamlParser graphYaml = graphYamlLoader.load(inputStream);
        return graphYaml.toGraphInfo(this);
    }

    @Override
    public VertexInfo loadVertex(String path) throws IOException {
        FSDataInputStream inputStream = fileSystem.open(new Path(path));
        Yaml vertexYamlLoader =
                new Yaml(new Constructor(VertexYamlParser.class, new LoaderOptions()));
        VertexYamlParser vertexYaml = vertexYamlLoader.load(inputStream);
        return vertexYaml.toVertexInfo();
    }

    @Override
    public EdgeInfo loadEdge(String path) throws IOException {
        FSDataInputStream inputStream = fileSystem.open(new Path(path));
        Yaml edgeYamlLoader =
                new Yaml(new Constructor(EdgeYamlParser.class, new LoaderOptions()));
        EdgeYamlParser edgeYaml = edgeYamlLoader.load(inputStream);
        return edgeYaml.toEdgeInfo();
    }
}
