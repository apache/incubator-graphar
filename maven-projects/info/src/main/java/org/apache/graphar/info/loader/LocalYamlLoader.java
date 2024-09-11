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

import java.io.IOException;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.info.VertexInfo;
import org.apache.graphar.info.yaml.EdgeYaml;
import org.apache.graphar.info.yaml.GraphYaml;
import org.apache.graphar.info.yaml.VertexYaml;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class LocalYamlLoader implements Loader {
    private static FileSystem fileSystem = null;

    public LocalYamlLoader() {}

    @Override
    public GraphInfo loadGraph(String path) throws IOException {
        checkFileSystem();
        FSDataInputStream inputStream = fileSystem.open(new Path(path));
        Yaml graphYamlLoader =
                new Yaml(new Constructor(GraphYaml.class, new LoaderOptions()));
        GraphYaml graphYaml = graphYamlLoader.load(inputStream);
        return graphYaml.toGraphInfo(this);
    }

    @Override
    public VertexInfo loadVertex(String path) throws IOException {
        checkFileSystem();
        FSDataInputStream inputStream = fileSystem.open(new Path(path));
        Yaml vertexYamlLoader =
                new Yaml(new Constructor(VertexYaml.class, new LoaderOptions()));
        VertexYaml vertexYaml = vertexYamlLoader.load(inputStream);
        return vertexYaml.toVertexInfo();
    }

    @Override
    public EdgeInfo loadEdge(String path) throws IOException {
        checkFileSystem();
        FSDataInputStream inputStream = fileSystem.open(new Path(path));
        Yaml edgeYamlLoader = new Yaml(new Constructor(EdgeYaml.class, new LoaderOptions()));
        EdgeYaml edgeYaml = edgeYamlLoader.load(inputStream);
        return edgeYaml.toEdgeInfo();
    }

    private static void checkFileSystem() throws IOException {
        if (fileSystem == null) {
            fileSystem = FileSystem.get(new Configuration());
        }
    }
}
