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

package org.apache.graphar.info.saver;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.info.VertexInfo;
import org.apache.graphar.util.PathUtil;

public class GraphSaver {
    public static void save(String path, GraphInfo graphInfo) throws IOException {
        save(
                path,
                graphInfo,
                (filePath, yamlString) -> {
                    Path outputPath = Path.of(filePath);
                    Files.createDirectories(outputPath.getParent());
                    Files.createFile(outputPath);
                    final BufferedWriter writer = Files.newBufferedWriter(outputPath);
                    writer.write(yamlString);
                    writer.close();
                });
    }

    public static void save(String path, GraphInfo graphInfo, YamlSaver yamlSaver)
            throws IOException {
        saveGraph(path, graphInfo, yamlSaver);
        for (VertexInfo vertexInfo : graphInfo.getVertexInfos()) {
            saveVertex(path, vertexInfo, yamlSaver);
        }
        for (EdgeInfo edgeInfo : graphInfo.getEdgeInfos()) {
            saveEdge(path, edgeInfo, yamlSaver);
        }
    }

    private static void saveGraph(String path, GraphInfo graphInfo, YamlSaver yamlSaver)
            throws IOException {
        yamlSaver.saveYaml(
                PathUtil.resolvePath(path, graphInfo.getName() + ".graph.yaml"), graphInfo.dump());
    }

    private static void saveVertex(String path, VertexInfo vertexInfo, YamlSaver yamlSaver)
            throws IOException {
        yamlSaver.saveYaml(
                PathUtil.resolvePath(path, vertexInfo.getType() + ".vertex.yaml"),
                vertexInfo.dump());
    }

    private static void saveEdge(String path, EdgeInfo edgeInfo, YamlSaver yamlSaver)
            throws IOException {
        yamlSaver.saveYaml(
                PathUtil.resolvePath(path, edgeInfo.getConcat() + ".edge.yaml"), edgeInfo.dump());
    }
}
