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

import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.info.VertexInfo;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

public class LocalYamlGraphSaver implements GraphSaver {

    @Override
    public void save(String path, GraphInfo graphInfo) throws IOException {
        final Path outputPath = FileSystems
            .getDefault()
            .getPath(path + FileSystems.getDefault().getSeparator() + graphInfo.getName() + ".graph.yaml");
        Files.createDirectories(outputPath.getParent());
        Files.createFile(outputPath);
        final BufferedWriter writer = Files.newBufferedWriter(outputPath);
        writer.write(graphInfo.dump());
        writer.close();

        for (VertexInfo vertexInfo : graphInfo.getVertexInfos()) {
            saveVertex(path, vertexInfo);
        }
        for (EdgeInfo edgeInfo : graphInfo.getEdgeInfos()) {
            saveEdge(path, edgeInfo);
        }
    }

    private void saveVertex(String path, VertexInfo vertexInfo) throws IOException {
        final Path outputPath = FileSystems
            .getDefault()
            .getPath(path + FileSystems.getDefault().getSeparator() + vertexInfo.getType() + ".vertex.yaml");
        Files.createDirectories(outputPath.getParent());
        Files.createFile(outputPath);
        final BufferedWriter writer = Files.newBufferedWriter(outputPath);
        writer.write(vertexInfo.dump());
        writer.close();
    }

    private void saveEdge(String path, EdgeInfo edgeInfo) throws IOException {
        final Path outputPath = FileSystems
            .getDefault()
            .getPath(path + FileSystems.getDefault().getSeparator() + edgeInfo.getConcat() + ".edge.yaml");
        Files.createDirectories(outputPath.getParent());
        Files.createFile(outputPath);
        final BufferedWriter writer = Files.newBufferedWriter(outputPath);
        writer.write(edgeInfo.dump());
        writer.close();
    }
}
