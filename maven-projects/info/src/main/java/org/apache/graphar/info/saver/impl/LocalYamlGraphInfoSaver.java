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

package org.apache.graphar.info.saver.impl;

import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.info.VertexInfo;
import org.apache.graphar.info.saver.GraphInfoSaver;

public class LocalYamlGraphInfoSaver implements GraphInfoSaver {

    public Writer writeYaml(URI uri) throws IOException {
        Path outputPath = FileSystems.getDefault().getPath(uri.toString());
        Files.createDirectories(outputPath.getParent());
        Files.createFile(outputPath);
        return Files.newBufferedWriter(outputPath);
    }

    @Override
    public void save(URI graphInfoUri, GraphInfo graphInfo) throws IOException {
        Writer writer = writeYaml(graphInfoUri.resolve(graphInfo.getName() + ".graph.yaml"));
        writer.write(graphInfo.dump(graphInfoUri));
        writer.close();

        for (VertexInfo vertexInfo : graphInfo.getVertexInfos()) {
            URI saveUri = graphInfoUri.resolve(graphInfo.getStoreUri(vertexInfo));
            save(saveUri, vertexInfo);
        }
        for (EdgeInfo edgeInfo : graphInfo.getEdgeInfos()) {
            URI saveUri = graphInfoUri.resolve(graphInfo.getStoreUri(edgeInfo));
            save(saveUri, edgeInfo);
        }
    }

    @Override
    public void save(URI vertexInfoUri, VertexInfo vertexInfo) throws IOException {
        Writer vertexWriter = writeYaml(vertexInfoUri);
        vertexWriter.write(vertexInfo.dump());
        vertexWriter.close();
    }

    @Override
    public void save(URI edgeInfoUri, EdgeInfo edgeInfo) throws IOException {
        Writer edgeWriter = writeYaml(edgeInfoUri);
        edgeWriter.write(edgeInfo.dump());
        edgeWriter.close();
    }
}
