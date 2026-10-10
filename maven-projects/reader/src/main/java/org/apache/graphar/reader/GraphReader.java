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

package org.apache.graphar.reader;

import java.io.IOException;
import java.net.URI;
import java.util.Objects;
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.info.loader.GraphInfoLoader;
import org.apache.graphar.io.PhysicalReader;
import org.apache.graphar.storage.Storage;

/**
 * Entry point for reading the topology and edge properties of a GraphAr dataset without JNI.
 * Metadata comes from {@code graphar-info}, chunk arithmetic from {@code graphar-core}, GraphAr
 * control files from a {@link Storage}, and every data chunk from a format-specific {@link
 * PhysicalReader}.
 */
public final class GraphReader {
    private final GraphInfo graphInfo;
    private final URI datasetRoot;
    private final Storage storage;
    private final PhysicalReader physicalReader;

    /**
     * Opens a graph whose metadata is already loaded.
     *
     * @param graphInfo the graph metadata
     * @param datasetRoot the directory that relative chunk paths resolve against
     * @param storage the storage that holds the GraphAr control files
     * @param physicalReader the reader for adjacency, offset and edge property chunks
     */
    public GraphReader(
            GraphInfo graphInfo, URI datasetRoot, Storage storage, PhysicalReader physicalReader) {
        this.graphInfo = Objects.requireNonNull(graphInfo, "Graph info cannot be null.");
        this.datasetRoot = DatasetUris.directory(datasetRoot);
        this.storage = Objects.requireNonNull(storage, "Storage cannot be null.");
        this.physicalReader =
                Objects.requireNonNull(physicalReader, "Physical reader cannot be null.");
    }

    /**
     * Loads graph metadata and opens the graph rooted at its declared base URI.
     *
     * @param graphYamlUri the graph YAML file
     * @param loader the metadata loader
     * @param storage the storage that holds the GraphAr control files
     * @param physicalReader the reader for adjacency, offset and edge property chunks
     * @return the opened graph
     * @throws IOException if the metadata cannot be loaded
     */
    public static GraphReader open(
            URI graphYamlUri,
            GraphInfoLoader loader,
            Storage storage,
            PhysicalReader physicalReader)
            throws IOException {
        Objects.requireNonNull(graphYamlUri, "Graph YAML URI cannot be null.");
        GraphInfo loaded =
                Objects.requireNonNull(loader, "Graph info loader cannot be null.")
                        .loadGraphInfo(graphYamlUri);
        return new GraphReader(loaded, loaded.getBaseUri(), storage, physicalReader);
    }

    /**
     * Returns the metadata that defines this graph.
     *
     * @return the graph metadata
     */
    public GraphInfo graphInfo() {
        return graphInfo;
    }

    /**
     * Opens the {@code ordered_by_source} adjacency of one declared edge type.
     *
     * @param srcType the source vertex type
     * @param edgeType the edge type
     * @param dstType the destination vertex type
     * @return the edge reader
     */
    public OrderedSourceEdgeReader edge(String srcType, String edgeType, String dstType) {
        return new OrderedSourceEdgeReader(
                graphInfo.getEdgeInfo(srcType, edgeType, dstType),
                datasetRoot,
                storage,
                physicalReader);
    }
}
