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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.apache.graphar.core.ChunkMath;
import org.apache.graphar.core.ChunkRange;
import org.apache.graphar.core.EdgeRange;
import org.apache.graphar.core.OffsetLocation;
import org.apache.graphar.core.OrderedAdjacencyResolver;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.type.AdjListType;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.ColumnRef;
import org.apache.graphar.io.PhysicalReader;
import org.apache.graphar.io.Projection;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.RowRange;
import org.apache.graphar.storage.Storage;

/**
 * Reads one GraphAr edge type through its {@code ordered_by_source} adjacency list.
 *
 * <p>The outgoing edges of a vertex are found through its offset chunk: the two offsets that bound
 * the vertex give a half-open row range of its partition, which is then requested from only the
 * adjacency and property chunks that range touches.
 */
public final class OrderedSourceEdgeReader {
    private static final AdjListType LAYOUT = AdjListType.ordered_by_source;
    private static final String OFFSET_COLUMN = "_graphArOffset";

    private final EdgeInfo edgeInfo;
    private final URI datasetRoot;
    private final Storage storage;
    private final PhysicalReader physicalReader;
    private final OrderedAdjacencyResolver resolver;

    OrderedSourceEdgeReader(
            EdgeInfo edgeInfo, URI datasetRoot, Storage storage, PhysicalReader physicalReader) {
        this.edgeInfo = Objects.requireNonNull(edgeInfo, "Edge info cannot be null.");
        this.datasetRoot = DatasetUris.directory(datasetRoot);
        this.storage = Objects.requireNonNull(storage, "Storage cannot be null.");
        this.physicalReader =
                Objects.requireNonNull(physicalReader, "Physical reader cannot be null.");
        this.resolver = new OrderedAdjacencyResolver(edgeInfo, LAYOUT);
    }

    /**
     * Returns the metadata that defines this edge type.
     *
     * @return the edge metadata
     */
    public EdgeInfo edgeInfo() {
        return edgeInfo;
    }

    /**
     * Returns the number of source vertices recorded for this adjacency list.
     *
     * @return the source vertex count
     * @throws IOException if the count file cannot be read
     */
    public long vertexCount() throws IOException {
        return ControlFileReader.readNonNegativeLong(
                storage, DatasetUris.resolve(datasetRoot, edgeInfo.getVerticesNumFileUri(LAYOUT)));
    }

    /**
     * Returns the number of edges, summed over the edge count file of every partition.
     *
     * @return the edge count
     * @throws IOException if a count file cannot be read
     */
    public long edgeCount() throws IOException {
        long total = 0;
        for (long count : partitionEdgeCounts()) {
            total = Math.addExact(total, count);
        }
        return total;
    }

    /**
     * Opens a cursor over the destinations of one source vertex, in adjacency order.
     *
     * @param sourceId the source vertex ID
     * @return an edge cursor without edge properties
     * @throws IOException if the offset chunk cannot be read
     */
    public EdgeCursor neighbors(long sourceId) throws IOException {
        return edges(sourceId, List.of());
    }

    /**
     * Opens a cursor over the outgoing edges of one source vertex with the requested properties.
     *
     * @param sourceId the source vertex ID
     * @param properties the edge property names to read; may be empty
     * @return the edge cursor
     * @throws IOException if the offset chunk cannot be read
     */
    public EdgeCursor edges(long sourceId, Collection<String> properties) throws IOException {
        List<PropertySelection> selections =
                PropertySelection.select(edgeInfo.getPropertyGroups(), properties);
        OffsetLocation location = resolver.locate(sourceId);
        EdgeRange range = readOffsetPair(location);
        List<EdgeCursor.Segment> segments = new ArrayList<>();
        addSegments(segments, location.vertexChunkIndex(), range);
        return new EdgeCursor(
                edgeInfo, datasetRoot, physicalReader, segments, sourceId, selections);
    }

    /**
     * Opens a cursor over every edge in adjacency order, without edge properties.
     *
     * @return the edge cursor
     * @throws IOException if a count file cannot be read
     */
    public EdgeCursor scanEdges() throws IOException {
        return scanEdges(List.of());
    }

    /**
     * Opens a cursor over every edge in adjacency order with the requested properties. Chunks are
     * opened one at a time as the cursor advances.
     *
     * @param properties the edge property names to read; may be empty
     * @return the edge cursor
     * @throws IOException if a count file cannot be read
     */
    public EdgeCursor scanEdges(Collection<String> properties) throws IOException {
        List<PropertySelection> selections =
                PropertySelection.select(edgeInfo.getPropertyGroups(), properties);
        long[] counts = partitionEdgeCounts();
        List<EdgeCursor.Segment> segments = new ArrayList<>();
        for (int partition = 0; partition < counts.length; partition++) {
            addSegments(segments, partition, EdgeRange.fromOffsets(0, counts[partition]));
        }
        return new EdgeCursor(edgeInfo, datasetRoot, physicalReader, segments, null, selections);
    }

    private void addSegments(List<EdgeCursor.Segment> segments, long partition, EdgeRange range) {
        long chunkSize = edgeInfo.getChunkSize();
        ChunkRange chunks = range.edgeChunks(chunkSize);
        for (long chunk = chunks.begin(); chunk < chunks.end(); chunk++) {
            long chunkStart = Math.multiplyExact(chunk, chunkSize);
            long start = Math.max(range.begin(), chunkStart) - chunkStart;
            long end = Math.min(range.end() - chunkStart, chunkSize);
            segments.add(new EdgeCursor.Segment(partition, chunk, new RowRange(start, end)));
        }
    }

    private EdgeRange readOffsetPair(OffsetLocation location) throws IOException {
        URI uri = DatasetUris.resolve(datasetRoot, location.offsetChunkUri());
        ReadRequest request =
                ReadRequest.builder(uri)
                        .projection(Projection.of(ColumnRef.of(OFFSET_COLUMN)))
                        .rowRange(
                                new RowRange(
                                        location.offsetIndex(),
                                        Math.addExact(location.offsetIndex(), 2)))
                        .build();
        long[] pair = new long[2];
        int count = 0;
        try (BatchCursor cursor = physicalReader.read(request).cursor()) {
            while (cursor.next()) {
                RecordBatch batch = cursor.batch();
                for (int row = 0; row < batch.rowCount(); row++) {
                    Object value = batch.column(0).getObject(row);
                    if (!(value instanceof Long)) {
                        throw new IllegalArgumentException(
                                "GraphAr offset chunk " + uri + " must hold INT64 offsets.");
                    }
                    if (count == 2) {
                        throw new IllegalArgumentException(
                                "GraphAr offset chunk "
                                        + uri
                                        + " returned rows outside its range.");
                    }
                    pair[count++] = (Long) value;
                }
            }
        }
        if (count != 2) {
            throw new IllegalArgumentException(
                    "Source vertex " + location.vertexId() + " has no offset pair in " + uri);
        }
        return EdgeRange.fromOffsets(pair[0], pair[1]);
    }

    private long[] partitionEdgeCounts() throws IOException {
        long partitions = ChunkMath.chunkCount(vertexCount(), edgeInfo.getSrcChunkSize());
        long[] counts = new long[Math.toIntExact(partitions)];
        for (int partition = 0; partition < counts.length; partition++) {
            counts[partition] =
                    ControlFileReader.readNonNegativeLong(
                            storage,
                            DatasetUris.resolve(
                                    datasetRoot, edgeInfo.getEdgesNumFileUri(LAYOUT, partition)));
        }
        return counts;
    }
}
