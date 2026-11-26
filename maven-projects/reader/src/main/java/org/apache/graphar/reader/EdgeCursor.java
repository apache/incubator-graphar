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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.type.AdjListType;
import org.apache.graphar.io.PhysicalReader;
import org.apache.graphar.io.ReadReport;
import org.apache.graphar.io.RowRange;

/**
 * A closeable cursor over {@code ordered_by_source} topology rows. Each row is joined with the same
 * row of the selected edge property chunks, which GraphAr aligns with the adjacency list.
 */
public final class EdgeCursor implements AutoCloseable {
    static final String SOURCE_COLUMN = "_graphArSrcIndex";
    static final String DESTINATION_COLUMN = "_graphArDstIndex";

    private final EdgeInfo edgeInfo;
    private final URI datasetRoot;
    private final PhysicalReader physicalReader;
    private final List<Segment> segments;
    private final Long source;
    private final List<PropertySelection> selections;
    private final List<ReadReport> reports = new ArrayList<>();
    private int segmentIndex;
    private AlignedRows rows;
    private GraphEdge current;
    private boolean closed;

    EdgeCursor(
            EdgeInfo edgeInfo,
            URI datasetRoot,
            PhysicalReader physicalReader,
            List<Segment> segments,
            Long source,
            List<PropertySelection> selections) {
        this.edgeInfo = edgeInfo;
        this.datasetRoot = datasetRoot;
        this.physicalReader = physicalReader;
        this.segments = List.copyOf(segments);
        this.source = source;
        this.selections = selections;
    }

    /**
     * Advances to the next edge.
     *
     * @return whether an edge is available
     * @throws IOException if a chunk cannot be read
     */
    public boolean next() throws IOException {
        current = null;
        while (!closed) {
            if (rows == null) {
                if (segmentIndex == segments.size()) {
                    close();
                    return false;
                }
                rows = open(segments.get(segmentIndex++));
                continue;
            }
            if (!rows.next()) {
                AlignedRows finished = rows;
                rows = null;
                finished.close();
                continue;
            }
            long edgeSource = source != null ? source : id(rows.value(0, 0), "source");
            long destination = id(rows.value(0, source != null ? 0 : 1), "destination");
            Map<String, Object> properties = new LinkedHashMap<>();
            for (int index = 0; index < selections.size(); index++) {
                List<String> names = selections.get(index).names;
                for (int column = 0; column < names.size(); column++) {
                    properties.put(names.get(column), rows.value(index + 1, column));
                }
            }
            current = new GraphEdge(edgeSource, destination, properties);
            return true;
        }
        return false;
    }

    /**
     * Returns the current edge after {@link #next()} returns {@code true}.
     *
     * @return the current edge
     */
    public GraphEdge edge() {
        if (current == null) {
            throw new IllegalStateException("No current edge. Call next() first.");
        }
        return current;
    }

    /**
     * Returns the physical read reports of the chunks opened so far, in request order.
     *
     * @return the read reports
     */
    public List<ReadReport> reports() {
        return List.copyOf(reports);
    }

    @Override
    public void close() throws IOException {
        closed = true;
        current = null;
        if (rows != null) {
            AlignedRows open = rows;
            rows = null;
            open.close();
        }
    }

    private AlignedRows open(Segment segment) throws IOException {
        List<URI> uris = new ArrayList<>();
        List<List<String>> columns = new ArrayList<>();
        uris.add(
                DatasetUris.resolve(
                        datasetRoot,
                        edgeInfo.getAdjacentListChunkUri(
                                AdjListType.ordered_by_source,
                                segment.vertexChunk,
                                segment.edgeChunk)));
        columns.add(
                source != null
                        ? List.of(DESTINATION_COLUMN)
                        : List.of(SOURCE_COLUMN, DESTINATION_COLUMN));
        for (PropertySelection selection : selections) {
            uris.add(
                    DatasetUris.resolve(
                            datasetRoot,
                            edgeInfo.getPropertyGroupChunkUri(
                                    selection.group,
                                    AdjListType.ordered_by_source,
                                    segment.vertexChunk,
                                    segment.edgeChunk)));
            columns.add(selection.names);
        }
        return AlignedRows.open(physicalReader, uris, columns, segment.range, reports);
    }

    private static long id(Object value, String kind) {
        if (!(value instanceof Long) || (Long) value < 0) {
            throw new IllegalArgumentException(
                    "GraphAr " + kind + " IDs must be non-negative INT64 values.");
        }
        return (Long) value;
    }

    static final class Segment {
        private final long vertexChunk;
        private final long edgeChunk;
        private final RowRange range;

        Segment(long vertexChunk, long edgeChunk, RowRange range) {
            this.vertexChunk = vertexChunk;
            this.edgeChunk = edgeChunk;
            this.range = range;
        }
    }
}
