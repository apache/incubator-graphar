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

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.ColumnRef;
import org.apache.graphar.io.PhysicalReader;
import org.apache.graphar.io.Projection;
import org.apache.graphar.io.ReadReport;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.ReadResult;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.RowRange;

/**
 * The same row range of several GraphAr chunks that GraphAr aligns by row position, such as a
 * topology chunk and its edge property chunks. The chunks are consumed in lockstep and must hold
 * exactly the rows of the range.
 */
final class AlignedRows implements Closeable {
    private final List<URI> uris;
    private final List<Stream> streams;
    private final long expectedRows;
    private long rows;

    private AlignedRows(List<URI> uris, List<Stream> streams, long expectedRows) {
        this.uris = uris;
        this.streams = streams;
        this.expectedRows = expectedRows;
    }

    /** Requests {@code range} of every chunk, projected to its columns, and records each report. */
    static AlignedRows open(
            PhysicalReader reader,
            List<URI> uris,
            List<List<String>> columns,
            RowRange range,
            List<ReadReport> reports)
            throws IOException {
        List<Stream> streams = new ArrayList<>(uris.size());
        try {
            for (int index = 0; index < uris.size(); index++) {
                ReadResult result =
                        reader.read(
                                ReadRequest.builder(uris.get(index))
                                        .projection(
                                                Projection.of(
                                                        columns.get(index).stream()
                                                                .map(ColumnRef::of)
                                                                .collect(Collectors.toList())))
                                        .rowRange(range)
                                        .build());
                reports.add(result.report());
                streams.add(new Stream(result.cursor()));
            }
        } catch (IOException | RuntimeException failure) {
            closeAll(streams, failure);
            throw failure;
        }
        return new AlignedRows(
                List.copyOf(uris), streams, range.endExclusive() - range.startInclusive());
    }

    /** Advances every chunk by one row, failing if they disagree on where the range ends. */
    boolean next() throws IOException {
        boolean advanced = streams.get(0).next();
        for (int index = 1; index < streams.size(); index++) {
            if (streams.get(index).next() != advanced) {
                throw new IllegalArgumentException(
                        "GraphAr chunk "
                                + uris.get(index)
                                + " is not row-aligned with "
                                + uris.get(0));
            }
        }
        if (advanced ? ++rows > expectedRows : rows != expectedRows) {
            throw new IllegalArgumentException(
                    "GraphAr chunk "
                            + uris.get(0)
                            + " returned a different row count than its range: expected "
                            + expectedRows);
        }
        return advanced;
    }

    /** Returns a value of the current row from one chunk's projected columns. */
    Object value(int chunk, int column) {
        return streams.get(chunk).value(column);
    }

    @Override
    public void close() throws IOException {
        IOException failure = closeAll(streams, null);
        if (failure != null) {
            throw failure;
        }
    }

    private static IOException closeAll(List<Stream> streams, Exception primary) {
        IOException failure = null;
        for (Stream stream : streams) {
            try {
                stream.cursor.close();
            } catch (IOException exception) {
                if (primary != null) {
                    primary.addSuppressed(exception);
                } else if (failure == null) {
                    failure = exception;
                } else {
                    failure.addSuppressed(exception);
                }
            }
        }
        return failure;
    }

    private static final class Stream {
        private final BatchCursor cursor;
        private RecordBatch batch;
        private int row;

        private Stream(BatchCursor cursor) {
            this.cursor = cursor;
        }

        private boolean next() throws IOException {
            while (batch == null || row + 1 >= batch.rowCount()) {
                if (!cursor.next()) {
                    batch = null;
                    return false;
                }
                batch = cursor.batch();
                row = -1;
            }
            row++;
            return true;
        }

        private Object value(int column) {
            return batch.column(column).getObject(row);
        }
    }
}
