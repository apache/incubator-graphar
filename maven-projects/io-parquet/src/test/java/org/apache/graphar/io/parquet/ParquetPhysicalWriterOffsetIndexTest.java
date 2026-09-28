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

package org.apache.graphar.io.parquet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.ColumnRef;
import org.apache.graphar.io.ColumnType;
import org.apache.graphar.io.Field;
import org.apache.graphar.io.Projection;
import org.apache.graphar.io.ReadCapability;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.ReadResult;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.RecordBatches;
import org.apache.graphar.io.RowRange;
import org.apache.graphar.io.Schema;
import org.apache.graphar.io.WriteMode;
import org.apache.graphar.io.WriteRequest;
import org.apache.graphar.storage.local.LocalStorage;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.junit.Test;

public class ParquetPhysicalWriterOffsetIndexTest {
    private static final Schema TOPOLOGY_SCHEMA =
            new Schema(
                    List.of(
                            new Field(
                                    "_graphArSrcIndex",
                                    ColumnType.of(ColumnType.Kind.INT64),
                                    false),
                            new Field(
                                    "_graphArDstIndex",
                                    ColumnType.of(ColumnType.Kind.INT64),
                                    false)));

    @Test
    public void writesOffsetIndexesForPhysicalTopologyRangeReads() throws Exception {
        Path directory = Files.createTempDirectory("graphar-parquet-offset-index-");
        Path file = directory.resolve("topology.parquet");
        LocalStorage storage = new LocalStorage();
        URI uri = file.toUri();
        try {
            new ParquetPhysicalWriter(storage)
                    .write(
                            new WriteRequest(uri, TOPOLOGY_SCHEMA, WriteMode.CREATE_NEW),
                            topologyBatches());

            assertOffsetIndexes(storage, uri);

            ReadResult result =
                    new ParquetPhysicalReader(storage)
                            .read(
                                    ReadRequest.builder(uri)
                                            .projection(
                                                    Projection.of(ColumnRef.of("_graphArDstIndex")))
                                            .rowRange(new RowRange(2051, 2063))
                                            .build());
            assertEquals(
                    EnumSet.of(ReadCapability.PROJECTION, ReadCapability.ROW_RANGE),
                    result.report().applied());
            assertTrue(result.report().declined().isEmpty());
            assertEquals(
                    List.of(
                            12051L, 12052L, 12053L, 12054L, 12055L, 12056L, 12057L, 12058L, 12059L,
                            12060L, 12061L, 12062L),
                    destinations(result));
        } finally {
            Files.deleteIfExists(file);
            Files.deleteIfExists(directory);
        }
    }

    private static void assertOffsetIndexes(LocalStorage storage, URI uri) throws IOException {
        try (ParquetFileReader fileReader =
                ParquetFileReader.open(new ParquetInputFile(storage.inputFile(uri)))) {
            assertEquals(1, fileReader.getRowGroups().size());
            BlockMetaData rowGroup = fileReader.getRowGroups().get(0);
            for (ColumnChunkMetaData column : rowGroup.getColumns()) {
                OffsetIndex offsetIndex = fileReader.readOffsetIndex(column);
                assertNotNull("Missing Offset Index for " + column.getPath(), offsetIndex);
                assertEquals(
                        "Expected bounded pages for " + column.getPath(),
                        4,
                        offsetIndex.getPageCount());
            }
        }
    }

    private static BatchCursor topologyBatches() {
        List<Object[]> rows = new ArrayList<>();
        for (long index = 0; index < 4096; index++) {
            rows.add(new Object[] {index / 64, 10000L + index});
        }
        return new ListBatchCursor(List.of(RecordBatches.ofArrays(TOPOLOGY_SCHEMA, rows)));
    }

    private static List<Long> destinations(ReadResult result) throws IOException {
        List<Long> destinations = new ArrayList<>();
        try (BatchCursor cursor = result.cursor()) {
            while (cursor.next()) {
                RecordBatch batch = cursor.batch();
                for (int index = 0; index < batch.rowCount(); index++) {
                    destinations.add((Long) batch.column(0).getObject(index));
                }
            }
        }
        return destinations;
    }

    private static final class ListBatchCursor implements BatchCursor {
        private final List<RecordBatch> batches;
        private int index = -1;

        private ListBatchCursor(List<RecordBatch> batches) {
            this.batches = List.copyOf(batches);
        }

        @Override
        public boolean next() {
            index++;
            return index < batches.size();
        }

        @Override
        public RecordBatch batch() {
            if (index < 0 || index >= batches.size()) {
                throw new IllegalStateException("No current batch. Call next() before batch().");
            }
            return batches.get(index);
        }

        @Override
        public void close() {}
    }
}
