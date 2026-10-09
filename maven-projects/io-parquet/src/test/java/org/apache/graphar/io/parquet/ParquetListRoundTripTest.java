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

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.ColumnType;
import org.apache.graphar.io.Field;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.RecordBatches;
import org.apache.graphar.io.Schema;
import org.apache.graphar.io.WriteMode;
import org.apache.graphar.io.WriteRequest;
import org.apache.graphar.storage.local.LocalStorage;
import org.junit.Test;

/**
 * Verifies standard Parquet LIST values, empty lists, and null lists round-trip through neutral IO.
 */
public class ParquetListRoundTripTest {
    private static final Schema SCHEMA =
            new Schema(
                    List.of(
                            new Field("id", ColumnType.of(ColumnType.Kind.INT64), false),
                            new Field(
                                    "tags",
                                    ColumnType.listOf(ColumnType.of(ColumnType.Kind.STRING)),
                                    true)));

    @Test
    public void roundTripsNullableStringLists() throws Exception {
        Path file = Files.createTempFile("graphar-list-", ".parquet");
        Files.deleteIfExists(file);
        LocalStorage storage = new LocalStorage();
        URI uri = file.toUri();
        try {
            new ParquetPhysicalWriter(storage)
                    .write(
                            new WriteRequest(uri, SCHEMA, WriteMode.CREATE_NEW),
                            new OneBatch(
                                    RecordBatches.ofRows(
                                            SCHEMA,
                                            List.of(
                                                    Arrays.asList(1L, List.of("one", "two")),
                                                    Arrays.asList(2L, List.of()),
                                                    Arrays.asList(3L, null)))));
            List<Object> lists = new ArrayList<>();
            try (BatchCursor cursor =
                    new ParquetPhysicalReader(storage)
                            .read(org.apache.graphar.io.ReadRequest.builder(uri).build())
                            .cursor()) {
                while (cursor.next()) {
                    RecordBatch batch = cursor.batch();
                    for (int index = 0; index < batch.rowCount(); index++) {
                        lists.add(batch.column(1).getObject(index));
                    }
                }
            }
            assertEquals(List.of("one", "two"), lists.get(0));
            assertEquals(List.of(), lists.get(1));
            assertEquals(null, lists.get(2));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    private static final class OneBatch implements BatchCursor {
        private final RecordBatch batch;
        private boolean advanced;

        private OneBatch(RecordBatch batch) {
            this.batch = batch;
        }

        @Override
        public boolean next() {
            if (advanced) {
                return false;
            }
            advanced = true;
            return true;
        }

        @Override
        public RecordBatch batch() {
            return batch;
        }

        @Override
        public void close() {}
    }
}
