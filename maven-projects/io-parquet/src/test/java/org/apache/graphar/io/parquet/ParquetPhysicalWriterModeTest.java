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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.ColumnType;
import org.apache.graphar.io.Field;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.RecordBatches;
import org.apache.graphar.io.Schema;
import org.apache.graphar.io.WriteMode;
import org.apache.graphar.io.WriteRequest;
import org.apache.graphar.storage.local.LocalStorage;
import org.junit.Test;

public class ParquetPhysicalWriterModeTest {
    private static final Schema SCHEMA =
            new Schema(List.of(new Field("id", ColumnType.of(ColumnType.Kind.INT64), false)));

    @Test
    public void rejectsAppendWithoutTouchingTheExistingFile() throws Exception {
        Path file = Files.createTempFile("graphar-mode-", ".parquet");
        Files.deleteIfExists(file);
        LocalStorage storage = new LocalStorage();
        URI uri = file.toUri();
        try {
            ParquetPhysicalWriter writer = new ParquetPhysicalWriter(storage);
            writer.write(new WriteRequest(uri, SCHEMA, WriteMode.CREATE_NEW), batch(7L));
            byte[] before = Files.readAllBytes(file);
            UnsupportedOperationException failure =
                    assertThrows(
                            UnsupportedOperationException.class,
                            () ->
                                    writer.write(
                                            new WriteRequest(uri, SCHEMA, WriteMode.APPEND),
                                            batch(8L)));
            assertTrue(failure.getMessage(), failure.getMessage().contains("APPEND"));
            assertArrayEquals(before, Files.readAllBytes(file));
            assertEquals(List.of(7L), ids(storage, uri));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    public void overwriteReplacesTheExistingFile() throws Exception {
        Path file = Files.createTempFile("graphar-mode-", ".parquet");
        Files.deleteIfExists(file);
        LocalStorage storage = new LocalStorage();
        URI uri = file.toUri();
        try {
            ParquetPhysicalWriter writer = new ParquetPhysicalWriter(storage);
            writer.write(new WriteRequest(uri, SCHEMA, WriteMode.CREATE_NEW), batch(7L));
            writer.write(new WriteRequest(uri, SCHEMA, WriteMode.OVERWRITE), batch(8L));
            assertEquals(List.of(8L), ids(storage, uri));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    private static BatchCursor batch(long id) {
        return new BatchCursor() {
            private boolean available = true;

            @Override
            public boolean next() {
                boolean result = available;
                available = false;
                return result;
            }

            @Override
            public org.apache.graphar.io.RecordBatch batch() {
                return RecordBatches.ofArrays(SCHEMA, List.<Object[]>of(new Object[] {id}));
            }

            @Override
            public void close() {}
        };
    }

    private static List<Object> ids(LocalStorage storage, URI uri) throws Exception {
        List<Object> ids = new java.util.ArrayList<>();
        try (BatchCursor cursor =
                new ParquetPhysicalReader(storage)
                        .read(ReadRequest.builder(uri).build())
                        .cursor()) {
            while (cursor.next()) {
                for (int index = 0; index < cursor.batch().rowCount(); index++) {
                    ids.add(cursor.batch().column(0).getObject(index));
                }
            }
        }
        return ids;
    }
}
