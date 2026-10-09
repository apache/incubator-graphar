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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.ColumnType;
import org.apache.graphar.io.Field;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.ReadResult;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.RecordBatches;
import org.apache.graphar.io.Schema;
import org.apache.graphar.io.WriteMode;
import org.apache.graphar.io.WriteRequest;
import org.apache.graphar.storage.InputFile;
import org.apache.graphar.storage.OutputFile;
import org.apache.graphar.storage.SeekableInput;
import org.apache.graphar.storage.Storage;
import org.apache.graphar.storage.local.LocalStorage;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.Type;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Verifies the Parquet layouts this module writes and accepts follow the Parquet format spec. */
public class ParquetSpecConformanceTest {
    private Path directory;

    @Before
    public void createDirectory() throws IOException {
        directory = Files.createTempDirectory("graphar-parquet-spec-");
    }

    @After
    public void deleteDirectory() throws IOException {
        try (var paths = Files.list(directory)) {
            for (Path path : (Iterable<Path>) paths::iterator) {
                Files.delete(path);
            }
        }
        Files.delete(directory);
    }

    @Test
    public void writesThreeLevelListsWithOptionalElementsAndNullEntries() throws IOException {
        Schema schema =
                new Schema(
                        List.of(
                                new Field(
                                        "tags",
                                        ColumnType.listOf(ColumnType.of(ColumnType.Kind.STRING)),
                                        true)));
        URI uri = write(schema, List.of(List.of(Arrays.asList("a", null, "b"))));

        Type element = listElement(uri, "tags");
        assertEquals("element", element.getName());
        assertTrue(element.isRepetition(Type.Repetition.OPTIONAL));

        ReadResult result = read(uri);
        assertEquals(schema, result.cursor().next() ? result.cursor().batch().schema() : null);
        assertEquals(Arrays.asList("a", null, "b"), result.cursor().batch().column(0).getObject(0));
        result.cursor().close();
    }

    @Test
    public void writesRequiredElementsForANonNullableElementList() throws IOException {
        Schema schema =
                new Schema(
                        List.of(
                                new Field(
                                        "ids",
                                        ColumnType.listOfElement(
                                                new Field(
                                                        "element",
                                                        ColumnType.of(ColumnType.Kind.INT64),
                                                        false)),
                                        false)));
        URI uri = write(schema, List.of(List.of(List.of(1L, 2L))));

        assertTrue(listElement(uri, "ids").isRepetition(Type.Repetition.REQUIRED));
        assertEquals(List.of(List.of(1L, 2L)), column(uri, 0));
    }

    @Test
    public void readsAListWrittenByAnotherParquetWriter() throws IOException {
        MessageType foreign =
                MessageTypeParser.parseMessageType(
                        "message spark_schema {"
                                + "  optional group scores (LIST) {"
                                + "    repeated group list {"
                                + "      optional int64 element;"
                                + "    }"
                                + "  }"
                                + "}");
        URI uri = directory.resolve("foreign.parquet").toUri();
        LocalStorage storage = new LocalStorage();
        try (ParquetWriter<Group> writer =
                ExampleParquetWriter.builder(new ParquetOutputFile(storage.outputFile(uri)))
                        .withType(foreign)
                        .build()) {
            Group row = new SimpleGroupFactory(foreign).newGroup();
            Group list = row.addGroup("scores");
            list.addGroup("list").add("element", 7L);
            list.addGroup("list");
            writer.write(row);
        }

        assertEquals(List.of(Arrays.asList(7L, null)), column(uri, 0));
    }

    @Test
    public void releasesTheFileAsSoonAsTheLimitIsReached() throws IOException {
        Schema schema =
                new Schema(List.of(new Field("id", ColumnType.of(ColumnType.Kind.INT64), false)));
        List<List<Object>> rows = new ArrayList<>();
        for (long id = 0; id < 10; id++) {
            rows.add(List.of(id));
        }
        URI uri = write(schema, rows);
        TrackingStorage storage = new TrackingStorage(new LocalStorage());

        BatchCursor cursor =
                new ParquetPhysicalReader(storage)
                        .read(ReadRequest.builder(uri).limit(3).build())
                        .cursor();

        assertTrue(cursor.next());
        assertEquals(3, cursor.batch().rowCount());
        assertEquals(storage.opened, storage.closed);
        assertTrue(storage.opened > 0);
        assertEquals(2L, cursor.batch().column(0).getObject(2));
        assertFalse(cursor.next());
        cursor.close();
    }

    private URI write(Schema schema, List<? extends List<?>> rows) throws IOException {
        URI uri = Files.createTempFile(directory, "spec-", ".parquet").toUri();
        Files.delete(Path.of(uri));
        RecordBatch batch = RecordBatches.ofRows(schema, rows);
        new ParquetPhysicalWriter(new LocalStorage())
                .write(new WriteRequest(uri, schema, WriteMode.CREATE_NEW), new OneBatch(batch));
        return uri;
    }

    private static ReadResult read(URI uri) throws IOException {
        return new ParquetPhysicalReader(new LocalStorage()).read(ReadRequest.builder(uri).build());
    }

    private static List<Object> column(URI uri, int column) throws IOException {
        List<Object> values = new ArrayList<>();
        try (BatchCursor cursor = read(uri).cursor()) {
            while (cursor.next()) {
                RecordBatch batch = cursor.batch();
                for (int row = 0; row < batch.rowCount(); row++) {
                    values.add(batch.column(column).getObject(row));
                }
            }
        }
        return values;
    }

    private static Type listElement(URI uri, String name) throws IOException {
        InputFile file = new LocalStorage().inputFile(uri);
        try (ParquetFileReader reader = ParquetFileReader.open(new ParquetInputFile(file))) {
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            return schema.getType(name).asGroupType().getType(0).asGroupType().getType(0);
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
            boolean first = !advanced;
            advanced = true;
            return first;
        }

        @Override
        public RecordBatch batch() {
            return batch;
        }

        @Override
        public void close() {}
    }

    private static final class TrackingStorage implements Storage {
        private final Storage delegate;
        private int opened;
        private int closed;

        private TrackingStorage(Storage delegate) {
            this.delegate = delegate;
        }

        @Override
        public InputFile inputFile(URI uri) {
            InputFile file = delegate.inputFile(uri);
            return new InputFile() {
                @Override
                public URI uri() {
                    return file.uri();
                }

                @Override
                public long size() throws IOException {
                    return file.size();
                }

                @Override
                public SeekableInput open() throws IOException {
                    SeekableInput input = file.open();
                    opened++;
                    return new SeekableInput() {
                        @Override
                        public long position() throws IOException {
                            return input.position();
                        }

                        @Override
                        public void seek(long newPosition) throws IOException {
                            input.seek(newPosition);
                        }

                        @Override
                        public int read(ByteBuffer destination) throws IOException {
                            return input.read(destination);
                        }

                        @Override
                        public void close() throws IOException {
                            closed++;
                            input.close();
                        }
                    };
                }
            };
        }

        @Override
        public OutputFile outputFile(URI uri) {
            return delegate.outputFile(uri);
        }

        @Override
        public boolean exists(URI uri) throws IOException {
            return delegate.exists(uri);
        }
    }
}
