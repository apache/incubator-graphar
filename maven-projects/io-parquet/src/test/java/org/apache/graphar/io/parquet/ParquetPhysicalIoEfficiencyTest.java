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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.ColumnRef;
import org.apache.graphar.io.ColumnType;
import org.apache.graphar.io.Field;
import org.apache.graphar.io.Projection;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.ReadResult;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.RowRange;
import org.apache.graphar.io.Schema;
import org.apache.graphar.io.ValueVector;
import org.apache.graphar.io.WriteMode;
import org.apache.graphar.io.WriteRequest;
import org.apache.graphar.storage.InputFile;
import org.apache.graphar.storage.OutputFile;
import org.apache.graphar.storage.PositionOutput;
import org.apache.graphar.storage.SeekableInput;
import org.apache.graphar.storage.Storage;
import org.apache.graphar.storage.local.LocalStorage;
import org.junit.Test;

/** Regression gate for physical I/O of an indexed, projected Parquet range read. */
public class ParquetPhysicalIoEfficiencyTest {
    private static final int ROW_COUNT = 262_144;
    private static final int BATCH_ROWS = 1_024;
    private static final long RANGE_START = 131_456;
    private static final int RANGE_ROWS = 32;
    private static final Schema SCHEMA =
            new Schema(
                    List.of(
                            new Field("id", ColumnType.of(ColumnType.Kind.INT64), false),
                            new Field("payload", ColumnType.of(ColumnType.Kind.INT64), false)));

    @Test
    public void readsAnIndexedProjectedRangeWithoutReadingTheWholeFile() throws Exception {
        Path directory = Files.createTempDirectory("graphar-parquet-io-efficiency-");
        Path file = directory.resolve("topology.parquet");
        CountingStorage storage = new CountingStorage(new LocalStorage());
        URI uri = file.toUri();
        try {
            new ParquetPhysicalWriter(storage)
                    .write(
                            new WriteRequest(uri, SCHEMA, WriteMode.CREATE_NEW),
                            new GeneratedBatches(ROW_COUNT, BATCH_ROWS));

            long fileBytes = Files.size(file);
            Counters write = storage.outputCounters();
            storage.resetInputCounters();

            ReadResult result =
                    new ParquetPhysicalReader(storage)
                            .read(
                                    ReadRequest.builder(uri)
                                            .projection(Projection.of(ColumnRef.of("payload")))
                                            .rowRange(
                                                    new RowRange(
                                                            RANGE_START, RANGE_START + RANGE_ROWS))
                                            .build());
            long rows = assertPayloads(result);
            Counters read = storage.inputCounters();

            System.out.println(
                    "PARQUET_IO_BASELINE rows="
                            + ROW_COUNT
                            + " file_bytes="
                            + fileBytes
                            + " write_bytes="
                            + write.bytes
                            + " write_calls="
                            + write.calls
                            + " read_bytes="
                            + read.bytes
                            + " read_calls="
                            + read.calls
                            + " read_seeks="
                            + read.seeks
                            + " read_opens="
                            + read.opens);
            assertEquals(RANGE_ROWS, rows);
            assertEquals(1, read.opens);
            assertTrue(
                    "The generated data must exercise a materially sized file.",
                    fileBytes > 1_000_000);
            assertTrue(
                    "A projected 32-row range must not read even one eighth of the Parquet file: "
                            + read.bytes
                            + " of "
                            + fileBytes,
                    read.bytes * 8L < fileBytes);
        } finally {
            Files.deleteIfExists(file);
            Files.deleteIfExists(directory);
        }
    }

    @Test
    public void reusesARememberedFooterAcrossRepeatedReadsOfOneFile() throws Exception {
        Path directory = Files.createTempDirectory("graphar-parquet-footer-cache-");
        Path file = directory.resolve("topology.parquet");
        CountingStorage storage = new CountingStorage(new LocalStorage());
        URI uri = file.toUri();
        try {
            new ParquetPhysicalWriter(storage)
                    .write(
                            new WriteRequest(uri, SCHEMA, WriteMode.CREATE_NEW),
                            new GeneratedBatches(ROW_COUNT, BATCH_ROWS));

            ParquetPhysicalReader uncached = new ParquetPhysicalReader(storage, 0);
            assertEquals(RANGE_ROWS, readRange(uncached, uri));
            storage.resetInputCounters();
            assertEquals(RANGE_ROWS, readRange(uncached, uri));
            Counters repeatedWithoutCache = storage.inputCounters();

            ParquetPhysicalReader cached = new ParquetPhysicalReader(storage, 16);
            assertEquals(RANGE_ROWS, readRange(cached, uri));
            storage.resetInputCounters();
            assertEquals(RANGE_ROWS, readRange(cached, uri));
            Counters repeatedWithCache = storage.inputCounters();

            System.out.println(
                    "PARQUET_FOOTER_CACHE repeat_bytes_uncached="
                            + repeatedWithoutCache.bytes
                            + " repeat_calls_uncached="
                            + repeatedWithoutCache.calls
                            + " repeat_bytes_cached="
                            + repeatedWithCache.bytes
                            + " repeat_calls_cached="
                            + repeatedWithCache.calls);
            assertTrue(
                    "A remembered footer must remove read work from the repeated read: "
                            + repeatedWithCache.bytes
                            + " of "
                            + repeatedWithoutCache.bytes,
                    repeatedWithCache.bytes < repeatedWithoutCache.bytes);
            assertTrue(
                    "A remembered footer must not add stream opens.",
                    repeatedWithCache.opens == repeatedWithoutCache.opens);
        } finally {
            Files.deleteIfExists(file);
            Files.deleteIfExists(directory);
        }
    }

    private static long readRange(ParquetPhysicalReader reader, URI uri) throws IOException {
        return assertPayloads(
                reader.read(
                        ReadRequest.builder(uri)
                                .projection(Projection.of(ColumnRef.of("payload")))
                                .rowRange(new RowRange(RANGE_START, RANGE_START + RANGE_ROWS))
                                .build()));
    }

    private static long assertPayloads(ReadResult result) throws IOException {
        long rows = 0;
        try (BatchCursor cursor = result.cursor()) {
            while (cursor.next()) {
                RecordBatch batch = cursor.batch();
                for (int index = 0; index < batch.rowCount(); index++) {
                    assertEquals(mix(RANGE_START + rows), batch.column(0).getObject(index));
                    rows++;
                }
            }
        }
        return rows;
    }

    private static long mix(long value) {
        long result = value + 0x9E3779B97F4A7C15L;
        result = (result ^ (result >>> 30)) * 0xBF58476D1CE4E5B9L;
        result = (result ^ (result >>> 27)) * 0x94D049BB133111EBL;
        return result ^ (result >>> 31);
    }

    private static final class GeneratedBatches implements BatchCursor {
        private final int rowCount;
        private final int batchRows;
        private int nextStart;
        private RecordBatch current;

        private GeneratedBatches(int rowCount, int batchRows) {
            this.rowCount = rowCount;
            this.batchRows = batchRows;
        }

        @Override
        public boolean next() {
            if (nextStart == rowCount) {
                current = null;
                return false;
            }
            int start = nextStart;
            int size = Math.min(batchRows, rowCount - start);
            nextStart += size;
            current = new GeneratedBatch(start, size);
            return true;
        }

        @Override
        public RecordBatch batch() {
            if (current == null) {
                throw new IllegalStateException("No current batch. Call next() before batch().");
            }
            return current;
        }

        @Override
        public void close() {}
    }

    private static final class GeneratedBatch implements RecordBatch {
        private final int start;
        private final int rowCount;

        private GeneratedBatch(int start, int rowCount) {
            this.start = start;
            this.rowCount = rowCount;
        }

        @Override
        public Schema schema() {
            return SCHEMA;
        }

        @Override
        public int rowCount() {
            return rowCount;
        }

        @Override
        public int columnCount() {
            return SCHEMA.fields().size();
        }

        @Override
        public ValueVector column(int columnIndex) {
            Field field = SCHEMA.fields().get(columnIndex);
            return new ValueVector() {
                @Override
                public Field field() {
                    return field;
                }

                @Override
                public int valueCount() {
                    return rowCount;
                }

                @Override
                public boolean isNull(int index) {
                    return false;
                }

                @Override
                public Object getObject(int index) {
                    if (index < 0 || index >= rowCount) {
                        throw new IndexOutOfBoundsException("Row index: " + index);
                    }
                    long value = start + index;
                    return columnIndex == 0 ? value : mix(value);
                }
            };
        }
    }

    private static final class CountingStorage implements Storage {
        private final Storage delegate;
        private final Counters input = new Counters();
        private final Counters output = new Counters();

        private CountingStorage(Storage delegate) {
            this.delegate = delegate;
        }

        @Override
        public InputFile inputFile(URI uri) {
            return new CountingInputFile(delegate.inputFile(uri), input);
        }

        @Override
        public OutputFile outputFile(URI uri) {
            return new CountingOutputFile(delegate.outputFile(uri), output);
        }

        @Override
        public boolean exists(URI uri) throws IOException {
            return delegate.exists(uri);
        }

        private Counters inputCounters() {
            return input.copy();
        }

        private Counters outputCounters() {
            return output.copy();
        }

        private void resetInputCounters() {
            input.reset();
        }
    }

    private static final class CountingInputFile implements InputFile {
        private final InputFile delegate;
        private final Counters counters;

        private CountingInputFile(InputFile delegate, Counters counters) {
            this.delegate = delegate;
            this.counters = counters;
        }

        @Override
        public URI uri() {
            return delegate.uri();
        }

        @Override
        public long size() throws IOException {
            return delegate.size();
        }

        @Override
        public SeekableInput open() throws IOException {
            counters.opens++;
            return new CountingSeekableInput(delegate.open(), counters);
        }
    }

    private static final class CountingOutputFile implements OutputFile {
        private final OutputFile delegate;
        private final Counters counters;

        private CountingOutputFile(OutputFile delegate, Counters counters) {
            this.delegate = delegate;
            this.counters = counters;
        }

        @Override
        public URI uri() {
            return delegate.uri();
        }

        @Override
        public PositionOutput create() throws IOException {
            counters.opens++;
            return new CountingPositionOutput(delegate.create(), counters);
        }

        @Override
        public PositionOutput createOrOverwrite() throws IOException {
            counters.opens++;
            return new CountingPositionOutput(delegate.createOrOverwrite(), counters);
        }
    }

    private static final class CountingSeekableInput implements SeekableInput {
        private final SeekableInput delegate;
        private final Counters counters;

        private CountingSeekableInput(SeekableInput delegate, Counters counters) {
            this.delegate = delegate;
            this.counters = counters;
        }

        @Override
        public long position() throws IOException {
            return delegate.position();
        }

        @Override
        public void seek(long newPosition) throws IOException {
            counters.seeks++;
            delegate.seek(newPosition);
        }

        @Override
        public int read(ByteBuffer destination) throws IOException {
            int read = delegate.read(destination);
            counters.calls++;
            if (read > 0) {
                counters.bytes += read;
            }
            return read;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static final class CountingPositionOutput implements PositionOutput {
        private final PositionOutput delegate;
        private final Counters counters;

        private CountingPositionOutput(PositionOutput delegate, Counters counters) {
            this.delegate = delegate;
            this.counters = counters;
        }

        @Override
        public long position() throws IOException {
            return delegate.position();
        }

        @Override
        public void write(ByteBuffer source) throws IOException {
            int bytes = source.remaining();
            delegate.write(source);
            counters.calls++;
            counters.bytes += bytes;
        }

        @Override
        public void write(byte[] source, int offset, int length) throws IOException {
            delegate.write(source, offset, length);
            counters.calls++;
            counters.bytes += length;
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static final class Counters {
        private long bytes;
        private long calls;
        private long seeks;
        private long opens;

        private Counters copy() {
            Counters result = new Counters();
            result.bytes = bytes;
            result.calls = calls;
            result.seeks = seeks;
            result.opens = opens;
            return result;
        }

        private void reset() {
            bytes = 0;
            calls = 0;
            seeks = 0;
            opens = 0;
        }
    }
}
