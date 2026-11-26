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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.ColumnRef;
import org.apache.graphar.io.Projection;
import org.apache.graphar.io.ReadCapability;
import org.apache.graphar.io.ReadReport;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.ReadResult;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.RowRange;
import org.apache.graphar.storage.local.LocalStorage;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Test;

public class ParquetPhysicalReaderWithoutOffsetIndexTest {
    private static final Path CANONICAL_ADJ_LIST =
            Path.of(
                    "..",
                    "..",
                    "testing",
                    "ldbc_sample",
                    "parquet",
                    "edge",
                    "person_knows_person",
                    "ordered_by_source",
                    "adj_list",
                    "part0",
                    "chunk0");
    private static final MessageType SCHEMA =
            MessageTypeParser.parseMessageType(
                    "message topology { required int64 id; required int64 payload; }");
    private static final int ROW_COUNT = 20_000;

    @Test
    public void slicesAPartialRangeOfACanonicalFileWithoutOffsetIndex() throws IOException {
        LocalStorage storage = new LocalStorage();
        URI uri = CANONICAL_ADJ_LIST.toUri();
        assertFalse(hasOffsetIndexes(storage, uri));
        List<List<Object>> all = rows(read(storage, ReadRequest.builder(uri).build()));
        assertTrue(all.size() > 400);

        ReadResult result =
                read(storage, ReadRequest.builder(uri).rowRange(new RowRange(123, 401)).build());

        assertEquals(EnumSet.noneOf(ReadCapability.class), result.report().applied());
        assertEquals(EnumSet.of(ReadCapability.ROW_RANGE), result.report().declined());
        assertEquals(all.subList(123, 401), rows(result));
    }

    @Test
    public void appliesProjectionAndLimitWhileDecliningTheRowRange() throws IOException {
        LocalStorage storage = new LocalStorage();
        URI uri = CANONICAL_ADJ_LIST.toUri();
        String column = firstColumn(storage, uri);
        List<List<Object>> all =
                rows(
                        read(
                                storage,
                                ReadRequest.builder(uri)
                                        .projection(Projection.of(ColumnRef.of(column)))
                                        .build()));

        ReadResult result =
                read(
                        storage,
                        ReadRequest.builder(uri)
                                .projection(Projection.of(ColumnRef.of(column)))
                                .rowRange(new RowRange(600, all.size()))
                                .limit(10)
                                .build());

        assertEquals(
                EnumSet.of(ReadCapability.PROJECTION, ReadCapability.LIMIT),
                result.report().applied());
        assertEquals(EnumSet.of(ReadCapability.ROW_RANGE), result.report().declined());
        assertEquals(all.subList(600, 610), rows(result));
    }

    @Test
    public void returnsTheSameRowsWithAndWithoutOffsetIndexAcrossRowGroups() throws Exception {
        Path directory = Files.createTempDirectory("graphar-parquet-without-offset-index-");
        Path indexed = directory.resolve("indexed.parquet");
        Path plain = directory.resolve("plain.parquet");
        LocalStorage storage = new LocalStorage();
        try {
            writeRowGroups(storage, indexed.toUri());
            Files.write(plain, withoutPageIndexes(Files.readAllBytes(indexed)));
            assertTrue(hasOffsetIndexes(storage, indexed.toUri()));
            assertFalse(hasOffsetIndexes(storage, plain.toUri()));
            List<Long> starts = rowGroupStarts(storage, plain.toUri());
            assertTrue("Expected several row groups, got " + starts, starts.size() >= 4);

            RowRange partial = new RowRange(starts.get(1) + 7, starts.get(3) - 5);
            ReadResult withIndex = read(storage, rangeRequest(indexed.toUri(), partial));
            ReadResult withoutIndex = read(storage, rangeRequest(plain.toUri(), partial));
            assertEquals(
                    new ReadReport(
                            EnumSet.of(ReadCapability.PROJECTION, ReadCapability.ROW_RANGE),
                            EnumSet.noneOf(ReadCapability.class)),
                    withIndex.report());
            assertEquals(
                    new ReadReport(
                            EnumSet.of(ReadCapability.PROJECTION),
                            EnumSet.of(ReadCapability.ROW_RANGE)),
                    withoutIndex.report());
            List<List<Object>> expected = payloads(partial);
            assertEquals(expected, rows(withIndex));
            assertEquals(expected, rows(withoutIndex));

            RowRange aligned = new RowRange(starts.get(1), starts.get(3));
            ReadResult wholeGroups = read(storage, rangeRequest(plain.toUri(), aligned));
            assertEquals(
                    new ReadReport(
                            EnumSet.of(ReadCapability.PROJECTION, ReadCapability.ROW_RANGE),
                            EnumSet.noneOf(ReadCapability.class)),
                    wholeGroups.report());
            assertEquals(payloads(aligned), rows(wholeGroups));
        } finally {
            Files.deleteIfExists(indexed);
            Files.deleteIfExists(plain);
            Files.deleteIfExists(directory);
        }
    }

    private static ReadRequest rangeRequest(URI uri, RowRange range) {
        return ReadRequest.builder(uri)
                .projection(Projection.of(ColumnRef.of("payload")))
                .rowRange(range)
                .build();
    }

    private static List<List<Object>> payloads(RowRange range) {
        List<List<Object>> rows = new ArrayList<>();
        for (long row = range.startInclusive(); row < range.endExclusive(); row++) {
            rows.add(List.of(row * 3));
        }
        return rows;
    }

    private static ReadResult read(LocalStorage storage, ReadRequest request) throws IOException {
        return new ParquetPhysicalReader(storage).read(request);
    }

    private static List<List<Object>> rows(ReadResult result) throws IOException {
        List<List<Object>> rows = new ArrayList<>();
        try (BatchCursor cursor = result.cursor()) {
            while (cursor.next()) {
                RecordBatch batch = cursor.batch();
                for (int row = 0; row < batch.rowCount(); row++) {
                    List<Object> values = new ArrayList<>();
                    for (int column = 0; column < batch.schema().fields().size(); column++) {
                        values.add(batch.column(column).getObject(row));
                    }
                    rows.add(values);
                }
            }
        }
        return rows;
    }

    private static String firstColumn(LocalStorage storage, URI uri) throws IOException {
        try (ParquetFileReader fileReader = open(storage, uri)) {
            return fileReader.getFileMetaData().getSchema().getFields().get(0).getName();
        }
    }

    private static boolean hasOffsetIndexes(LocalStorage storage, URI uri) throws IOException {
        try (ParquetFileReader fileReader = open(storage, uri)) {
            for (BlockMetaData rowGroup : fileReader.getRowGroups()) {
                for (ColumnChunkMetaData column : rowGroup.getColumns()) {
                    if (fileReader.readOffsetIndex(column) == null) {
                        assertNull(column.getOffsetIndexReference());
                        return false;
                    }
                    assertNotNull(column.getOffsetIndexReference());
                }
            }
            return true;
        }
    }

    private static List<Long> rowGroupStarts(LocalStorage storage, URI uri) throws IOException {
        List<Long> starts = new ArrayList<>();
        long start = 0;
        try (ParquetFileReader fileReader = open(storage, uri)) {
            for (BlockMetaData rowGroup : fileReader.getRowGroups()) {
                starts.add(start);
                start += rowGroup.getRowCount();
            }
        }
        assertEquals(ROW_COUNT, start);
        return starts;
    }

    private static ParquetFileReader open(LocalStorage storage, URI uri) throws IOException {
        return ParquetFileReader.open(new ParquetInputFile(storage.inputFile(uri)));
    }

    private static void writeRowGroups(LocalStorage storage, URI uri) throws IOException {
        SimpleGroupFactory groups = new SimpleGroupFactory(SCHEMA);
        try (ParquetWriter<Group> writer =
                ExampleParquetWriter.builder(new ParquetOutputFile(storage.outputFile(uri)))
                        .withType(SCHEMA)
                        .withRowGroupSize(32 * 1024L)
                        .withPageRowCountLimit(256)
                        .build()) {
            for (long row = 0; row < ROW_COUNT; row++) {
                writer.write(groups.newGroup().append("id", row).append("payload", row * 3));
            }
        }
    }

    private static byte[] withoutPageIndexes(byte[] file) throws IOException {
        int footerLength =
                ByteBuffer.wrap(file, file.length - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        int footerStart = file.length - 8 - footerLength;
        FileMetaData footer =
                Util.readFileMetaData(new ByteArrayInputStream(file, footerStart, footerLength));
        for (RowGroup rowGroup : footer.getRow_groups()) {
            for (ColumnChunk column : rowGroup.getColumns()) {
                column.unsetOffset_index_offset();
                column.unsetOffset_index_length();
                column.unsetColumn_index_offset();
                column.unsetColumn_index_length();
            }
        }
        ByteArrayOutputStream encodedFooter = new ByteArrayOutputStream();
        Util.writeFileMetaData(footer, encodedFooter);
        ByteArrayOutputStream rewritten = new ByteArrayOutputStream();
        rewritten.write(file, 0, footerStart);
        encodedFooter.writeTo(rewritten);
        rewritten.write(
                ByteBuffer.allocate(4)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .putInt(encodedFooter.size())
                        .array());
        rewritten.write(Arrays.copyOfRange(file, file.length - 4, file.length));
        return rewritten.toByteArray();
    }
}
