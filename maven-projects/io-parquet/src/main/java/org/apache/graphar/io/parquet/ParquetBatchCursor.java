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

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.RowRange;
import org.apache.graphar.io.Schema;
import org.apache.graphar.io.ValueVector;
import org.apache.graphar.io.VectorRecordBatch;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.filter2.columnindex.RowRanges;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore.MissingOffsetIndexException;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

/** Streams materialized Parquet row groups as neutral record batches. */
final class ParquetBatchCursor implements BatchCursor {
    private static final int BATCH_ROWS = 1_024;
    private final ParquetFileReader fileReader;
    private final MessageType fileSchema;
    private final MessageType readSchema;
    private final List<ParquetColumn> readColumns;
    private final Schema outputSchema;
    private final Map<String, Integer> readColumnIndexes;
    private final int[] outputIndexes;
    private final long rangeStart;
    private final long rangeEnd;
    private final long limit;
    private final List<BlockRange> rowGroups;
    private int nextRowGroup;
    private long emitted;
    private long rowsRemainingInGroup;
    private PageReadStore pages;
    private RecordReader<Group> rows;
    private boolean exhausted;
    private boolean closed;
    private RecordBatch current;

    ParquetBatchCursor(
            ParquetFileReader fileReader,
            MessageType fileSchema,
            MessageType readSchema,
            List<ParquetColumn> readColumns,
            List<ParquetColumn> outputColumns,
            Schema outputSchema,
            ReadRequest request) {
        this.fileReader = fileReader;
        this.fileSchema = fileSchema;
        this.readSchema = readSchema;
        this.readColumns = readColumns;
        this.outputSchema = outputSchema;
        this.readColumnIndexes = indexes(readColumns);
        this.outputIndexes = outputIndexes(outputColumns, readColumnIndexes);
        RowRange range = request.rowRange().orElse(null);
        this.rangeStart = range == null ? 0 : range.startInclusive();
        this.rangeEnd = range == null ? Long.MAX_VALUE : range.endExclusive();
        this.limit = request.limit().isPresent() ? request.limit().getAsLong() : Long.MAX_VALUE;
        this.rowGroups = rowGroups(fileReader.getRowGroups());
    }

    @Override
    public boolean next() throws IOException {
        if (closed || exhausted) {
            current = null;
            return false;
        }
        if (limit == 0) {
            finish();
            return false;
        }
        try {
            while (true) {
                if (rows == null && !openNextRowGroup()) {
                    finish();
                    return false;
                }
                int batchSize =
                        (int) Math.min(Math.min(rowsRemainingInGroup, BATCH_ROWS), limit - emitted);
                Object[][] columns = new Object[outputIndexes.length][batchSize];
                for (int index = 0; index < batchSize; index++) {
                    Group group = rows.read();
                    project(values(group), columns, index);
                    emitted++;
                    rowsRemainingInGroup--;
                }
                if (rowsRemainingInGroup == 0) {
                    closePages();
                }
                current = batch(columns, batchSize);
                if (emitted == limit) {
                    exhausted = true;
                    closeReader();
                }
                return true;
            }
        } catch (MissingOffsetIndexException exception) {
            try {
                closeReader();
            } catch (IOException closeException) {
                exception.addSuppressed(closeException);
            }
            throw new UnsupportedOperationException(
                    "A partial row-group range requires a Parquet Offset Index; refusing JVM fallback.",
                    exception);
        } catch (IOException | RuntimeException exception) {
            try {
                closeReader();
            } catch (IOException closeException) {
                exception.addSuppressed(closeException);
            }
            throw exception;
        }
    }

    @Override
    public RecordBatch batch() {
        if (current == null) {
            throw new IllegalStateException("No current batch. Call next() before batch().");
        }
        return current;
    }

    @Override
    public void close() throws IOException {
        current = null;
        exhausted = true;
        closeReader();
    }

    private Object[] values(Group group) {
        Object[] values = new Object[readColumns.size()];
        for (int index = 0; index < readColumns.size(); index++) {
            if (group.getFieldRepetitionCount(index) != 0) {
                values[index] = value(group, index, readColumns.get(index));
            }
        }
        return values;
    }

    private static Object value(Group group, int index, ParquetColumn column) {
        if (column.field().type().kind() == org.apache.graphar.io.ColumnType.Kind.LIST) {
            return listValue(group, index, column);
        }
        return scalarValue(group, index, column.field().type().kind());
    }

    private static List<Object> listValue(Group group, int index, ParquetColumn column) {
        Group listGroup = group.getGroup(index, 0);
        if (listGroup.getType().getFieldCount() != 1) {
            throw new IllegalArgumentException(
                    "Unsupported Parquet LIST field: " + column.field().name());
        }
        int count = listGroup.getFieldRepetitionCount(0);
        if (count == 0) {
            return List.of();
        }
        List<Object> values = new ArrayList<>(count);
        org.apache.graphar.io.ColumnType elementType =
                column.field().type().elementType().orElseThrow();
        for (int position = 0; position < count; position++) {
            Group repeatedGroup = listGroup.getGroup(0, position);
            if (repeatedGroup.getType().getFieldCount() != 1) {
                throw new IllegalArgumentException(
                        "Unsupported Parquet LIST element: " + column.field().name());
            }
            values.add(
                    repeatedGroup.getFieldRepetitionCount(0) == 0
                            ? null
                            : scalarValue(repeatedGroup, 0, elementType, 0));
        }
        return Collections.unmodifiableList(values);
    }

    private static Object scalarValue(
            Group group, int index, org.apache.graphar.io.ColumnType.Kind kind) {
        return scalarValue(group, index, org.apache.graphar.io.ColumnType.of(kind), 0);
    }

    private static Object scalarValue(
            Group group, int index, org.apache.graphar.io.ColumnType type, int repetitionIndex) {
        switch (type.kind()) {
            case BOOLEAN:
                return group.getBoolean(index, repetitionIndex);
            case INT8:
                return (byte) group.getInteger(index, repetitionIndex);
            case INT16:
                return (short) group.getInteger(index, repetitionIndex);
            case INT32:
                return group.getInteger(index, repetitionIndex);
            case INT64:
                return group.getLong(index, repetitionIndex);
            case FLOAT32:
                return group.getFloat(index, repetitionIndex);
            case FLOAT64:
                return group.getDouble(index, repetitionIndex);
            case STRING:
                return group.getBinary(index, repetitionIndex).toStringUsingUTF8();
            case BINARY:
                return group.getBinary(index, repetitionIndex).getBytes();
            case DATE:
                return LocalDate.ofEpochDay(group.getInteger(index, repetitionIndex));
            case TIMESTAMP_MILLIS:
                return Instant.ofEpochMilli(group.getLong(index, repetitionIndex));
            default:
                throw new IllegalArgumentException(
                        "Unsupported Parquet column type: " + type.kind());
        }
    }

    private void project(Object[] values, Object[][] columns, int row) {
        for (int index = 0; index < outputIndexes.length; index++) {
            Object value = values[outputIndexes[index]];
            columns[index][row] = value instanceof byte[] ? ((byte[]) value).clone() : value;
        }
    }

    private RecordBatch batch(Object[][] columns, int rowCount) {
        List<ValueVector> vectors = new ArrayList<>(columns.length);
        for (int index = 0; index < columns.length; index++) {
            vectors.add(new ParquetValueVector(outputSchema.fields().get(index), columns[index]));
        }
        return new VectorRecordBatch(outputSchema, vectors, rowCount);
    }

    private void finish() throws IOException {
        exhausted = true;
        current = null;
        closeReader();
    }

    private void closeReader() throws IOException {
        if (!closed) {
            closed = true;
            try {
                closePages();
            } finally {
                fileReader.close();
            }
        }
    }

    private boolean openNextRowGroup() throws IOException {
        BlockRange rowGroup = nextRange();
        if (rowGroup == null) return false;
        pages =
                rowGroup.whole
                        ? fileReader.readRowGroup(rowGroup.index)
                        : fileReader.readFilteredRowGroup(
                                rowGroup.index,
                                RowRanges.builder()
                                        .addSelectedRange(rowGroup.start, rowGroup.end - 1)
                                        .build());
        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(readSchema, fileSchema);
        rows = columnIO.getRecordReader(pages, new GroupRecordConverter(readSchema));
        rowsRemainingInGroup = pages.getRowCount();
        if (rowsRemainingInGroup == 0) {
            closePages();
            return openNextRowGroup();
        }
        return true;
    }

    private void closePages() throws IOException {
        rows = null;
        rowsRemainingInGroup = 0;
        if (pages != null) {
            PageReadStore openPages = pages;
            pages = null;
            openPages.close();
        }
    }

    private static Map<String, Integer> indexes(List<ParquetColumn> columns) {
        Map<String, Integer> indexes = new HashMap<>();
        for (int index = 0; index < columns.size(); index++) {
            indexes.put(columns.get(index).field().name(), index);
        }
        return indexes;
    }

    private static int[] outputIndexes(
            List<ParquetColumn> outputColumns, Map<String, Integer> readColumnIndexes) {
        int[] indexes = new int[outputColumns.size()];
        for (int index = 0; index < outputColumns.size(); index++) {
            indexes[index] = readColumnIndexes.get(outputColumns.get(index).field().name());
        }
        return indexes;
    }

    private BlockRange nextRange() {
        while (nextRowGroup < rowGroups.size()) {
            BlockRange rowGroup = rowGroups.get(nextRowGroup++);
            long begin = Math.max(rangeStart, rowGroup.start);
            long end = Math.min(rangeEnd, rowGroup.end);
            if (begin < end) {
                return new BlockRange(
                        rowGroup.index,
                        begin - rowGroup.start,
                        end - rowGroup.start,
                        begin == rowGroup.start && end == rowGroup.end);
            }
        }
        return null;
    }

    private static List<BlockRange> rowGroups(List<BlockMetaData> blocks) {
        List<BlockRange> result = new ArrayList<>(blocks.size());
        long start = 0;
        for (int index = 0; index < blocks.size(); index++) {
            long end = Math.addExact(start, blocks.get(index).getRowCount());
            result.add(new BlockRange(index, start, end, true));
            start = end;
        }
        return List.copyOf(result);
    }

    private static final class BlockRange {
        private final int index;
        private final long start;
        private final long end;
        private final boolean whole;

        private BlockRange(int index, long start, long end, boolean whole) {
            this.index = index;
            this.start = start;
            this.end = end;
            this.whole = whole;
        }
    }
}
