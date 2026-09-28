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
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.graphar.io.ColumnRef;
import org.apache.graphar.io.ColumnType;
import org.apache.graphar.io.Field;
import org.apache.graphar.io.PhysicalReader;
import org.apache.graphar.io.ReadCapability;
import org.apache.graphar.io.ReadReport;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.ReadResult;
import org.apache.graphar.io.Schema;
import org.apache.graphar.storage.InputFile;
import org.apache.graphar.storage.Storage;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/** A storage-backed reader for GraphAr primitive and LIST Parquet fields. */
public final class ParquetPhysicalReader implements PhysicalReader {
    private static final Set<ReadCapability> CAPABILITIES =
            Collections.unmodifiableSet(
                    EnumSet.of(
                            ReadCapability.PROJECTION,
                            ReadCapability.ROW_RANGE,
                            ReadCapability.LIMIT));

    private final Storage storage;
    private final FooterCache footers;

    /**
     * Creates a reader that resolves each request URI through {@code storage} and parses the footer
     * of every file it opens.
     */
    public ParquetPhysicalReader(Storage storage) {
        this(storage, 0);
    }

    /**
     * Creates a reader that keeps at most {@code footerCacheCapacity} Parquet footers in memory, so
     * repeated range reads of one file parse its footer once. A capacity of zero parses the footer
     * on every request.
     *
     * <p>A remembered footer is keyed by URI and file size only. The caller must guarantee that no
     * file read through this reader is rewritten in place while the reader is in use: a file
     * replaced with different content of the same size would be read with a stale footer. Use a
     * non-zero capacity only for immutable, published datasets.
     *
     * @param storage resolves each request URI to a readable file
     * @param footerCacheCapacity maximum number of remembered footers
     */
    public ParquetPhysicalReader(Storage storage, int footerCacheCapacity) {
        this.storage = Objects.requireNonNull(storage, "storage");
        this.footers = new FooterCache(footerCacheCapacity);
    }

    @Override
    public Set<ReadCapability> capabilities() {
        return CAPABILITIES;
    }

    @Override
    public ReadResult read(ReadRequest request) throws IOException {
        Objects.requireNonNull(request, "request");
        if (!request.filters().isEmpty()) {
            throw new UnsupportedOperationException(
                    "Parquet filter pushdown is not implemented; refusing semantic fallback.");
        }
        InputFile inputFile =
                Objects.requireNonNull(storage.inputFile(request.uri()), "storage inputFile");
        ParquetFileReader fileReader = null;
        try {
            fileReader = open(request.uri(), new ParquetInputFile(inputFile));
            MessageType fileSchema = fileReader.getFooter().getFileMetaData().getSchema();
            List<ParquetColumn> fileColumns = columns(fileSchema);
            Map<String, ParquetColumn> columnsByName = byName(fileColumns);
            List<ParquetColumn> outputColumns = outputColumns(request, fileColumns, columnsByName);
            List<ParquetColumn> readColumns = readColumns(fileColumns, outputColumns);
            MessageType readSchema =
                    new MessageType(fileSchema.getName(), parquetTypes(readColumns));
            fileReader.setRequestedSchema(readSchema);

            Schema outputSchema = new Schema(fields(outputColumns));
            ReadReport report = new ReadReport(applied(request), declined(request));
            ParquetBatchCursor cursor =
                    new ParquetBatchCursor(
                            fileReader,
                            fileSchema,
                            readSchema,
                            readColumns,
                            outputColumns,
                            outputSchema,
                            request);
            fileReader = null;
            return new ReadResult(request, cursor, report);
        } finally {
            if (fileReader != null) {
                fileReader.close();
            }
        }
    }

    /**
     * Opens a Parquet reader, reusing a remembered footer when the file is unchanged in size. The
     * returned reader owns the stream opened here and closes it.
     */
    private ParquetFileReader open(URI uri, ParquetInputFile file) throws IOException {
        ParquetReadOptions options = ParquetReadOptions.builder().build();
        long size = file.getLength();
        ParquetMetadata remembered = footers.get(uri, size);
        SeekableInputStream stream = file.newStream();
        try {
            ParquetMetadata footer = remembered;
            if (footer == null) {
                footer = ParquetFileReader.readFooter(file, options, stream);
                if (isPlaintext(footer)) {
                    footers.put(uri, size, footer);
                }
            }
            ParquetFileReader reader = ParquetFileReader.open(file, footer, options, stream);
            stream = null;
            return reader;
        } finally {
            if (stream != null) {
                stream.close();
            }
        }
    }

    /**
     * Returns whether a footer is safe to remember. An encrypted file carries a stateful decryptor
     * inside its metadata, so its footer is parsed again for every reader.
     */
    private static boolean isPlaintext(ParquetMetadata footer) {
        return footer.getFileMetaData().getFileDecryptor() == null;
    }

    private static List<ParquetColumn> columns(MessageType fileSchema) {
        List<ParquetColumn> columns = new ArrayList<>();
        for (Type type : fileSchema.getFields()) {
            if (type.isRepetition(Type.Repetition.REPEATED)
                    || (!type.isPrimitive() && !isList(type))) {
                throw new IllegalArgumentException(
                        "Only primitive or standard LIST Parquet columns are supported: "
                                + type.getName());
            }
            columns.add(new ParquetColumn(type, toField(type)));
        }
        return List.copyOf(columns);
    }

    private static Map<String, ParquetColumn> byName(List<ParquetColumn> columns) {
        Map<String, ParquetColumn> columnsByName = new HashMap<>();
        for (ParquetColumn column : columns) {
            if (columnsByName.put(column.field().name(), column) != null) {
                throw new IllegalArgumentException(
                        "Duplicate Parquet column: " + column.field().name());
            }
        }
        return columnsByName;
    }

    private static List<ParquetColumn> outputColumns(
            ReadRequest request,
            List<ParquetColumn> fileColumns,
            Map<String, ParquetColumn> columnsByName) {
        if (request.projection().isAllColumns()) {
            return fileColumns;
        }
        List<ParquetColumn> result = new ArrayList<>();
        for (ColumnRef reference : request.projection().columns()) {
            ParquetColumn column = columnsByName.get(reference.name());
            if (column == null) {
                throw new IllegalArgumentException("Unknown projection column: " + reference);
            }
            result.add(column);
        }
        return List.copyOf(result);
    }

    private static List<ParquetColumn> readColumns(
            List<ParquetColumn> fileColumns, List<ParquetColumn> outputColumns) {
        Map<String, ParquetColumn> needed = new LinkedHashMap<>();
        for (ParquetColumn column : outputColumns) {
            needed.put(column.field().name(), column);
        }
        List<ParquetColumn> result = new ArrayList<>();
        for (ParquetColumn column : fileColumns) {
            if (needed.containsKey(column.field().name())) {
                result.add(column);
            }
        }
        return List.copyOf(result);
    }

    private static List<Type> parquetTypes(List<ParquetColumn> columns) {
        List<Type> types = new ArrayList<>();
        for (ParquetColumn column : columns) {
            types.add(column.parquetType());
        }
        return types;
    }

    private static List<Field> fields(List<ParquetColumn> columns) {
        List<Field> fields = new ArrayList<>();
        for (ParquetColumn column : columns) {
            fields.add(column.field());
        }
        return fields;
    }

    private static Set<ReadCapability> applied(ReadRequest request) {
        EnumSet<ReadCapability> applied = EnumSet.noneOf(ReadCapability.class);
        if (!request.projection().isAllColumns()) {
            applied.add(ReadCapability.PROJECTION);
        }
        if (request.rowRange().isPresent()) {
            applied.add(ReadCapability.ROW_RANGE);
        }
        if (request.limit().isPresent()) {
            applied.add(ReadCapability.LIMIT);
        }
        return applied;
    }

    private static Set<ReadCapability> declined(ReadRequest request) {
        return Collections.emptySet();
    }

    private static boolean isList(Type type) {
        return type.getLogicalTypeAnnotation()
                instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation;
    }

    private static Field toField(Type type) {
        return new Field(type.getName(), type(type), !type.isRepetition(Type.Repetition.REQUIRED));
    }

    private static ColumnType type(Type type) {
        if (!type.isPrimitive()) {
            return listType(type);
        }
        PrimitiveType primitive = type.asPrimitiveType();
        return primitiveType(primitive);
    }

    private static ColumnType listType(Type type) {
        if (!isList(type) || type.asGroupType().getFieldCount() != 1) {
            throw new IllegalArgumentException("Unsupported Parquet LIST field: " + type);
        }
        Type repeated = type.asGroupType().getType(0);
        if (!repeated.isRepetition(Type.Repetition.REPEATED)
                || repeated.isPrimitive()
                || repeated.asGroupType().getFieldCount() != 1) {
            throw new IllegalArgumentException("Unsupported Parquet LIST field: " + type);
        }
        Type element = repeated.asGroupType().getType(0);
        if (!element.isPrimitive() || element.isRepetition(Type.Repetition.REPEATED)) {
            throw new IllegalArgumentException("Unsupported Parquet LIST field: " + type);
        }
        return ColumnType.listOfElement(
                new Field(
                        "element",
                        primitiveType(element.asPrimitiveType()),
                        element.isRepetition(Type.Repetition.OPTIONAL)));
    }

    private static ColumnType primitiveType(PrimitiveType type) {
        LogicalTypeAnnotation logical = type.getLogicalTypeAnnotation();
        if (logical instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
            requirePhysical(type, PrimitiveType.PrimitiveTypeName.INT32, "DATE");
            return ColumnType.of(ColumnType.Kind.DATE);
        }
        if (logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
            LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestamp =
                    (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logical;
            requirePhysical(type, PrimitiveType.PrimitiveTypeName.INT64, "TIMESTAMP");
            if (timestamp.getUnit() != LogicalTypeAnnotation.TimeUnit.MILLIS) {
                throw unsupported(type, "only millisecond timestamps are supported");
            }
            return ColumnType.of(ColumnType.Kind.TIMESTAMP_MILLIS);
        }
        if (logical instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
            LogicalTypeAnnotation.IntLogicalTypeAnnotation integer =
                    (LogicalTypeAnnotation.IntLogicalTypeAnnotation) logical;
            if (!integer.isSigned()) {
                throw unsupported(type, "unsigned integer logical types are not supported");
            }
            switch (integer.getBitWidth()) {
                case 8:
                    requirePhysical(type, PrimitiveType.PrimitiveTypeName.INT32, "INT(8)");
                    return ColumnType.of(ColumnType.Kind.INT8);
                case 16:
                    requirePhysical(type, PrimitiveType.PrimitiveTypeName.INT32, "INT(16)");
                    return ColumnType.of(ColumnType.Kind.INT16);
                case 32:
                    requirePhysical(type, PrimitiveType.PrimitiveTypeName.INT32, "INT(32)");
                    return ColumnType.of(ColumnType.Kind.INT32);
                case 64:
                    requirePhysical(type, PrimitiveType.PrimitiveTypeName.INT64, "INT(64)");
                    return ColumnType.of(ColumnType.Kind.INT64);
                default:
                    throw unsupported(type, "unsupported integer bit width");
            }
        }
        if (logical instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
            if (type.getPrimitiveTypeName() != PrimitiveType.PrimitiveTypeName.BINARY
                    && type.getPrimitiveTypeName()
                            != PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
                throw unsupported(type, "STRING must use BINARY or FIXED_LEN_BYTE_ARRAY");
            }
            return ColumnType.of(ColumnType.Kind.STRING);
        }
        if (logical != null) {
            throw unsupported(type, "unsupported logical type " + logical);
        }
        switch (type.getPrimitiveTypeName()) {
            case BOOLEAN:
                return ColumnType.of(ColumnType.Kind.BOOLEAN);
            case INT32:
                return ColumnType.of(ColumnType.Kind.INT32);
            case INT64:
                return ColumnType.of(ColumnType.Kind.INT64);
            case FLOAT:
                return ColumnType.of(ColumnType.Kind.FLOAT32);
            case DOUBLE:
                return ColumnType.of(ColumnType.Kind.FLOAT64);
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
                return ColumnType.of(ColumnType.Kind.BINARY);
            default:
                throw unsupported(type, "unsupported primitive type");
        }
    }

    private static void requirePhysical(
            PrimitiveType type, PrimitiveType.PrimitiveTypeName expected, String logicalName) {
        if (type.getPrimitiveTypeName() != expected) {
            throw unsupported(type, logicalName + " must use " + expected);
        }
    }

    private static IllegalArgumentException unsupported(PrimitiveType type, String reason) {
        return new IllegalArgumentException(
                "Unsupported Parquet column " + type.getName() + ": " + reason);
    }

    /**
     * A bounded least-recently-used cache of Parquet footers. A remembered footer is only reused
     * when the file still reports the size it had when the footer was parsed.
     */
    private static final class FooterCache {
        private final int capacity;
        private final LinkedHashMap<URI, Entry> entries;

        private FooterCache(int capacity) {
            if (capacity < 0) {
                throw new IllegalArgumentException(
                        "Parquet footer cache capacity cannot be negative: " + capacity);
            }
            this.capacity = capacity;
            this.entries =
                    new LinkedHashMap<>(16, 0.75f, true) {
                        @Override
                        protected boolean removeEldestEntry(Map.Entry<URI, Entry> eldest) {
                            return size() > FooterCache.this.capacity;
                        }
                    };
        }

        private synchronized ParquetMetadata get(URI uri, long size) {
            if (capacity == 0) {
                return null;
            }
            Entry entry = entries.get(uri);
            if (entry == null || entry.size != size) {
                return null;
            }
            return entry.footer;
        }

        private synchronized void put(URI uri, long size, ParquetMetadata footer) {
            if (capacity == 0) {
                return;
            }
            entries.put(uri, new Entry(size, footer));
        }

        private static final class Entry {
            private final long size;
            private final ParquetMetadata footer;

            private Entry(long size, ParquetMetadata footer) {
                this.size = size;
                this.footer = footer;
            }
        }
    }
}
