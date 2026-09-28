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
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Objects;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.ColumnType;
import org.apache.graphar.io.Field;
import org.apache.graphar.io.PhysicalWriter;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.Schema;
import org.apache.graphar.io.WriteMode;
import org.apache.graphar.io.WriteRequest;
import org.apache.graphar.storage.Storage;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

/** A storage-backed writer for flat, primitive Parquet batches. */
public final class ParquetPhysicalWriter implements PhysicalWriter {
    private static final int INDEXED_PAGE_ROW_COUNT = 1024;

    private final Storage storage;

    /**
     * Maps the request mode onto a Parquet file mode. Parquet cannot append to a closed file
     * without rewriting it, so {@link WriteMode#APPEND} is rejected instead of being downgraded to
     * an overwrite.
     */
    static ParquetFileWriter.Mode fileMode(WriteMode mode) {
        switch (Objects.requireNonNull(mode, "mode")) {
            case CREATE_NEW:
                return ParquetFileWriter.Mode.CREATE;
            case OVERWRITE:
                return ParquetFileWriter.Mode.OVERWRITE;
            default:
                throw new UnsupportedOperationException(
                        "Parquet cannot append to an existing file; write mode " + mode);
        }
    }

    /** Creates a writer that resolves every output URI through {@code storage}. */
    public ParquetPhysicalWriter(Storage storage) {
        this.storage = Objects.requireNonNull(storage, "storage");
    }

    @Override
    public void write(WriteRequest request, BatchCursor batches) throws IOException {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(batches, "batches");
        MessageType parquetSchema = parquetSchema(request.schema());
        ParquetFileWriter.Mode mode = fileMode(request.mode());
        try (ParquetWriter<Group> writer =
                ExampleParquetWriter.builder(
                                new ParquetOutputFile(storage.outputFile(request.uri())))
                        .withType(parquetSchema)
                        .withWriteMode(mode)
                        .withPageRowCountLimit(INDEXED_PAGE_ROW_COUNT)
                        .build()) {
            SimpleGroupFactory groups = new SimpleGroupFactory(parquetSchema);
            while (batches.next()) {
                RecordBatch batch =
                        Objects.requireNonNull(batches.batch(), "batch cursor returned null");
                requireSchema(request.schema(), batch.schema());
                for (int rowIndex = 0; rowIndex < batch.rowCount(); rowIndex++) {
                    writer.write(toGroup(groups, request.schema(), batch, rowIndex));
                }
            }
        } finally {
            batches.close();
        }
    }

    private static MessageType parquetSchema(Schema schema) {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (Field field : schema.fields()) {
            if (field.type().kind() == ColumnType.Kind.LIST) {
                builder.addField(listType(field));
                continue;
            }
            Types.PrimitiveBuilder<Types.GroupBuilder<MessageType>> primitive =
                    builder.primitive(physicalType(field.type()), repetition(field));
            LogicalTypeAnnotation logicalType = logicalType(field.type());
            if (logicalType != null) {
                primitive.as(logicalType);
            }
            primitive.named(field.name());
        }
        return builder.named("graphar");
    }

    private static Type listType(Field field) {
        Field element = field.type().children().get(0);
        return (field.nullable() ? Types.optionalList() : Types.requiredList())
                .element(listElementType(element))
                .named(field.name());
    }

    private static Type listElementType(Field element) {
        Types.PrimitiveBuilder<PrimitiveType> builder =
                Types.primitive(physicalType(element.type()), repetition(element));
        LogicalTypeAnnotation logical = logicalType(element.type());
        if (logical != null) {
            builder.as(logical);
        }
        return builder.named("element");
    }

    private static PrimitiveType.PrimitiveTypeName physicalType(ColumnType type) {
        switch (type.kind()) {
            case BOOLEAN:
                return PrimitiveType.PrimitiveTypeName.BOOLEAN;
            case INT8:
            case INT16:
            case INT32:
            case DATE:
                return PrimitiveType.PrimitiveTypeName.INT32;
            case INT64:
            case TIMESTAMP_MILLIS:
                return PrimitiveType.PrimitiveTypeName.INT64;
            case FLOAT32:
                return PrimitiveType.PrimitiveTypeName.FLOAT;
            case FLOAT64:
                return PrimitiveType.PrimitiveTypeName.DOUBLE;
            case STRING:
            case BINARY:
                return PrimitiveType.PrimitiveTypeName.BINARY;
            case LIST:
            default:
                throw new IllegalArgumentException("Unsupported flat Parquet type: " + type.kind());
        }
    }

    private static Type.Repetition repetition(Field field) {
        return field.nullable() ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED;
    }

    private static LogicalTypeAnnotation logicalType(ColumnType type) {
        switch (type.kind()) {
            case INT8:
                return LogicalTypeAnnotation.intType(8, true);
            case INT16:
                return LogicalTypeAnnotation.intType(16, true);
            case DATE:
                return LogicalTypeAnnotation.dateType();
            case TIMESTAMP_MILLIS:
                return LogicalTypeAnnotation.timestampType(
                        true, LogicalTypeAnnotation.TimeUnit.MILLIS);
            case STRING:
                return LogicalTypeAnnotation.stringType();
            default:
                return null;
        }
    }

    private static void requireSchema(Schema expected, Schema actual) {
        List<Field> expectedFields = expected.fields();
        List<Field> actualFields = actual.fields();
        if (expectedFields.size() != actualFields.size()) {
            throw new IllegalArgumentException("Record batch schema does not match write request.");
        }
        for (int index = 0; index < expectedFields.size(); index++) {
            Field left = expectedFields.get(index);
            Field right = actualFields.get(index);
            if (!left.name().equals(right.name())
                    || !left.type().equals(right.type())
                    || left.nullable() != right.nullable()) {
                throw new IllegalArgumentException(
                        "Record batch schema does not match write request.");
            }
        }
    }

    private static Group toGroup(
            SimpleGroupFactory groups, Schema schema, RecordBatch batch, int rowIndex) {
        Group group = groups.newGroup();
        for (int index = 0; index < schema.fields().size(); index++) {
            Field field = schema.fields().get(index);
            Object value = batch.column(index).getObject(rowIndex);
            if (value == null) {
                if (!field.nullable()) {
                    throw new IllegalArgumentException("Required field is null: " + field.name());
                }
                continue;
            }
            add(group, field, value);
        }
        return group;
    }

    private static void add(Group group, Field field, Object value) {
        String name = field.name();
        switch (field.type().kind()) {
            case BOOLEAN:
                group.add(name, require(value, Boolean.class, name));
                return;
            case INT8:
            case INT16:
            case INT32:
                group.add(name, require(value, Number.class, name).intValue());
                return;
            case INT64:
                group.add(name, require(value, Number.class, name).longValue());
                return;
            case FLOAT32:
                group.add(name, require(value, Number.class, name).floatValue());
                return;
            case FLOAT64:
                group.add(name, require(value, Number.class, name).doubleValue());
                return;
            case STRING:
                group.add(name, Binary.fromString(require(value, String.class, name)));
                return;
            case BINARY:
                ByteBuffer bytes = require(value, ByteBuffer.class, name).asReadOnlyBuffer();
                byte[] copy = new byte[bytes.remaining()];
                bytes.get(copy);
                group.add(name, Binary.fromConstantByteArray(copy));
                return;
            case DATE:
                group.add(
                        name, Math.toIntExact(require(value, LocalDate.class, name).toEpochDay()));
                return;
            case TIMESTAMP_MILLIS:
                group.add(name, require(value, Instant.class, name).toEpochMilli());
                return;
            case LIST:
                addList(group, field, value);
                return;
            default:
                throw new IllegalArgumentException(
                        "Unsupported Parquet type: " + field.type().kind());
        }
    }

    private static void addList(Group group, Field field, Object value) {
        if (!(value instanceof List<?>)) {
            throw new IllegalArgumentException(
                    "Unexpected value for " + field.name() + ": expected List");
        }
        Group list = group.addGroup(field.name());
        Field element = field.type().children().get(0);
        for (Object elementValue : (List<?>) value) {
            Group repeated = list.addGroup("list");
            if (elementValue != null) {
                add(repeated, element, elementValue);
            } else if (!element.nullable()) {
                throw new IllegalArgumentException(
                        "Required LIST element is null: " + field.name());
            }
        }
    }

    private static <T> T require(Object value, Class<T> type, String name) {
        if (!type.isInstance(value)) {
            throw new IllegalArgumentException(
                    "Unexpected value for " + name + ": expected " + type.getSimpleName());
        }
        return type.cast(value);
    }
}
