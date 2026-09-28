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
package org.apache.graphar.io;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A {@link ValueVector} over boxed values held in memory.
 *
 * <p>This is the vector a producer uses when it assembles a batch from Java objects rather than
 * decoding one from a file. Every value is checked against the vector {@link Field} at
 * construction, so a malformed value fails here, naming the column and index, instead of later
 * inside a format encoder. The accepted Java representations are:
 *
 * <ul>
 *   <li>{@code BOOLEAN}: {@link Boolean}
 *   <li>{@code INT8}, {@code INT16}, {@code INT32}, {@code INT64}: {@link Byte}, {@link Short},
 *       {@link Integer} or {@link Long} whose value fits the kind
 *   <li>{@code FLOAT32}, {@code FLOAT64}: {@link Float} or {@link Double}
 *   <li>{@code STRING}: {@link String}
 *   <li>{@code BINARY}: {@link ByteBuffer}
 *   <li>{@code DATE}: {@link LocalDate}
 *   <li>{@code TIMESTAMP_MILLIS}: {@link Instant}
 *   <li>{@code LIST}: {@link List} whose elements follow the element field, including its
 *       nullability
 * </ul>
 *
 * <p>Other kinds are checked for nullability only. List values are snapshotted recursively into
 * unmodifiable copies that may hold null elements, so neither the producer nor a consumer can
 * mutate the vector afterwards at any nesting depth. Binary values are held as given.
 */
public final class ObjectValueVector implements ValueVector {
    private final Field field;
    private final List<Object> values;

    /**
     * Creates a vector over {@code values}.
     *
     * @throws IllegalArgumentException if a value is null in a non-nullable field or does not have
     *     a Java type accepted for the field type
     */
    public ObjectValueVector(Field field, List<?> values) {
        this.field = Objects.requireNonNull(field, "A vector field cannot be null.");
        Objects.requireNonNull(values, "Vector values cannot be null.");
        List<Object> copy = new ArrayList<>(values.size());
        for (int index = 0; index < values.size(); index++) {
            copy.add(checked(field, values.get(index), field.name(), index));
        }
        this.values = Collections.unmodifiableList(copy);
    }

    @Override
    public Field field() {
        return field;
    }

    @Override
    public int valueCount() {
        return values.size();
    }

    @Override
    public boolean isNull(int index) {
        return values.get(index) == null;
    }

    @Override
    public Object getObject(int index) {
        return values.get(index);
    }

    private static Object checked(Field field, Object value, String column, int index) {
        if (value == null) {
            if (!field.nullable()) {
                throw new IllegalArgumentException(
                        "Column '"
                                + column
                                + "' index "
                                + index
                                + ": null in a non-nullable field");
            }
            return null;
        }
        ColumnType type = field.type();
        switch (type.kind()) {
            case BOOLEAN:
                return require(value, column, index, type, Boolean.class);
            case INT8:
            case INT16:
            case INT32:
            case INT64:
                return integer(
                        type,
                        require(
                                value,
                                column,
                                index,
                                type,
                                Byte.class,
                                Short.class,
                                Integer.class,
                                Long.class),
                        column,
                        index);
            case FLOAT32:
            case FLOAT64:
                return require(value, column, index, type, Float.class, Double.class);
            case STRING:
                return require(value, column, index, type, String.class);
            case BINARY:
                return require(value, column, index, type, ByteBuffer.class);
            case DATE:
                return require(value, column, index, type, LocalDate.class);
            case TIMESTAMP_MILLIS:
                return require(value, column, index, type, Instant.class);
            case LIST:
                return list(type, require(value, column, index, type, List.class), column, index);
            default:
                return value;
        }
    }

    private static Number integer(ColumnType type, Number value, String column, int index) {
        long bound;
        switch (type.kind()) {
            case INT8:
                bound = Byte.MAX_VALUE;
                break;
            case INT16:
                bound = Short.MAX_VALUE;
                break;
            case INT32:
                bound = Integer.MAX_VALUE;
                break;
            default:
                return value;
        }
        long actual = value.longValue();
        if (actual > bound || actual < -bound - 1) {
            throw new IllegalArgumentException(
                    "Column '"
                            + column
                            + "' index "
                            + index
                            + ": "
                            + actual
                            + " is out of range for "
                            + type.kind());
        }
        return value;
    }

    private static List<Object> list(ColumnType type, List<?> value, String column, int index) {
        Field element = type.children().get(0);
        List<Object> copy = new ArrayList<>(value.size());
        for (Object elementValue : value) {
            copy.add(checked(element, elementValue, column, index));
        }
        return Collections.unmodifiableList(copy);
    }

    @SafeVarargs
    private static <T> T require(
            Object value,
            String column,
            int index,
            ColumnType type,
            Class<? extends T>... accepted) {
        for (Class<? extends T> candidate : accepted) {
            if (candidate.isInstance(value)) {
                return candidate.cast(value);
            }
        }
        throw new IllegalArgumentException(
                "Column '"
                        + column
                        + "' index "
                        + index
                        + ": "
                        + value.getClass().getSimpleName()
                        + " is not a "
                        + type.kind()
                        + " value");
    }
}
