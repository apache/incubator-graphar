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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Builds {@link RecordBatch} instances from values a producer holds row by row. */
public final class RecordBatches {
    private RecordBatches() {}

    /**
     * Transposes row-major values into a columnar batch.
     *
     * @param schema the batch schema; every row must hold one value per field
     * @param rows the rows, each a list of boxed values in schema order
     * @return a batch backed by {@link ObjectValueVector} columns
     * @throws IllegalArgumentException if a row has the wrong width, or a value is null in a
     *     non-nullable field or has a Java type the field does not accept; see {@link
     *     ObjectValueVector}
     */
    public static RecordBatch ofRows(Schema schema, List<? extends List<?>> rows) {
        Objects.requireNonNull(schema, "A batch schema cannot be null.");
        Objects.requireNonNull(rows, "Batch rows cannot be null.");
        int width = schema.fields().size();
        List<List<Object>> valuesByColumn = new ArrayList<>(width);
        for (int column = 0; column < width; column++) {
            valuesByColumn.add(new ArrayList<>(rows.size()));
        }
        for (List<?> row : rows) {
            Objects.requireNonNull(row, "A batch row cannot be null.");
            if (row.size() != width) {
                throw new IllegalArgumentException("A batch row does not match the schema width.");
            }
            for (int column = 0; column < width; column++) {
                valuesByColumn.get(column).add(row.get(column));
            }
        }
        List<ValueVector> columns = new ArrayList<>(width);
        for (int column = 0; column < width; column++) {
            columns.add(
                    new ObjectValueVector(schema.fields().get(column), valuesByColumn.get(column)));
        }
        return new VectorRecordBatch(schema, columns, rows.size());
    }

    /**
     * Transposes row-major arrays into a columnar batch.
     *
     * @param schema the batch schema; every row must hold one value per field
     * @param rows the rows, each an array of boxed values in schema order
     * @return a batch backed by {@link ObjectValueVector} columns
     * @throws IllegalArgumentException under the same conditions as {@link #ofRows(Schema, List)}
     */
    public static RecordBatch ofArrays(Schema schema, List<Object[]> rows) {
        Objects.requireNonNull(rows, "Batch rows cannot be null.");
        List<List<Object>> lists = new ArrayList<>(rows.size());
        for (Object[] row : rows) {
            Objects.requireNonNull(row, "A batch row cannot be null.");
            List<Object> list = new ArrayList<>(row.length);
            for (Object value : row) {
                list.add(value);
            }
            lists.add(list);
        }
        return ofRows(schema, lists);
    }
}
