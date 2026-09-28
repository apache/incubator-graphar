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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class RecordBatchesTest {
    private static final Schema SCHEMA =
            new Schema(
                    List.of(
                            new Field("id", ColumnType.of(ColumnType.Kind.INT64), false),
                            new Field("name", ColumnType.of(ColumnType.Kind.STRING), true)));

    @Test
    public void transposesRowsIntoColumns() {
        RecordBatch batch =
                RecordBatches.ofRows(
                        SCHEMA, List.of(Arrays.asList(1L, "a"), Arrays.asList(2L, null)));

        assertEquals(2, batch.rowCount());
        assertEquals(2, batch.columnCount());
        assertEquals(1L, batch.column(0).getObject(0));
        assertEquals(2L, batch.column(0).getObject(1));
        assertEquals("a", batch.column(1).getObject(0));
        assertTrue(batch.column(1).isNull(1));
        assertFalse(batch.column(0).isNull(1));
        assertEquals(SCHEMA.fields().get(1), batch.column(1).field());
    }

    @Test
    public void acceptsArrays() {
        RecordBatch batch =
                RecordBatches.ofArrays(SCHEMA, List.<Object[]>of(new Object[] {1L, "a"}));

        assertEquals(1, batch.rowCount());
        assertEquals("a", batch.column(1).getObject(0));
    }

    @Test
    public void buildsAnEmptyBatch() {
        RecordBatch batch = RecordBatches.ofRows(SCHEMA, List.of());

        assertEquals(0, batch.rowCount());
        assertEquals(0, batch.column(0).valueCount());
    }

    @Test
    public void rejectsARowOfTheWrongWidth() {
        assertThrows(
                IllegalArgumentException.class,
                () -> RecordBatches.ofRows(SCHEMA, List.of(List.of(1L))));
    }

    @Test
    public void copiesListValuesOnRead() {
        Field field =
                new Field("tags", ColumnType.listOf(ColumnType.of(ColumnType.Kind.STRING)), true);
        List<Object> tags = new ArrayList<>(List.of("x"));
        ObjectValueVector vector = new ObjectValueVector(field, List.of(tags));
        tags.add("y");

        assertEquals(List.of("x"), vector.getObject(0));
        assertThrows(
                UnsupportedOperationException.class,
                () -> ((List<Object>) vector.getObject(0)).add("z"));
    }

    @Test
    public void keepsNullElementsOfANullableElementList() {
        Field field =
                new Field("tags", ColumnType.listOf(ColumnType.of(ColumnType.Kind.STRING)), true);

        ObjectValueVector vector = new ObjectValueVector(field, List.of(Arrays.asList("x", null)));

        assertEquals(Arrays.asList("x", null), vector.getObject(0));
    }

    @Test
    public void rejectsANullElementOfARequiredElementList() {
        Field field =
                new Field(
                        "tags",
                        ColumnType.listOfElement(
                                new Field("element", ColumnType.of(ColumnType.Kind.STRING), false)),
                        true);

        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> new ObjectValueVector(field, List.of(Arrays.asList("x", null))));
        assertTrue(error.getMessage(), error.getMessage().contains("'tags' index 0"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void snapshotsNestedListsAtEveryDepth() {
        Field field =
                new Field(
                        "matrix",
                        ColumnType.listOf(ColumnType.listOf(ColumnType.of(ColumnType.Kind.INT64))),
                        true);
        List<Object> inner = new ArrayList<>(List.of(1L));
        ObjectValueVector vector = new ObjectValueVector(field, List.of(List.of(inner)));
        inner.add(2L);

        List<Object> outer = (List<Object>) vector.getObject(0);
        assertEquals(List.of(List.of(1L)), outer);
        assertThrows(
                UnsupportedOperationException.class, () -> ((List<Object>) outer.get(0)).add(3L));
    }

    @Test
    public void rejectsANullInANonNullableColumn() {
        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                RecordBatches.ofRows(
                                        SCHEMA,
                                        List.of(Arrays.asList(1L, "a"), Arrays.asList(null, "b"))));
        assertTrue(error.getMessage(), error.getMessage().contains("'id' index 1"));
    }

    @Test
    public void rejectsAValueOfTheWrongType() {
        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> RecordBatches.ofRows(SCHEMA, List.of(Arrays.asList("1", "a"))));
        assertTrue(error.getMessage(), error.getMessage().contains("String is not a INT64"));
    }

    @Test
    public void rejectsAnIntegerOutsideItsKind() {
        Field field = new Field("small", ColumnType.of(ColumnType.Kind.INT8), false);

        assertThrows(
                IllegalArgumentException.class, () -> new ObjectValueVector(field, List.of(128)));
        assertEquals((byte) -128, new ObjectValueVector(field, List.of((byte) -128)).getObject(0));
    }
}
