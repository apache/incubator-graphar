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

package org.apache.graphar.reader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.graphar.info.loader.impl.LocalFileSystemStringGraphInfoLoader;
import org.apache.graphar.io.ColumnRef;
import org.apache.graphar.io.ReadReport;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.RowRange;
import org.apache.graphar.io.parquet.ParquetPhysicalReader;
import org.apache.graphar.storage.local.LocalStorage;
import org.junit.Test;

/**
 * Reads the canonical {@code testing/ldbc_sample/parquet} dataset, written by the C++ and Spark
 * implementations, and compares it with values read independently through Arrow C++.
 */
public class GraphReaderFixtureTest {
    static final Path FIXTURE = Path.of("..", "..", "testing", "ldbc_sample", "parquet");

    static final List<Long> NEIGHBORS_OF_297 =
            List.of(
                    4L, 25L, 28L, 45L, 58L, 62L, 74L, 84L, 104L, 105L, 126L, 130L, 169L, 180L, 197L,
                    201L, 231L, 252L, 262L, 271L, 273L, 300L, 307L, 324L, 345L, 357L, 385L, 425L,
                    468L, 470L, 507L, 538L, 540L, 544L, 550L, 566L, 576L, 587L, 604L, 614L, 622L,
                    623L, 652L, 671L, 678L, 698L, 749L, 756L, 777L, 840L, 851L, 878L, 884L);

    private final CapturingPhysicalReader physicalReader =
            new CapturingPhysicalReader(new ParquetPhysicalReader(new LocalStorage()));

    @Test
    public void scansEveryTopologyRowInSourceOrder() throws Exception {
        OrderedSourceEdgeReader edges = graph(FIXTURE).edge("person", "knows", "person");

        assertEquals(903L, edges.vertexCount());
        assertEquals(6626L, edges.edgeCount());
        List<Long> neighborsOf297 = new ArrayList<>();
        long rows = 0;
        long previousSource = 0;
        try (EdgeCursor cursor = edges.scanEdges()) {
            while (cursor.next()) {
                GraphEdge edge = cursor.edge();
                if (rows++ == 0) {
                    assertEquals(0L, edge.source());
                    assertEquals(87L, edge.destination());
                }
                assertTrue(edge.source() >= previousSource);
                previousSource = edge.source();
                if (edge.source() == 297) {
                    neighborsOf297.add(edge.destination());
                }
                assertTrue(edge.properties().isEmpty());
            }
            assertFullyApplied(cursor.reports());
        }

        assertEquals(6626L, rows);
        assertEquals(901L, previousSource);
        assertEquals(NEIGHBORS_OF_297, neighborsOf297);
        assertEquals(11, physicalReader.requests.size());
        assertRequest(
                physicalReader.requests.get(3),
                FIXTURE,
                "edge/person_knows_person/ordered_by_source/adj_list/part2/chunk1",
                List.of(EdgeCursor.SOURCE_COLUMN, EdgeCursor.DESTINATION_COLUMN),
                new RowRange(0, 53));
    }

    @Test
    public void joinsEdgePropertiesWithTheirTopologyRows() throws Exception {
        OrderedSourceEdgeReader edges = graph(FIXTURE).edge("person", "knows", "person");

        List<String> datesOf297 = new ArrayList<>();
        long rows = 0;
        try (EdgeCursor cursor = edges.scanEdges(List.of("creationDate"))) {
            while (cursor.next()) {
                GraphEdge edge = cursor.edge();
                if (rows++ == 0) {
                    assertEquals(
                            Map.of("creationDate", "2010-07-30T15:19:53.298+0000"),
                            edge.properties());
                }
                if (edge.source() == 297) {
                    datesOf297.add((String) edge.properties().get("creationDate"));
                }
            }
            assertFullyApplied(cursor.reports());
        }

        assertEquals(6626L, rows);
        assertEquals(53, datesOf297.size());
        assertEquals("2012-03-05T05:51:04.681+0000", datesOf297.get(0));
        assertEquals("2012-03-07T06:38:36.410+0000", datesOf297.get(52));
        assertEquals(22, physicalReader.requests.size());
        assertRequest(
                physicalReader.requests.get(7),
                FIXTURE,
                "edge/person_knows_person/ordered_by_source/creationDate/part2/chunk1",
                List.of("creationDate"),
                new RowRange(0, 53));
    }

    @Test
    public void rejectsUndeclaredPropertiesBeforeReading() throws Exception {
        GraphReader graph = graph(FIXTURE);

        assertThrows(
                IllegalArgumentException.class,
                () -> graph.edge("person", "knows", "person").scanEdges(List.of("missing")));
        assertTrue(physicalReader.requests.isEmpty());
    }

    GraphReader graph(Path root) throws IOException {
        return GraphReader.open(
                root.resolve("ldbc_sample.graph.yml").toUri(),
                new LocalFileSystemStringGraphInfoLoader(),
                new LocalStorage(),
                physicalReader);
    }

    static void assertFullyApplied(List<ReadReport> reports) {
        assertTrue(!reports.isEmpty());
        for (ReadReport report : reports) {
            assertTrue(report.declined().isEmpty());
        }
    }

    static void assertRequest(
            ReadRequest request, Path root, String path, List<String> columns, RowRange range) {
        assertEquals(
                root.toAbsolutePath().normalize().resolve(path),
                Path.of(request.uri()).normalize());
        assertEquals(
                columns.stream().map(ColumnRef::of).collect(Collectors.toList()),
                request.projection().columns());
        assertEquals(range, request.rowRange().orElseThrow());
    }
}
