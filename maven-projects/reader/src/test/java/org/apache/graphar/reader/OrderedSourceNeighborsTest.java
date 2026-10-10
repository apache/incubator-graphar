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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.graphar.info.loader.impl.LocalFileSystemStringGraphInfoLoader;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.RowRange;
import org.apache.graphar.io.Schema;
import org.apache.graphar.io.WriteMode;
import org.apache.graphar.io.WriteRequest;
import org.apache.graphar.io.parquet.ParquetPhysicalReader;
import org.apache.graphar.io.parquet.ParquetPhysicalWriter;
import org.apache.graphar.storage.local.LocalStorage;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Reads single-vertex adjacency from a copy of the canonical {@code person_knows_person} edges.
 *
 * <p>The canonical edge chunks were written by Arrow C++ without a Parquet Offset Index, and the
 * Parquet backend refuses a partial row-group read without one. The copy holds the same rows
 * rewritten by {@link ParquetPhysicalWriter}, which emits Offset Indexes, so the reader's minimal
 * requests can be served and compared with a scan of the canonical files.
 */
public class OrderedSourceNeighborsTest {
    private static final String EDGES = "edge/person_knows_person/ordered_by_source/";
    private static Path copy;

    private final CountingStorage storage = new CountingStorage(new LocalStorage());
    private final CapturingPhysicalReader physicalReader =
            new CapturingPhysicalReader(new ParquetPhysicalReader(storage));

    @BeforeClass
    public static void rewriteCanonicalEdgesWithOffsetIndexes() throws IOException {
        copy = Files.createTempDirectory("graphar-reader-");
        Path canonical = GraphReaderFixtureTest.FIXTURE;
        for (String file :
                List.of(
                        "ldbc_sample.graph.yml",
                        "person.vertex.yml",
                        "person_knows_person.edge.yml")) {
            Files.copy(canonical.resolve(file), copy.resolve(file));
        }
        Files.createDirectories(copy.resolve(EDGES));
        try (Stream<Path> files = Files.list(canonical.resolve(EDGES))) {
            for (Path file : files.filter(Files::isRegularFile).collect(Collectors.toList())) {
                Files.copy(file, copy.resolve(EDGES).resolve(file.getFileName().toString()));
            }
        }
        for (String directory : List.of("offset", "adj_list", "creationDate")) {
            Path source = canonical.resolve(EDGES).resolve(directory);
            try (Stream<Path> files = Files.walk(source)) {
                for (Path file : files.filter(Files::isRegularFile).collect(Collectors.toList())) {
                    rewrite(
                            file,
                            copy.resolve(EDGES)
                                    .resolve(directory)
                                    .resolve(source.relativize(file).toString()));
                }
            }
        }
    }

    @AfterClass
    public static void deleteCopy() throws IOException {
        try (Stream<Path> files = Files.walk(copy)) {
            for (Path file : files.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
                Files.delete(file);
            }
        }
    }

    @Test
    public void readsOnlyTheOffsetPairAndTheDestinationRowsOfOneVertex() throws Exception {
        OrderedSourceEdgeReader edges = edges();

        List<Long> neighbors;
        try (EdgeCursor cursor = edges.neighbors(297)) {
            neighbors = destinations(cursor);
            GraphReaderFixtureTest.assertFullyApplied(cursor.reports());
        }

        assertEquals(GraphReaderFixtureTest.NEIGHBORS_OF_297, neighbors);
        assertEquals(
                List.of(
                        uri(EDGES + "offset/chunk2"),
                        uri(EDGES + "adj_list/part2/chunk0"),
                        uri(EDGES + "adj_list/part2/chunk1")),
                storage.inputs);
        GraphReaderFixtureTest.assertRequest(
                physicalReader.requests.get(0),
                copy,
                EDGES + "offset/chunk2",
                List.of("_graphArOffset"),
                new RowRange(97, 99));
        GraphReaderFixtureTest.assertRequest(
                physicalReader.requests.get(1),
                copy,
                EDGES + "adj_list/part2/chunk0",
                List.of(EdgeCursor.DESTINATION_COLUMN),
                new RowRange(1008, 1024));
        GraphReaderFixtureTest.assertRequest(
                physicalReader.requests.get(2),
                copy,
                EDGES + "adj_list/part2/chunk1",
                List.of(EdgeCursor.DESTINATION_COLUMN),
                new RowRange(0, 37));
    }

    @Test
    public void agreesWithAScanOfTheCanonicalFilesForEveryVertex() throws Exception {
        Map<Long, List<Long>> expected = new HashMap<>();
        try (EdgeCursor cursor =
                new GraphReaderFixtureTest()
                        .graph(GraphReaderFixtureTest.FIXTURE)
                        .edge("person", "knows", "person")
                        .scanEdges()) {
            while (cursor.next()) {
                expected.computeIfAbsent(cursor.edge().source(), unused -> new ArrayList<>())
                        .add(cursor.edge().destination());
            }
        }
        OrderedSourceEdgeReader edges = edges();

        long total = 0;
        for (long vertex = 0; vertex < edges.vertexCount(); vertex++) {
            try (EdgeCursor cursor = edges.neighbors(vertex)) {
                List<Long> neighbors = destinations(cursor);
                assertEquals(expected.getOrDefault(vertex, List.of()), neighbors);
                total += neighbors.size();
            }
        }
        assertEquals(6626L, total);
    }

    @Test
    public void joinsEdgePropertiesOfOneSourceVertex() throws Exception {
        List<GraphEdge> read = new ArrayList<>();
        try (EdgeCursor cursor = edges().edges(297, List.of("creationDate"))) {
            while (cursor.next()) {
                read.add(cursor.edge());
            }
        }

        assertEquals(53, read.size());
        for (GraphEdge edge : read) {
            assertEquals(297L, edge.source());
        }
        assertEquals("2012-03-05T05:51:04.681+0000", read.get(0).properties().get("creationDate"));
        assertEquals("2012-03-07T06:38:36.410+0000", read.get(52).properties().get("creationDate"));
        assertEquals(5, physicalReader.requests.size());
        GraphReaderFixtureTest.assertRequest(
                physicalReader.requests.get(4),
                copy,
                EDGES + "creationDate/part2/chunk1",
                List.of("creationDate"),
                new RowRange(0, 37));
    }

    @Test
    public void opensNoAdjacencyChunkForAVertexWithoutEdges() throws Exception {
        try (EdgeCursor cursor = edges().neighbors(200)) {
            assertFalse(cursor.next());
        }

        assertEquals(List.of(uri(EDGES + "offset/chunk2")), storage.inputs);
    }

    @Test
    public void rejectsAVertexBeyondTheLastOffsetPair() throws Exception {
        IllegalArgumentException failure =
                assertThrows(IllegalArgumentException.class, () -> edges().neighbors(903));

        assertTrue(failure.getMessage(), failure.getMessage().contains("no offset pair"));
    }

    private OrderedSourceEdgeReader edges() throws IOException {
        return GraphReader.open(
                        copy.resolve("ldbc_sample.graph.yml").toUri(),
                        new LocalFileSystemStringGraphInfoLoader(),
                        storage,
                        physicalReader)
                .edge("person", "knows", "person");
    }

    private static URI uri(String path) {
        return copy.toUri().resolve(path);
    }

    private static List<Long> destinations(EdgeCursor cursor) throws IOException {
        List<Long> destinations = new ArrayList<>();
        while (cursor.next()) {
            destinations.add(cursor.edge().destination());
        }
        return destinations;
    }

    private static void rewrite(Path source, Path target) throws IOException {
        LocalStorage local = new LocalStorage();
        ParquetPhysicalReader reader = new ParquetPhysicalReader(local);
        Schema schema;
        try (BatchCursor cursor =
                reader.read(ReadRequest.builder(source.toUri()).build()).cursor()) {
            assertTrue(cursor.next());
            schema = cursor.batch().schema();
        }
        Files.createDirectories(target.getParent());
        new ParquetPhysicalWriter(local)
                .write(
                        new WriteRequest(target.toUri(), schema, WriteMode.CREATE_NEW),
                        reader.read(ReadRequest.builder(source.toUri()).build()).cursor());
    }
}
