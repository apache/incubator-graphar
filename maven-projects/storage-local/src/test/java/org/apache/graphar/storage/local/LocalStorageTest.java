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

package org.apache.graphar.storage.local;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.graphar.storage.InputFile;
import org.apache.graphar.storage.OutputFile;
import org.apache.graphar.storage.PositionOutput;
import org.apache.graphar.storage.SeekableInput;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LocalStorageTest {
    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final LocalStorage storage = new LocalStorage();

    @Test
    public void writesReadsAndSeeksAcrossNestedFile() throws IOException {
        Path path = temporaryFolder.getRoot().toPath().resolve("nested/data.bin");
        OutputFile outputFile = storage.outputFile(path.toUri());
        assertFalse(storage.exists(path.toUri()));

        try (PositionOutput output = outputFile.create()) {
            output.write("ab".getBytes(UTF_8));
            ByteBuffer direct = ByteBuffer.allocateDirect(2);
            direct.put("cd".getBytes(UTF_8));
            direct.flip();
            output.write(direct);
            ByteBuffer heap = ByteBuffer.wrap("00efgh".getBytes(UTF_8));
            heap.position(2);
            heap.limit(6);
            ByteBuffer slicedHeap = heap.slice();
            slicedHeap.position(1);
            slicedHeap.limit(3);
            output.write(slicedHeap);
            output.flush();
            assertEquals(6, output.position());
            assertEquals(2, direct.position());
            assertEquals(3, slicedHeap.position());
        }

        assertTrue(storage.exists(path.toUri()));
        InputFile inputFile = storage.inputFile(path.toUri());
        assertEquals(path.toUri(), inputFile.uri());
        assertEquals(6, inputFile.size());
        try (SeekableInput input = inputFile.open()) {
            ByteBuffer allBytes = ByteBuffer.allocate(6);
            input.readFully(allBytes);
            assertEquals(6, input.position());
            assertArrayEquals("abcdfg".getBytes(UTF_8), allBytes.array());

            input.seek(1);
            ByteBuffer suffix = ByteBuffer.allocate(2);
            input.readFully(suffix);
            assertArrayEquals("bc".getBytes(UTF_8), suffix.array());
            assertEquals(3, input.position());
        }
    }

    @Test
    public void createDoesNotOverwriteAndCreateOrOverwriteTruncates() throws IOException {
        Path path = temporaryFolder.newFile("existing.bin").toPath();
        Files.write(path, "old".getBytes(UTF_8));
        OutputFile outputFile = storage.outputFile(path.toUri());

        assertThrows(IOException.class, outputFile::create);
        assertArrayEquals("old".getBytes(UTF_8), Files.readAllBytes(path));

        try (PositionOutput output = outputFile.createOrOverwrite()) {
            output.write("new".getBytes(UTF_8));
        }
        assertArrayEquals("new".getBytes(UTF_8), Files.readAllBytes(path));
    }

    @Test
    public void rejectsNonFileUrisAndNegativeSeekPositions() throws IOException {
        assertThrows(
                IllegalArgumentException.class,
                () -> storage.inputFile(URI.create("s3://bucket/a")));
        assertThrows(
                IllegalArgumentException.class,
                () -> storage.inputFile(URI.create("relative/path")));

        Path path = temporaryFolder.newFile("data.bin").toPath();
        try (SeekableInput input = storage.inputFile(path.toUri()).open()) {
            assertThrows(IllegalArgumentException.class, () -> input.seek(-1));
        }
    }

    @Test
    public void readFullyFailsWhenTheFileIsShorterThanTheDestination() throws IOException {
        Path path = temporaryFolder.newFile("short.bin").toPath();
        Files.write(path, "x".getBytes(UTF_8));

        try (SeekableInput input = storage.inputFile(path.toUri()).open()) {
            assertThrows(IOException.class, () -> input.readFully(ByteBuffer.allocate(2)));
        }
    }

    @Test
    public void writesADirectBufferLargerThanOneTransferChunk() throws IOException {
        Path path = temporaryFolder.getRoot().toPath().resolve("large.bin");
        byte[] expected = new byte[20_000];
        for (int index = 0; index < expected.length; index++) {
            expected[index] = (byte) index;
        }
        ByteBuffer direct = ByteBuffer.allocateDirect(expected.length);
        direct.put(expected);
        direct.flip();

        try (PositionOutput output = storage.outputFile(path.toUri()).create()) {
            output.write(direct);
            assertEquals(expected.length, output.position());
        }

        assertFalse(direct.hasRemaining());
        assertArrayEquals(expected, Files.readAllBytes(path));
    }
}
