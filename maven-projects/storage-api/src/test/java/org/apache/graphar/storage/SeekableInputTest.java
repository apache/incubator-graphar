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

package org.apache.graphar.storage;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.Test;

public class SeekableInputTest {
    @Test
    public void readFullyCombinesPartialReads() throws IOException {
        ByteBuffer destination = ByteBuffer.allocate(3);
        try (SeekableInput input = new StubSeekableInput(bytes("a"), bytes("bc"))) {
            input.readFully(destination);
            assertEquals(3, input.position());
        }
        assertArrayEquals(bytes("abc"), destination.array());
    }

    @Test
    public void readFullyFailsAtEndOfInput() throws IOException {
        try (SeekableInput input = new StubSeekableInput(bytes("a"), null)) {
            assertThrows(IOException.class, () -> input.readFully(ByteBuffer.allocate(2)));
        }
    }

    @Test
    public void readFullyFailsOnZeroProgress() throws IOException {
        try (SeekableInput input = new StubSeekableInput(new byte[0])) {
            assertThrows(IOException.class, () -> input.readFully(ByteBuffer.allocate(1)));
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(UTF_8);
    }

    private static final class StubSeekableInput implements SeekableInput {
        private final byte[][] reads;
        private int readIndex;
        private long position;

        StubSeekableInput(byte[]... reads) {
            this.reads = reads;
        }

        @Override
        public long position() {
            return position;
        }

        @Override
        public void seek(long newPosition) {
            position = newPosition;
        }

        @Override
        public int read(ByteBuffer destination) {
            byte[] bytes = reads[readIndex++];
            if (bytes == null) {
                return -1;
            }
            destination.put(bytes);
            position += bytes.length;
            return bytes.length;
        }

        @Override
        public void close() {}
    }
}
