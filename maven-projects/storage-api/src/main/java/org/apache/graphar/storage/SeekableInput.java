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

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/** A readable stream whose position can be changed without reopening the file. */
public interface SeekableInput extends Closeable {
    /** Returns the current byte offset. */
    long position() throws IOException;

    /** Moves this stream to {@code newPosition}. */
    void seek(long newPosition) throws IOException;

    /**
     * Reads bytes into {@code destination}.
     *
     * <p>Implementations must return a positive byte count while {@code destination} has remaining
     * capacity, or {@code -1} at end of input. Implementations that wrap a non-blocking source must
     * wait or retry internally rather than return zero.
     */
    int read(ByteBuffer destination) throws IOException;

    /** Reads until {@code destination} is full or throws on end of input. */
    default void readFully(ByteBuffer destination) throws IOException {
        while (destination.hasRemaining()) {
            int bytesRead = read(destination);
            if (bytesRead < 0) {
                throw new EOFException("Reached end of input before filling destination.");
            }
            if (bytesRead == 0) {
                throw new IOException("Seekable input made no progress while reading.");
            }
        }
    }
}
