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
import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;

/** A sequential output with a byte position suitable for file-format writers. */
public interface PositionOutput extends Closeable, Flushable {
    /** Returns the number of bytes written to this output. */
    long position() throws IOException;

    /** Writes bytes from {@code source}, advancing its position by the bytes written. */
    void write(ByteBuffer source) throws IOException;

    /** Writes bytes from {@code source}. */
    void write(byte[] source, int offset, int length) throws IOException;

    /** Writes all bytes from {@code source}. */
    default void write(byte[] source) throws IOException {
        write(source, 0, source.length);
    }
}
