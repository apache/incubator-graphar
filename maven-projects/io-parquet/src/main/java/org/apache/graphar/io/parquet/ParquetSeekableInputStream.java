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
import java.util.Objects;
import org.apache.graphar.storage.SeekableInput;

/** Adapts a GraphAr seekable input to Parquet's seekable input stream. */
final class ParquetSeekableInputStream extends org.apache.parquet.io.SeekableInputStream {
    private final SeekableInput input;

    ParquetSeekableInputStream(SeekableInput input) {
        this.input = Objects.requireNonNull(input, "input");
    }

    @Override
    public long getPos() throws IOException {
        return input.position();
    }

    @Override
    public void seek(long newPos) throws IOException {
        input.seek(newPos);
    }

    @Override
    public int read() throws IOException {
        byte[] singleByte = new byte[1];
        return read(singleByte, 0, 1) < 0 ? -1 : Byte.toUnsignedInt(singleByte[0]);
    }

    @Override
    public int read(byte[] bytes, int offset, int length) throws IOException {
        Objects.requireNonNull(bytes, "bytes");
        Objects.checkFromIndexSize(offset, length, bytes.length);
        if (length == 0) {
            return 0;
        }
        return input.read(ByteBuffer.wrap(bytes, offset, length));
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
        Objects.requireNonNull(bytes, "bytes");
        input.readFully(ByteBuffer.wrap(bytes));
    }

    @Override
    public void readFully(byte[] bytes, int offset, int length) throws IOException {
        Objects.requireNonNull(bytes, "bytes");
        Objects.checkFromIndexSize(offset, length, bytes.length);
        input.readFully(ByteBuffer.wrap(bytes, offset, length));
    }

    @Override
    public int read(ByteBuffer buffer) throws IOException {
        return input.read(buffer);
    }

    @Override
    public void readFully(ByteBuffer buffer) throws IOException {
        input.readFully(buffer);
    }

    @Override
    public void close() throws IOException {
        input.close();
    }
}
