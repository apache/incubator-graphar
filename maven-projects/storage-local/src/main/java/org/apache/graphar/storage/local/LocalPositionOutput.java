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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.graphar.storage.PositionOutput;

final class LocalPositionOutput implements PositionOutput {
    private static final int BUFFER_SIZE = 8192;

    private final OutputStream output;
    private long position;
    private byte[] transfer;

    LocalPositionOutput(OutputStream output) {
        this.output = output;
    }

    @Override
    public long position() {
        return position;
    }

    @Override
    public void write(ByteBuffer source) throws IOException {
        if (source.hasArray()) {
            int length = source.remaining();
            write(source.array(), source.arrayOffset() + source.position(), length);
            source.position(source.position() + length);
            return;
        }

        if (transfer == null) {
            transfer = new byte[BUFFER_SIZE];
        }
        while (source.hasRemaining()) {
            int length = Math.min(source.remaining(), transfer.length);
            source.get(transfer, 0, length);
            write(transfer, 0, length);
        }
    }

    @Override
    public void write(byte[] source, int offset, int length) throws IOException {
        output.write(source, offset, length);
        position += length;
    }

    @Override
    public void flush() throws IOException {
        output.flush();
    }

    @Override
    public void close() throws IOException {
        output.close();
    }
}
