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

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import org.apache.graphar.storage.InputFile;
import org.apache.graphar.storage.SeekableInput;
import org.apache.graphar.storage.Storage;

/** Reads the fixed-width little-endian INT64 control files prescribed by GraphAr. */
final class ControlFileReader {
    private static final int INT64_BYTES = Long.BYTES;

    private ControlFileReader() {}

    static long readNonNegativeLong(Storage storage, URI uri) throws IOException {
        Objects.requireNonNull(storage, "Storage cannot be null.");
        Objects.requireNonNull(uri, "Control file URI cannot be null.");
        InputFile file = storage.inputFile(uri);
        if (file.size() != INT64_BYTES) {
            throw new IllegalArgumentException(
                    "GraphAr control file must contain exactly one INT64: " + uri);
        }
        ByteBuffer bytes = ByteBuffer.allocate(INT64_BYTES).order(ByteOrder.LITTLE_ENDIAN);
        try (SeekableInput input = file.open()) {
            input.readFully(bytes);
        }
        long value = bytes.flip().getLong();
        if (value < 0) {
            throw new IllegalArgumentException(
                    "GraphAr control file value must be non-negative: " + uri);
        }
        return value;
    }
}
