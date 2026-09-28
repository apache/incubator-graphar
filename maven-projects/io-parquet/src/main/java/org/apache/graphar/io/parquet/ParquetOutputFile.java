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
import java.util.Objects;
import org.apache.graphar.storage.OutputFile;
import org.apache.graphar.storage.PositionOutput;
import org.apache.parquet.io.PositionOutputStream;

/** Adapts a GraphAr storage output file to Parquet's output-file interface. */
final class ParquetOutputFile implements org.apache.parquet.io.OutputFile {
    private final OutputFile outputFile;

    ParquetOutputFile(OutputFile outputFile) {
        this.outputFile = Objects.requireNonNull(outputFile, "outputFile");
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
        return new ParquetPositionOutputStream(outputFile.create());
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
        return new ParquetPositionOutputStream(outputFile.createOrOverwrite());
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return 0;
    }

    @Override
    public String getPath() {
        return outputFile.uri().toString();
    }

    private static final class ParquetPositionOutputStream extends PositionOutputStream {
        private final PositionOutput output;
        private final byte[] single = new byte[1];

        private ParquetPositionOutputStream(PositionOutput output) {
            this.output = output;
        }

        @Override
        public long getPos() throws IOException {
            return output.position();
        }

        @Override
        public void write(int value) throws IOException {
            single[0] = (byte) value;
            output.write(single);
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            output.write(bytes, offset, length);
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
}
