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
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.graphar.storage.InputFile;
import org.apache.graphar.storage.SeekableInput;

final class LocalInputFile implements InputFile {
    private final Path path;

    LocalInputFile(Path path) {
        this.path = path;
    }

    @Override
    public URI uri() {
        return path.toUri();
    }

    @Override
    public long size() throws IOException {
        return Files.size(path);
    }

    @Override
    public SeekableInput open() throws IOException {
        return new LocalSeekableInput(path);
    }
}
