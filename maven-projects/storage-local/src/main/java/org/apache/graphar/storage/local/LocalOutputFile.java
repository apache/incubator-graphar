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
import java.nio.file.StandardOpenOption;
import org.apache.graphar.storage.OutputFile;
import org.apache.graphar.storage.PositionOutput;

final class LocalOutputFile implements OutputFile {
    private final Path path;

    LocalOutputFile(Path path) {
        this.path = path;
    }

    @Override
    public URI uri() {
        return path.toUri();
    }

    @Override
    public PositionOutput create() throws IOException {
        createParentDirectories();
        return new LocalPositionOutput(
                Files.newOutputStream(
                        path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE));
    }

    @Override
    public PositionOutput createOrOverwrite() throws IOException {
        createParentDirectories();
        return new LocalPositionOutput(
                Files.newOutputStream(
                        path,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE));
    }

    private void createParentDirectories() throws IOException {
        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
    }
}
