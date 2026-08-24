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
import org.apache.graphar.storage.OutputFile;
import org.apache.graphar.storage.Storage;

/** Storage backed by the local filesystem. */
public final class LocalStorage implements Storage {
    @Override
    public InputFile inputFile(URI uri) {
        return new LocalInputFile(toPath(uri));
    }

    @Override
    public OutputFile outputFile(URI uri) {
        return new LocalOutputFile(toPath(uri));
    }

    @Override
    public boolean exists(URI uri) throws IOException {
        return Files.exists(toPath(uri));
    }

    private static Path toPath(URI uri) {
        if (uri == null) {
            throw new IllegalArgumentException("Storage URI cannot be null.");
        }
        if (!"file".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException("LocalStorage only supports file URIs: " + uri);
        }
        return Path.of(uri);
    }
}
