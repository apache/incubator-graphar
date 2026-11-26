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
import java.util.ArrayList;
import java.util.List;
import org.apache.graphar.storage.InputFile;
import org.apache.graphar.storage.OutputFile;
import org.apache.graphar.storage.Storage;

/** Records the URI of every input file opened through the wrapped storage. */
final class CountingStorage implements Storage {
    final List<URI> inputs = new ArrayList<>();
    private final Storage delegate;

    CountingStorage(Storage delegate) {
        this.delegate = delegate;
    }

    @Override
    public InputFile inputFile(URI uri) {
        inputs.add(uri);
        return delegate.inputFile(uri);
    }

    @Override
    public OutputFile outputFile(URI uri) {
        return delegate.outputFile(uri);
    }

    @Override
    public boolean exists(URI uri) throws IOException {
        return delegate.exists(uri);
    }
}
