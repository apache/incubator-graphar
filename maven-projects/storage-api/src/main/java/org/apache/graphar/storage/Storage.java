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

import java.io.IOException;
import java.net.URI;

/** Resolves GraphAr URIs to storage-specific readable and writable objects. */
public interface Storage {
    /** Resolves a readable file without opening it. */
    InputFile inputFile(URI uri);

    /** Resolves a writable file without opening it. */
    OutputFile outputFile(URI uri);

    /** Returns whether a file exists at {@code uri}. */
    boolean exists(URI uri) throws IOException;
}
