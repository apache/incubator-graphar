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

package org.apache.graphar.info.saver.impl;

import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.apache.graphar.info.saver.BaseGraphInfoSaver;

public class LocalFileSystemYamlGraphSaver extends BaseGraphInfoSaver {

    public Writer writeYaml(URI uri) throws IOException {
        Path outputPath = FileSystems.getDefault().getPath(uri.toString());
        Path parentDir = outputPath.getParent();
        if (parentDir != null) {
            Files.createDirectories(parentDir);
        }
        return Files.newBufferedWriter(
                outputPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);
    }
}
