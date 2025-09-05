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

package org.apache.graphar.info.loader.impl;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import org.apache.graphar.info.loader.StreamGraphInfoLoader;

public class LocalFileSystemStreamGraphInfoLoader extends StreamGraphInfoLoader {
    @Override
    public InputStream readYaml(URI uri) throws IOException {
        if (uri.getScheme() != null && !"file".equals(uri.getScheme())) {
            throw new RuntimeException("Only file:// scheme is supported in Local File System");
        }
        String path = uri.getPath();
        return new FileInputStream(path);
    }
}
