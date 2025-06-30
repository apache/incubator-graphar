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

package org.apache.graphar.info;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.graphar.types.FileType;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URI;

public static class FileReader {

    //TODO API to read data files based on type (CSV, Parquet, ..)
    public static long getFileCount(VertexInfo vertexInfo, PropertyGroup propertyGroup) {
        // TODO check equality test for type
        FileType type = propertyGroup.getFileType();
        numberOfParts = vertexInfo.getChunkSize()
        chunkBasePath = vertexInfo.getPropertyGroupPrefix() + "/part";
        totalRowCount = 0;

        for (int i : numberOfParts) {
            chunkPath = chunkBasePath + Integer.toString(i);
            switch (type):
            case CSV {
                long currentChunkEntryCount = FileReaderUtils.countCsvFileRows(chunkPath);
                break;
            }
            case PARQUET {
                long currentChunkEntryCount = FileReaderUtils.countParquetFileRows(chunkPath);
                break;
            }
            // TODO ORC...
            totalRowCount = totalRowCount + currentChunkEntryCount;
        }
        return totalRowCount;

    }

}