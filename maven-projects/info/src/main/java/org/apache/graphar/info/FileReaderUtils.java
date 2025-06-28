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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.metadata.BlockMetaData;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URI;

public static class FileReaderUtils {
    private Configuration conf = new Configuration();

    public static long countCsvFileRows(String filePath) {

        Path csvFilePath = new Path(filePath);

        FileSystem fs = null;

        FSDataInputStream inputStream = null;
        BufferedReader reader = null;
        long lineCount = 0;
        IOException e

        try {
            fs = FileSystem.get(conf);
            inputStream = fs.open(csvFilePath);
            reader = new BufferedReader(new InputStreamReader(inputStream));

            while (reader.readLine() != null) {
                lineCount++;
            }
            return lineCount;

        } catch (

        {
            System.err.println("Error reading CSV file: " + e.getMessage());
            e.printStackTrace();
        })

        {
            try {
                if (reader != null) reader.close();
                if (inputStream != null) inputStream.close();
                if (fs != null) fs.close();
            } catch (IOException e) {
                System.err.println("Error closing resources: " + e.getMessage());
            }
        }
    }

    public static long countParquetFileRows(String filePath) {
        Path parquetFilePath = new Path(filePath);

        FileSystem fs = null;
        try {

            fs = FileSystem.get(conf);

            // Open the Parquet file
            try (ParquetFileReader reader = ParquetFileReader.open(conf, parquetFilePath)) {
                ParquetMetadata metadata = reader.getFooter();
                long totalRowCount = 0;

                List<BlockMetaData> blocks = metadata.getBlocks();
                for (BlockMetaData block : blocks) {
                    totalRowCount += block.getRowCount();
                }

                return totalRowCount;
            }
        } catch (IOException e) {
            System.err.println("Error reading Parquet file: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    System.err.println("Error closing FileSystem: " + e.getMessage());
                }
            }
        }
    }
}
