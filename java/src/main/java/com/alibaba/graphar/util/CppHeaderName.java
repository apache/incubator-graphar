/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphar.util;

public class CppHeaderName {
    public static final String ARROW_API_H = "arrow/api.h";
    public static final String ARROW_C_BRIDGE_H = "arrow/c/bridge.h";
    public static final String ARROW_RECORD_BATCH_H = "arrow/record_batch.h";
    public static final String GAR_GRAPH_INFO_H = "gar/graph_info.h";
    public static final String GAR_GRAPH_H = "gar/graph.h";
    // reader
    public static final String GAR_CHUNK_INFO_READER_H = "gar/reader/chunk_info_reader.h";
    public static final String GAR_ARROW_CHUNK_READER_H = "gar/reader/arrow_chunk_reader.h";
    // writer
    public static final String GAR_VERTICES_BUILDER_H = "gar/writer/vertices_builder.h";
    public static final String GAR_EDGES_BUILDER_H = "gar/writer/edges_builder.h";
    public static final String GAR_ARROW_CHUNK_WRITER_H = "gar/writer/arrow_chunk_writer.h";
    // util
    public static final String GAR_UTIL_UTIL_H = "gar/util/util.h";
    public static final String GAR_UTIL_YAML_H = "gar/util/yaml.h";
    public static final String GAR_UTIL_READER_UTIL_H = "gar/util/reader_util.h";
}
