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

package org.apache.graphar.util;

public class CppClassName {
    public static final String GAR_NAMESPACE = "graphar";
    // arrow
    public static final String ARROW_RESULT = "arrow::Result";
    public static final String ARROW_STATUS = "arrow::Status";
    public static final String ARROW_TABLE = "arrow::Table";
    public static final String ARROW_ARRAY = "arrow::Array";
    public static final String ARROW_RECORD_BATCH = "arrow::RecordBatch";
    public static final String ARROW_SCHEMA = "arrow::Schema";

    // stdcxx
    public static final String STD_STRING = "std::string";
    public static final String STD_MAP = "std::map";
    public static final String STD_PAIR = "std::pair";
    public static final String STD_SHARED_PTR = "std::shared_ptr";

    // util
    public static final String GAR_INFO_VERSION = "graphar::InfoVersion";
    public static final String GAR_STATUS_CODE = "graphar::StatusCode";
    public static final String GAR_STATUS = "graphar::Status";
    public static final String GAR_RESULT = "graphar::Result";
    public static final String GAR_GRAPH_INFO = "graphar::GraphInfo";
    public static final String GAR_VERTEX_INFO = "graphar::VertexInfo";
    public static final String GAR_EDGE_INFO = "graphar::EdgeInfo";
    public static final String GAR_PROPERTY = "graphar::Property";
    public static final String GAR_PROPERTY_GROUP = "graphar::PropertyGroup";
    public static final String GAR_UTIL_INDEX_CONVERTER = "graphar::util::IndexConverter";
    public static final String GAR_UTIL_FILTER_OPTIONS = "graphar::util::FilterOptions";
    public static final String GAR_YAML = "graphar::Yaml";

    // types
    public static final String GAR_ID_TYPE = "graphar::IdType";
    public static final String GAR_TYPE = "graphar::Type";
    public static final String GAR_DATA_TYPE = "graphar::DataType";
    public static final String GAR_ADJ_LIST_TYPE = "graphar::AdjListType";
    public static final String GAR_FILE_TYPE = "graphar::FileType";
    public static final String GAR_VALIDATE_LEVEL = "graphar::ValidateLevel";

    // vertices
    public static final String GAR_VERTEX = "graphar::Vertex";
    public static final String GAR_VERTEX_ITER = "graphar::VertexIter";
    public static final String GAR_VERTICES_COLLECTION = "graphar::VerticesCollection";

    // edges
    public static final String GAR_EDGE = "graphar::Edge";
    public static final String GAR_EDGE_ITER = "graphar::EdgeIter";
    public static final String GAR_EDGES_COLLECTION = "graphar::EdgesCollection";
    public static final String GAR_EDGES_COLLECTION_ORDERED_BY_SOURCE =
            "graphar::EdgesCollection<graphar::AdjListType::ordered_by_source>";
    public static final String GAR_EDGES_COLLECTION_ORDERED_BY_DEST =
            "graphar::EdgesCollection<graphar::AdjListType::ordered_by_dest>";
    public static final String GAR_EDGES_COLLECTION_UNORDERED_BY_SOURCE =
            "graphar::EdgesCollection<graphar::AdjListType::unordered_by_source>";
    public static final String GAR_EDGES_COLLECTION_UNORDERED_BY_DEST =
            "graphar::EdgesCollection<graphar::AdjListType::unordered_by_dest>";

    public static final String GAR_ADJACENT_LIST = "graphar::AdjacentList";

    // readers.chunkinfo
    public static final String GAR_VERTEX_PROPERTY_CHUNK_INFO_READER =
            "graphar::VertexPropertyChunkInfoReader";
    public static final String GAR_ADJ_LIST_CHUNK_INFO_READER = "graphar::AdjListChunkInfoReader";
    public static final String GAR_ADJ_LIST_PROPERTY_CHUNK_INFO_READER =
            "graphar::AdjListPropertyChunkInfoReader";

    // readers.arrowchunk
    public static final String GAR_VERTEX_PROPERTY_ARROW_CHUNK_READER =
            "graphar::VertexPropertyArrowChunkReader";
    public static final String GAR_ADJ_LIST_ARROW_CHUNK_READER = "graphar::AdjListArrowChunkReader";
    public static final String GAR_ADJ_LIST_OFFSET_ARROW_CHUNK_READER =
            "graphar::AdjListOffsetArrowChunkReader";
    public static final String GAR_ADJ_LIST_PROPERTY_ARROW_CHUNK_READER =
            "graphar::AdjListPropertyArrowChunkReader";

    // writers
    public static final String GAR_BUILDER_VERTEX_PROPERTY_WRITER = "graphar::VertexPropertyWriter";
    public static final String GAR_EDGE_CHUNK_WRITER = "graphar::EdgeChunkWriter";
    // writers.builder
    public static final String GAR_BUILDER_VERTEX = "graphar::builder::Vertex";
    public static final String GAR_BUILDER_VERTICES_BUILDER = "graphar::builder::VerticesBuilder";
    public static final String GAR_BUILDER_EDGE = "graphar::builder::Edge";
    public static final String GAR_BUILDER_EDGES_BUILDER = "graphar::builder::EdgesBuilder";
}
