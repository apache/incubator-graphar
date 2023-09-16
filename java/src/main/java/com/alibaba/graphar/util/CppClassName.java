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

public class CppClassName {
  public static final String GAR_NAMESPACE = "GraphArchive";
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

  // util
  public static final String GAR_INFO_VERSION = "GraphArchive::InfoVersion";
  public static final String GAR_STATUS_CODE = "GraphArchive::StatusCode";
  public static final String GAR_STATUS = "GraphArchive::Status";
  public static final String GAR_RESULT = "GraphArchive::Result";
  public static final String GAR_GRAPH_INFO = "GraphArchive::GraphInfo";
  public static final String GAR_VERTEX_INFO = "GraphArchive::VertexInfo";
  public static final String GAR_EDGE_INFO = "GraphArchive::EdgeInfo";
  public static final String GAR_PROPERTY = "GraphArchive::Property";
  public static final String GAR_PROPERTY_GROUP = "GraphArchive::PropertyGroup";
  public static final String GAR_UTIL_INDEX_CONVERTER = "GraphArchive::util::IndexConverter";
  public static final String GAR_UTIL_FILTER_OPTIONS = "GraphArchive::util::FilterOptions";
  public static final String GAR_YAML = "GraphArchive::Yaml";

  // types
  public static final String GAR_ID_TYPE = "GraphArchive::IdType";
  public static final String GAR_TYPE = "GraphArchive::Type";
  public static final String GAR_DATA_TYPE = "GraphArchive::DataType";
  public static final String GAR_ADJ_LIST_TYPE = "GraphArchive::AdjListType";
  public static final String GAR_FILE_TYPE = "GraphArchive::FileType";
  public static final String GAR_VALIDATE_LEVEL = "GraphArchive::ValidateLevel";

  // vertices
  public static final String GAR_VERTEX = "GraphArchive::Vertex";
  public static final String GAR_VERTEX_ITER = "GraphArchive::VertexIter";
  public static final String GAR_VERTICES_COLLECTION = "GraphArchive::VerticesCollection";

  // edges
  public static final String GAR_EDGE = "GraphArchive::Edge";
  public static final String GAR_EDGE_ITER = "GraphArchive::EdgeIter";
  public static final String GAR_EDGES_COLLECTION = "GraphArchive::EdgesCollection";
  public static final String GAR_EDGES_COLLECTION_ORDERED_BY_SOURCE = "GraphArchive::EdgesCollection<GraphArchive::AdjListType::ordered_by_source>";
  public static final String GAR_EDGES_COLLECTION_ORDERED_BY_DEST = "GraphArchive::EdgesCollection<GraphArchive::AdjListType::ordered_by_dest>";
  public static final String GAR_EDGES_COLLECTION_UNORDERED_BY_SOURCE = "GraphArchive::EdgesCollection<GraphArchive::AdjListType::unordered_by_source>";
  public static final String GAR_EDGES_COLLECTION_UNORDERED_BY_DEST = "GraphArchive::EdgesCollection<GraphArchive::AdjListType::unordered_by_dest>";

  // readers.chunkinfo
  public static final String GAR_VERTEX_PROPERTY_CHUNK_INFO_READER = "GraphArchive::VertexPropertyChunkInfoReader";
  public static final String GAR_ADJ_LIST_CHUNK_INFO_READER = "GraphArchive::AdjListChunkInfoReader";
  public static final String GAR_ADJ_LIST_PROPERTY_CHUNK_INFO_READER = "GraphArchive::AdjListPropertyChunkInfoReader";

  // readers.arrowchunk
  public static final String GAR_VERTEX_PROPERTY_ARROW_CHUNK_READER = "GraphArchive::VertexPropertyArrowChunkReader";
  public static final String GAR_ADJ_LIST_ARROW_CHUNK_READER = "GraphArchive::AdjListArrowChunkReader";
  public static final String GAR_ADJ_LIST_OFFSET_ARROW_CHUNK_READER = "GraphArchive::AdjListOffsetArrowChunkReader";
  public static final String GAR_ADJ_LIST_PROPERTY_ARROW_CHUNK_READER = "GraphArchive::AdjListPropertyArrowChunkReader";

  // writers
  public static final String GAR_BUILDER_VERTEX_PROPERTY_WRITER = "GraphArchive::VertexPropertyWriter";
  public static final String GAR_EDGE_CHUNK_WRITER = "GraphArchive::EdgeChunkWriter";
  // writers.builder
  public static final String GAR_BUILDER_VERTEX = "GraphArchive::builder::Vertex";
  public static final String GAR_BUILDER_VERTICES_BUILDER = "GraphArchive::builder::VerticesBuilder";
  public static final String GAR_BUILDER_EDGE = "GraphArchive::builder::Edge";
  public static final String GAR_BUILDER_EDGES_BUILDER = "GraphArchive::builder::EdgesBuilder";
}
