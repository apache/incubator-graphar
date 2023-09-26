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

import static com.alibaba.graphar.util.CppClassName.GAR_ADJ_LIST_ARROW_CHUNK_READER;
import static com.alibaba.graphar.util.CppClassName.GAR_ADJ_LIST_CHUNK_INFO_READER;
import static com.alibaba.graphar.util.CppClassName.GAR_ADJ_LIST_OFFSET_ARROW_CHUNK_READER;
import static com.alibaba.graphar.util.CppClassName.GAR_ADJ_LIST_PROPERTY_ARROW_CHUNK_READER;
import static com.alibaba.graphar.util.CppClassName.GAR_ADJ_LIST_PROPERTY_CHUNK_INFO_READER;
import static com.alibaba.graphar.util.CppClassName.GAR_DATA_TYPE;
import static com.alibaba.graphar.util.CppClassName.GAR_EDGE_INFO;
import static com.alibaba.graphar.util.CppClassName.GAR_FILE_TYPE;
import static com.alibaba.graphar.util.CppClassName.GAR_GRAPH_INFO;
import static com.alibaba.graphar.util.CppClassName.GAR_ID_TYPE;
import static com.alibaba.graphar.util.CppClassName.GAR_INFO_VERSION;
import static com.alibaba.graphar.util.CppClassName.GAR_PROPERTY_GROUP;
import static com.alibaba.graphar.util.CppClassName.GAR_RESULT;
import static com.alibaba.graphar.util.CppClassName.GAR_VERTEX_INFO;
import static com.alibaba.graphar.util.CppClassName.GAR_VERTEX_PROPERTY_ARROW_CHUNK_READER;
import static com.alibaba.graphar.util.CppClassName.GAR_VERTEX_PROPERTY_CHUNK_INFO_READER;
import static com.alibaba.graphar.util.CppClassName.STD_STRING;
import static com.alibaba.graphar.util.CppHeaderName.GAR_ARROW_CHUNK_READER_H;
import static com.alibaba.graphar.util.CppHeaderName.GAR_CHUNK_INFO_READER_H;
import static com.alibaba.graphar.util.CppHeaderName.GAR_GRAPH_INFO_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXTemplate;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;

@FFIGen
@FFITypeAlias(GAR_RESULT)
@CXXHead(GAR_GRAPH_INFO_H)
@CXXHead(GAR_CHUNK_INFO_READER_H)
@CXXHead(GAR_ARROW_CHUNK_READER_H)
@CXXTemplate(cxx = "bool", java = "java.lang.Boolean")
@CXXTemplate(cxx = "long", java = "java.lang.Long")
@CXXTemplate(cxx = "int64_t", java = "java.lang.Long")
@CXXTemplate(cxx = GAR_ID_TYPE, java = "java.lang.Long")
@CXXTemplate(cxx = STD_STRING, java = "com.alibaba.graphar.stdcxx.StdString")
@CXXTemplate(cxx = GAR_GRAPH_INFO, java = "com.alibaba.graphar.graphinfo.GraphInfo")
@CXXTemplate(cxx = GAR_VERTEX_INFO, java = "com.alibaba.graphar.graphinfo.VertexInfo")
@CXXTemplate(cxx = GAR_EDGE_INFO, java = "com.alibaba.graphar.graphinfo.EdgeInfo")
@CXXTemplate(cxx = GAR_PROPERTY_GROUP, java = "com.alibaba.graphar.graphinfo.PropertyGroup")
@CXXTemplate(cxx = GAR_DATA_TYPE, java = "com.alibaba.graphar.types.DataType")
@CXXTemplate(cxx = GAR_FILE_TYPE, java = "com.alibaba.graphar.types.FileType")
@CXXTemplate(cxx = GAR_INFO_VERSION, java = "com.alibaba.graphar.util.InfoVersion")
@CXXTemplate(
        cxx = "std::shared_ptr<GraphArchive::Yaml>",
        java = "com.alibaba.graphar.stdcxx.StdSharedPtr<com.alibaba.graphar.util.Yaml>")
@CXXTemplate(
        cxx = "std::pair<GraphArchive::IdType,GraphArchive::IdType>",
        java = "com.alibaba.graphar.stdcxx.StdPair<Long,Long>")
@CXXTemplate(
        cxx = GAR_ADJ_LIST_ARROW_CHUNK_READER,
        java = "com.alibaba.graphar.readers.arrowchunk.AdjListArrowChunkReader")
@CXXTemplate(
        cxx = GAR_ADJ_LIST_OFFSET_ARROW_CHUNK_READER,
        java = "com.alibaba.graphar.readers.arrowchunk.AdjListOffsetArrowChunkReader")
@CXXTemplate(
        cxx = GAR_ADJ_LIST_PROPERTY_ARROW_CHUNK_READER,
        java = "com.alibaba.graphar.readers.arrowchunk.AdjListPropertyArrowChunkReader")
@CXXTemplate(
        cxx = GAR_VERTEX_PROPERTY_ARROW_CHUNK_READER,
        java = "com.alibaba.graphar.readers.arrowchunk.VertexPropertyArrowChunkReader")
@CXXTemplate(
        cxx = GAR_ADJ_LIST_CHUNK_INFO_READER,
        java = "com.alibaba.graphar.readers.chunkinfo.AdjListChunkInfoReader")
@CXXTemplate(
        cxx = GAR_ADJ_LIST_PROPERTY_CHUNK_INFO_READER,
        java = "com.alibaba.graphar.readers.chunkinfo.AdjListPropertyChunkInfoReader")
@CXXTemplate(
        cxx = GAR_VERTEX_PROPERTY_CHUNK_INFO_READER,
        java = "com.alibaba.graphar.readers.chunkinfo.VertexPropertyChunkInfoReader")
public interface Result<T> extends CXXPointer {

    @CXXReference
    T value();

    @CXXValue
    Status status();

    @FFINameAlias("has_error")
    boolean hasError();
}
