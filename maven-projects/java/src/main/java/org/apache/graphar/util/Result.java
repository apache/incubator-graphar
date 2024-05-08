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

import static org.apache.graphar.util.CppClassName.GAR_ADJ_LIST_ARROW_CHUNK_READER;
import static org.apache.graphar.util.CppClassName.GAR_ADJ_LIST_CHUNK_INFO_READER;
import static org.apache.graphar.util.CppClassName.GAR_ADJ_LIST_OFFSET_ARROW_CHUNK_READER;
import static org.apache.graphar.util.CppClassName.GAR_ADJ_LIST_PROPERTY_ARROW_CHUNK_READER;
import static org.apache.graphar.util.CppClassName.GAR_ADJ_LIST_PROPERTY_CHUNK_INFO_READER;
import static org.apache.graphar.util.CppClassName.GAR_DATA_TYPE;
import static org.apache.graphar.util.CppClassName.GAR_EDGES_COLLECTION;
import static org.apache.graphar.util.CppClassName.GAR_EDGE_INFO;
import static org.apache.graphar.util.CppClassName.GAR_FILE_TYPE;
import static org.apache.graphar.util.CppClassName.GAR_GRAPH_INFO;
import static org.apache.graphar.util.CppClassName.GAR_ID_TYPE;
import static org.apache.graphar.util.CppClassName.GAR_INFO_VERSION;
import static org.apache.graphar.util.CppClassName.GAR_PROPERTY_GROUP;
import static org.apache.graphar.util.CppClassName.GAR_RESULT;
import static org.apache.graphar.util.CppClassName.GAR_VERTEX_INFO;
import static org.apache.graphar.util.CppClassName.GAR_VERTEX_PROPERTY_ARROW_CHUNK_READER;
import static org.apache.graphar.util.CppClassName.GAR_VERTEX_PROPERTY_CHUNK_INFO_READER;
import static org.apache.graphar.util.CppClassName.GAR_VERTICES_COLLECTION;
import static org.apache.graphar.util.CppClassName.STD_PAIR;
import static org.apache.graphar.util.CppClassName.STD_SHARED_PTR;
import static org.apache.graphar.util.CppClassName.STD_STRING;
import static org.apache.graphar.util.CppHeaderName.GAR_ARROW_CHUNK_READER_H;
import static org.apache.graphar.util.CppHeaderName.GAR_CHUNK_INFO_READER_H;
import static org.apache.graphar.util.CppHeaderName.GAR_GRAPH_H;
import static org.apache.graphar.util.CppHeaderName.GAR_GRAPH_INFO_H;

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
@CXXHead(GAR_GRAPH_H)
@CXXHead(GAR_CHUNK_INFO_READER_H)
@CXXHead(GAR_ARROW_CHUNK_READER_H)
@CXXTemplate(cxx = "bool", java = "java.lang.Boolean")
@CXXTemplate(cxx = "long", java = "java.lang.Long")
@CXXTemplate(cxx = "int64_t", java = "java.lang.Long")
@CXXTemplate(cxx = GAR_ID_TYPE, java = "java.lang.Long")
@CXXTemplate(cxx = STD_STRING, java = "org.apache.graphar.stdcxx.StdString")
@CXXTemplate(cxx = GAR_GRAPH_INFO, java = "org.apache.graphar.graphinfo.GraphInfo")
@CXXTemplate(cxx = GAR_VERTEX_INFO, java = "org.apache.graphar.graphinfo.VertexInfo")
@CXXTemplate(cxx = GAR_EDGE_INFO, java = "org.apache.graphar.graphinfo.EdgeInfo")
@CXXTemplate(cxx = GAR_PROPERTY_GROUP, java = "org.apache.graphar.graphinfo.PropertyGroup")
@CXXTemplate(cxx = GAR_DATA_TYPE, java = "org.apache.graphar.types.DataType")
@CXXTemplate(cxx = GAR_FILE_TYPE, java = "org.apache.graphar.types.FileType")
@CXXTemplate(cxx = GAR_INFO_VERSION, java = "org.apache.graphar.util.InfoVersion")
@CXXTemplate(
        cxx = STD_SHARED_PTR + "<GraphArchive::Yaml>",
        java = "org.apache.graphar.stdcxx.StdSharedPtr<org.apache.graphar.util.Yaml>")
@CXXTemplate(
        cxx = STD_PAIR + "<GraphArchive::IdType,GraphArchive::IdType>",
        java = "org.apache.graphar.stdcxx.StdPair<Long,Long>")
@CXXTemplate(
        cxx = GAR_ADJ_LIST_ARROW_CHUNK_READER,
        java = "org.apache.graphar.readers.arrowchunk.AdjListArrowChunkReader")
@CXXTemplate(
        cxx = GAR_ADJ_LIST_OFFSET_ARROW_CHUNK_READER,
        java = "org.apache.graphar.readers.arrowchunk.AdjListOffsetArrowChunkReader")
@CXXTemplate(
        cxx = GAR_ADJ_LIST_PROPERTY_ARROW_CHUNK_READER,
        java = "org.apache.graphar.readers.arrowchunk.AdjListPropertyArrowChunkReader")
@CXXTemplate(
        cxx = GAR_VERTEX_PROPERTY_ARROW_CHUNK_READER,
        java = "org.apache.graphar.readers.arrowchunk.VertexPropertyArrowChunkReader")
@CXXTemplate(
        cxx = GAR_ADJ_LIST_CHUNK_INFO_READER,
        java = "org.apache.graphar.readers.chunkinfo.AdjListChunkInfoReader")
@CXXTemplate(
        cxx = GAR_ADJ_LIST_PROPERTY_CHUNK_INFO_READER,
        java = "org.apache.graphar.readers.chunkinfo.AdjListPropertyChunkInfoReader")
@CXXTemplate(
        cxx = GAR_VERTEX_PROPERTY_CHUNK_INFO_READER,
        java = "org.apache.graphar.readers.chunkinfo.VertexPropertyChunkInfoReader")
@CXXTemplate(
        cxx = STD_SHARED_PTR + "<" + GAR_VERTICES_COLLECTION + ">",
        java =
                "org.apache.graphar.stdcxx.StdSharedPtr<org.apache.graphar.vertices.VerticesCollection>")
@CXXTemplate(
        cxx = STD_SHARED_PTR + "<" + GAR_EDGES_COLLECTION + ">",
        java = "org.apache.graphar.stdcxx.StdSharedPtr<org.apache.graphar.edges.EdgesCollection>")
public interface Result<T> extends CXXPointer {

    @CXXReference
    T value();

    @CXXValue
    Status status();

    @FFINameAlias("has_error")
    boolean hasError();
}
