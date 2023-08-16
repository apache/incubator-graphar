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
    // stdcxx
    public static final String STD_STRING = "std::string";
    public static final String STD_MAP = "std::map";

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

    // types
    public static final String GAR_ID_TYPE = "GraphArchive::IdType";
    public static final String GAR_TYPE = "GraphArchive::Type";
    public static final String GAR_DATA_TYPE = "GraphArchive::DataType";
    public static final String GAR_ADJ_LIST_TYPE = "GraphArchive::AdjListType";
    public static final String GAR_FILE_TYPE = "GraphArchive::FileType";
    public static final String GAR_VALIDATE_LEVEL = "GraphArchive::ValidateLevel";
}
