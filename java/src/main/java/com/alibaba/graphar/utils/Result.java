/* Copyright 2022 Alibaba Group Holding Limited.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package com.alibaba.graphar.utils;

import static com.alibaba.graphar.utils.CppClassName.GAR_DATA_TYPE;
import static com.alibaba.graphar.utils.CppClassName.GAR_EDGE_INFO;
import static com.alibaba.graphar.utils.CppClassName.GAR_FILE_TYPE;
import static com.alibaba.graphar.utils.CppClassName.GAR_GRAPH_INFO;
import static com.alibaba.graphar.utils.CppClassName.GAR_INFO_VERSION;
import static com.alibaba.graphar.utils.CppClassName.GAR_PROPERTY_GROUP;
import static com.alibaba.graphar.utils.CppClassName.GAR_RESULT;
import static com.alibaba.graphar.utils.CppClassName.GAR_VERTEX_INFO;
import static com.alibaba.graphar.utils.CppClassName.STD_STRING;
import static com.alibaba.graphar.utils.CppHeaderName.GAR_GRAPH_INFO_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXTemplate;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFITypeAlias;

@FFIGen
@FFITypeAlias(GAR_RESULT)
@CXXHead(GAR_GRAPH_INFO_H)
@CXXTemplate(cxx = "bool", java = "Boolean")
@CXXTemplate(cxx = STD_STRING, java = "com.alibaba.graphar.stdcxx.StdString")
@CXXTemplate(cxx = GAR_GRAPH_INFO, java = "com.alibaba.graphar.graphinfo.GraphInfo")
@CXXTemplate(cxx = GAR_VERTEX_INFO, java = "com.alibaba.graphar.graphinfo.VertexInfo")
@CXXTemplate(cxx = GAR_EDGE_INFO, java = "com.alibaba.graphar.graphinfo.EdgeInfo")
@CXXTemplate(cxx = GAR_PROPERTY_GROUP, java = "com.alibaba.graphar.graphinfo.PropertyGroup")
@CXXTemplate(cxx = GAR_DATA_TYPE, java = "com.alibaba.graphar.types.DataType")
@CXXTemplate(cxx = GAR_FILE_TYPE, java = "com.alibaba.graphar.types.FileType")
@CXXTemplate(cxx = GAR_INFO_VERSION, java = "com.alibaba.graphar.utils.InfoVersion")
public interface Result<T> extends CXXPointer {
  @CXXReference
  T value();

  @CXXValue
  Status status();
}
