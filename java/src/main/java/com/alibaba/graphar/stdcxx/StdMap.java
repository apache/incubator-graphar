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

package com.alibaba.graphar.stdcxx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXTemplate;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;

import static com.alibaba.graphar.util.CppClassName.GAR_EDGE_INFO;
import static com.alibaba.graphar.util.CppClassName.GAR_VERTEX_INFO;
import static com.alibaba.graphar.util.CppClassName.STD_STRING;
import static com.alibaba.graphar.util.CppHeaderName.GAR_GRAPH_INFO_H;

@FFIGen
@CXXHead(system = {"map"})
@CXXHead(GAR_GRAPH_INFO_H)
@FFITypeAlias("std::map")
@CXXTemplate(
    cxx = {STD_STRING, GAR_EDGE_INFO},
    java = {"com.alibaba.graphar.stdcxx.StdString", "com.alibaba.graphar.graphinfo.EdgeInfo"})
@CXXTemplate(
    cxx = {STD_STRING, GAR_VERTEX_INFO},
    java = {"com.alibaba.graphar.stdcxx.StdString", "com.alibaba.graphar.graphinfo.VertexInfo"})
public interface StdMap<K, V> extends FFIPointer {

  @CXXOperator("[]")
  @CXXReference
  V get(@CXXReference K key);

  int size();

  @FFIFactory
  interface Factory<K, V> {
    StdMap<K, V> create();
  }
}
