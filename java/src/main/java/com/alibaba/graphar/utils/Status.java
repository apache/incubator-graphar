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

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFITypeAlias;

import static com.alibaba.graphar.utils.CppClassName.GAR_STATUS;
import static com.alibaba.graphar.utils.CppHeaderName.GAR_GRAPH_INFO_H;

@FFIGen
@FFITypeAlias(GAR_STATUS)
@CXXHead(GAR_GRAPH_INFO_H)
public interface Status extends CXXPointer {
  boolean ok();
}
