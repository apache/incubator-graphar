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

package org.apache.graphar.stdcxx;

import static org.apache.graphar.util.CppClassName.STD_PAIR;
import static org.apache.graphar.util.CppHeaderName.GAR_GRAPH_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXTemplate;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIGetter;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;

@FFIGen
@FFITypeAlias(STD_PAIR)
@CXXHead(system = "utility")
@CXXHead(GAR_GRAPH_H)
@CXXTemplate(
        cxx = {"GraphArchive::IdType", "GraphArchive::IdType"},
        java = {"Long", "Long"})
@CXXTemplate(
        cxx = {"int64_t", "int64_t"},
        java = {"Long", "Long"})
public interface StdPair<T1, T2> extends FFIPointer {
    @FFINameAlias("first")
    @FFIGetter
    T1 getFirst();

    @FFINameAlias("second")
    @FFIGetter
    T2 getSecond();
}
