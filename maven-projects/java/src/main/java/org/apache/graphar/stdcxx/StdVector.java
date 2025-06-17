/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Derived from alibaba/GraphScope v0.25.0
// https://github.com/alibaba/GraphScope/blob/8235b29/analytical_engine/java/grape-jdk/src/main/java/com/alibaba/graphscope/stdcxx/StdMap.java

package org.apache.graphar.stdcxx;

import static org.apache.graphar.util.CppHeaderName.ARROW_API_H;
import static org.apache.graphar.util.CppHeaderName.GAR_GRAPH_INFO_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXTemplate;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFISettablePointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;

@FFIGen
@CXXHead(system = {"vector", "string"})
@CXXHead(GAR_GRAPH_INFO_H)
@CXXHead(ARROW_API_H)
@FFITypeAlias("std::vector")
@CXXTemplate(cxx = "char", java = "java.lang.Byte")
@CXXTemplate(cxx = "int32_t", java = "java.lang.Integer")
public interface StdVector<E> extends CXXPointer, FFISettablePointer {

    static Factory getStdVectorFactory(String foreignName) {
        return FFITypeFactory.getFactory(StdVector.class, foreignName);
    }

    long size();

    @CXXOperator("[]")
    @CXXReference
    E get(long index);

    @CXXOperator("[]")
    void set(long index, @CXXReference E value);

    @CXXOperator("==")
    boolean eq(@CXXReference StdVector<E> other);

    void push_back(@CXXValue E e);

    void clear();

    long data();

    long capacity();

    @FFIFactory
    interface Factory<E> {

        StdVector<E> create();
    }
}
