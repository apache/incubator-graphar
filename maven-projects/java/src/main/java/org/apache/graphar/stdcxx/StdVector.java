/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache GraphAr (incubating) project, which was derived from GraphScope.
 */

/*
 * Copyright 2021 Alibaba Group Holding Limited.
 */

// Derived from alibaba/GraphScope v0.25.0
// https://github.com/alibaba/GraphScope/blob/8235b29/analytical_engine/java/grape-jdk/src/main/java/com/alibaba/graphscope/stdcxx/StdMap.java

package org.apache.graphar.stdcxx;

import static org.apache.graphar.util.CppClassName.GAR_PROPERTY;
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
@CXXTemplate(cxx = GAR_PROPERTY, java = "org.apache.graphar.graphinfo.Property")
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

    default void add(@CXXReference E value) {
        long size = size();
        long cap = capacity();
        if (size == cap) {
            reserve(cap << 1);
        }
        push_back(value);
    }

    default @CXXReference E append() {
        long size = size();
        long cap = capacity();
        if (size == cap) {
            reserve(cap << 1);
        }
        resize(size + 1);
        return get(size);
    }

    void clear();

    long data();

    long capacity();

    void reserve(long size);

    void resize(long size);

    @FFIFactory
    interface Factory<E> {

        StdVector<E> create();
    }
}
