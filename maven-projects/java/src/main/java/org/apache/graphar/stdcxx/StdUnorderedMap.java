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
// https://github.com/alibaba/GraphScope/blob/8235b29/analytical_engine/java/grape-jdk/src/main/java/com/alibaba/graphscope/stdcxx/StdUnorderedMap.java

package org.apache.graphar.stdcxx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXTemplate;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFITypeAlias;

@FFIGen
@CXXHead(
        value = {"stdint.h"},
        system = {"unordered_map"})
@FFITypeAlias("std::unordered_map")
@CXXTemplate(
        cxx = {"unsigned", "uint64_t"},
        java = {"java.lang.Integer", "java.lang.Long"})
public interface StdUnorderedMap<KEY_T, VALUE_T> extends CXXPointer {

    int size();

    boolean empty();

    @CXXReference
    @CXXOperator("[]")
    VALUE_T get(@CXXReference KEY_T key);

    @CXXOperator("[]")
    void set(@CXXReference KEY_T key, @CXXReference VALUE_T value);

    @FFIFactory
    interface Factory<KEY_T, VALUE_T> {

        StdUnorderedMap<KEY_T, VALUE_T> create();
    }
}
