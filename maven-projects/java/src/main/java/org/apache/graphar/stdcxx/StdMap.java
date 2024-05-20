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
// https://github.com/alibaba/GraphScope/blob/8235b29/analytical_engine/java/grape-jdk/src/main/java/com/alibaba/graphscope/stdcxx/StdVector.java

package org.apache.graphar.stdcxx;

import static org.apache.graphar.util.CppClassName.GAR_EDGE_INFO;
import static org.apache.graphar.util.CppClassName.GAR_VERTEX_INFO;
import static org.apache.graphar.util.CppClassName.STD_STRING;
import static org.apache.graphar.util.CppHeaderName.GAR_GRAPH_INFO_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXTemplate;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;

@FFIGen
@CXXHead(system = {"map"})
@CXXHead(GAR_GRAPH_INFO_H)
@FFITypeAlias("std::map")
@CXXTemplate(
        cxx = {STD_STRING, GAR_EDGE_INFO},
        java = {"org.apache.graphar.stdcxx.StdString", "org.apache.graphar.graphinfo.EdgeInfo"})
@CXXTemplate(
        cxx = {STD_STRING, GAR_VERTEX_INFO},
        java = {"org.apache.graphar.stdcxx.StdString", "org.apache.graphar.graphinfo.VertexInfo"})
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
