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
// https://github.com/alibaba/GraphScope/blob/8235b29/analytical_engine/java/grape-jdk/src/main/java/com/alibaba/graphscope/stdcxx/StdSharedPtr.java

package org.apache.graphar.stdcxx;

import static org.apache.graphar.util.CppClassName.ARROW_ARRAY;
import static org.apache.graphar.util.CppClassName.ARROW_TABLE;
import static org.apache.graphar.util.CppClassName.GAR_UTIL_INDEX_CONVERTER;
import static org.apache.graphar.util.CppHeaderName.ARROW_API_H;
import static org.apache.graphar.util.CppHeaderName.GAR_UTIL_UTIL_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXTemplate;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;

@FFIGen
@CXXHead(system = "memory")
@CXXHead(ARROW_API_H)
@CXXHead(GAR_UTIL_UTIL_H)
@FFITypeAlias("std::shared_ptr")
@CXXTemplate(cxx = GAR_UTIL_INDEX_CONVERTER, java = "org.apache.graphar.util.IndexConverter")
@CXXTemplate(cxx = ARROW_TABLE, java = "org.apache.graphar.arrow.ArrowTable")
@CXXTemplate(cxx = ARROW_ARRAY, java = "org.apache.graphar.arrow.ArrowArray")
public interface StdSharedPtr<T extends FFIPointer> extends FFIPointer {
    // & will return the pointer of T.
    // shall be cxxvalue?
    T get();
}
