/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache GraphAr (incubating) project, which was derived from fastFFI.
 */

/*
 * Copyright 1999-2021 Alibaba Group Holding Ltd.
 */

// Derived from alibaba/fastFFI v0.1.2
// https://github.com/alibaba/fastFFI/blob/1eca42b/llvm/src/main/java/com/alibaba/fastffi/stdcxx/StdString.java

package org.apache.graphar.stdcxx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIStringProvider;
import com.alibaba.fastffi.FFIStringReceiver;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.fastffi.llvm.CharPointer;
import com.alibaba.fastffi.llvm.LLVMPointer;

@FFIGen
@CXXHead(system = "string")
@FFITypeAlias("std::string")
public interface StdString extends CXXPointer, LLVMPointer, FFIStringReceiver, FFIStringProvider {

    static StdString create() {
        return factory.create();
    }

    static StdString create(CharPointer buf) {
        return factory.create(buf);
    }

    static StdString create(CharPointer buf, long length) {
        return factory.create(buf, length);
    }

    static StdString create(String string) {
        return factory.create(string);
    }

    Factory factory = FFITypeFactory.getFactory(StdString.class);

    @FFIFactory
    interface Factory {
        StdString create();

        StdString create(CharPointer buf);

        StdString create(CharPointer buf, long length);

        default StdString create(String string) {
            StdString std = create();
            std.fromJavaString(string);
            return std;
        }

        StdString create(@CXXReference StdString string);
    }

    @CXXOperator("==")
    boolean eq(@CXXReference StdString other);

    long size();

    long data();

    void resize(long size);

    long c_str();
}
