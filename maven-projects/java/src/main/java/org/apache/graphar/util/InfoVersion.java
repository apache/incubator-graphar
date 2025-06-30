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

package org.apache.graphar.util;

import static org.apache.graphar.util.CppClassName.GAR_INFO_VERSION;
import static org.apache.graphar.util.CppHeaderName.GAR_VERSION_PARSER_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import org.apache.graphar.stdcxx.StdString;
import org.apache.graphar.stdcxx.StdVector;

/** InfoVersion is a class provide version information of info. */
@FFIGen
@CXXHead(GAR_VERSION_PARSER_H)
@FFITypeAlias(GAR_INFO_VERSION)
public interface InfoVersion extends CXXPointer {

    static InfoVersion create(int version) {
        return factory.create(version);
    }

    /**
     * Check if two InfoVersion are equal
     *
     * @param other
     * @return equal ro not
     */
    @CXXOperator("==")
    boolean eq(@CXXReference InfoVersion other);

    /**
     * get version integer
     *
     * @return version integer
     */
    int version();

    /**
     * get user define types
     *
     * @return StdVector of StdString
     */
    @FFINameAlias("user_define_types")
    @FFITypeAlias("std::vector<std::string>")
    @CXXReference
    StdVector<StdString> userDefineTypes();

    /**
     * describe the InfoVersion like toString, but return StdString
     *
     * @return StdString that describe the InfoVersion
     */
    @FFINameAlias("ToString")
    @CXXValue
    StdString toStdString();

    /**
     * Check specific type in InfoVersion
     *
     * @param typeStr StdString of type that you want check
     * @return whether InfoVersion has this type
     */
    @FFINameAlias("CheckType")
    boolean checkType(@CXXReference StdString typeStr);

    Factory factory = FFITypeFactory.getFactory(InfoVersion.class);

    @FFIFactory
    interface Factory {
        /** Default constructor */
        InfoVersion create();

        /** Constructor with version */
        InfoVersion create(int version);

        /** Constructor with version and user defined types. */
        InfoVersion create(int version, @CXXReference StdVector<StdString> userDefineTypes);

        /** Constructor with version and user defined types. */
        InfoVersion create(@CXXReference InfoVersion other);
    }
}
