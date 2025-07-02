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

package org.apache.graphar.types;

import static org.apache.graphar.util.CppClassName.GAR_DATA_TYPE;
import static org.apache.graphar.util.CppHeaderName.GAR_DATA_TYPE_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import org.apache.graphar.stdcxx.StdString;

@FFIGen
@FFITypeAlias(GAR_DATA_TYPE)
@CXXHead(GAR_DATA_TYPE_H)
public interface DataType extends CXXPointer {
    Factory factory = FFITypeFactory.getFactory(DataType.class);

    @CXXOperator("==")
    boolean eq(@CXXReference DataType other);

    @FFIFactory
    interface Factory {
        @CXXValue
        DataType create();

        /**
         * Construct a DateType object
         *
         * @param id
         * @param usrDefinedTypeName c++ default = ""
         * @return a DateType object
         */
        @CXXValue
        DataType create(@CXXValue Type id, @CXXReference StdString usrDefinedTypeName);

        /**
         * Construct a DateType object
         *
         * @param id
         * @return a DateType object
         */
        @CXXValue
        DataType create(@CXXValue Type id);
    }
}
