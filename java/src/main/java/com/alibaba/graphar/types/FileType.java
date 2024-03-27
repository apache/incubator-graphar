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

package com.alibaba.graphar.types;

import static com.alibaba.graphar.util.CppClassName.GAR_FILE_TYPE;

import com.alibaba.fastffi.CXXEnum;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeRefiner;

@FFITypeAlias(GAR_FILE_TYPE)
@FFITypeRefiner("com.alibaba.graphar.types.FileType.get")
public enum FileType implements CXXEnum {
    CSV,
    PARQUET,
    ORC;

    public static FileType get(int value) {
        switch (value) {
            case 0:
                return CSV;
            case 1:
                return PARQUET;
            case 2:
                return ORC;
            default:
                throw new IllegalStateException("Unknown value for file type: " + value);
        }
    }

    @Override
    public int getValue() {
        return ordinal();
    }
}
