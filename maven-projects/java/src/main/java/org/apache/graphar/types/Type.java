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

import static org.apache.graphar.util.CppClassName.GAR_TYPE;

import com.alibaba.fastffi.CXXEnum;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeRefiner;

@FFITypeAlias(GAR_TYPE)
@FFITypeRefiner("org.apache.graphar.types.Type.get")
public enum Type implements CXXEnum {
    /** Boolean */
    BOOL,

    /** Signed 32-bit integer */
    INT32,

    /** Signed 64-bit integer */
    INT64,

    /** 4-byte floating point value */
    FLOAT,

    /** 8-byte floating point value */
    DOUBLE,

    /** UTF8 variable-length string */
    STRING,

    /** User-defined data type */
    USER_DEFINED,

    // Leave this at the end
    MAX_ID;

    public static Type get(int value) {
        switch (value) {
            case 0:
                return BOOL;
            case 1:
                return INT32;
            case 2:
                return INT64;
            case 3:
                return FLOAT;
            case 4:
                return DOUBLE;
            case 5:
                return STRING;
            case 6:
                return USER_DEFINED;
            case 7:
                return MAX_ID;
            default:
                throw new IllegalStateException("Unknown value for type: " + value);
        }
    }

    @Override
    public int getValue() {
        return ordinal();
    }
}
