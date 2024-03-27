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

/*
 * Copyright 2022-2023 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphar.types;

import static com.alibaba.graphar.util.CppClassName.GAR_ADJ_LIST_TYPE;

import com.alibaba.fastffi.CXXEnum;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeRefiner;

@FFITypeAlias(GAR_ADJ_LIST_TYPE)
@FFITypeRefiner("com.alibaba.graphar.types.AdjListType.get")
public enum AdjListType implements CXXEnum {
    /** collection of edges by source, but unordered, can represent COO format */
    unordered_by_source(0b00000001),
    /** collection of edges by destination, but unordered, can represent COO */
    // format
    unordered_by_dest(0b00000010),
    /** collection of edges by source, ordered by source, can represent CSR format */
    ordered_by_source(0b00000100),
    /** collection of edges by destination, ordered by destination, can represent CSC format */
    ordered_by_dest(0b00001000);

    private final int binaryNum;

    AdjListType(int binaryNum) {
        this.binaryNum = binaryNum;
    }

    public static AdjListType get(int binaryValue) {
        switch (binaryValue) {
            case 0b00000001:
                return unordered_by_source;
            case 0b00000010:
                return unordered_by_dest;
            case 0b00000100:
                return ordered_by_source;
            case 0b00001000:
                return ordered_by_dest;
            default:
                throw new IllegalStateException("Unknown value for AdjList type: " + binaryValue);
        }
    }

    @Override
    public String toString() {
        return adjListType2String(this);
    }

    public static String adjListType2String(AdjListType adjListType) {
        switch (adjListType) {
            case unordered_by_source:
                return "unordered_by_source";
            case unordered_by_dest:
                return "unordered_by_dest";
            case ordered_by_source:
                return "ordered_by_source";
            case ordered_by_dest:
                return "ordered_by_dest";
        }
        throw new IllegalStateException("Unknown adjListType:" + adjListType);
    }

    @Override
    public int getValue() {
        return binaryNum;
    }
}
