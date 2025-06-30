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

package org.apache.graphar.graphinfo;

import static org.apache.graphar.util.CppClassName.GAR_ADJACENT_LIST;
import static org.apache.graphar.util.CppHeaderName.GAR_GRAPH_INFO_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIConst;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import org.apache.graphar.stdcxx.StdString;
import org.apache.graphar.types.AdjListType;
import org.apache.graphar.types.FileType;

@FFIGen
@FFITypeAlias(GAR_ADJACENT_LIST)
@CXXHead(GAR_GRAPH_INFO_H)
public interface AdjacentList extends CXXPointer {

    @FFINameAlias("GetType")
    @CXXValue
    @FFIConst
    AdjListType getType();

    @FFINameAlias("GetFileType")
    @CXXValue
    @FFIConst
    FileType getFileType();

    @FFINameAlias("GetPrefix")
    @FFIConst
    @CXXReference
    StdString getPrefix();

    @FFINameAlias("IsValidated")
    boolean isValidated();
}
