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

import static org.apache.graphar.util.CppClassName.GAR_STATUS;
import static org.apache.graphar.util.CppHeaderName.GAR_UTIL_STATUS_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import org.apache.graphar.stdcxx.StdString;

@FFIGen
@FFITypeAlias(GAR_STATUS)
@CXXHead(GAR_UTIL_STATUS_H)
public interface Status extends CXXPointer {
    /** Return true if the status indicates success. */
    boolean ok();

    /** Return true if the status indicates a key lookup error. */
    @FFINameAlias("IsKeyError")
    boolean isKeyError();

    /** Return true if the status indicates a type match error. */
    @FFINameAlias("IsTypeError")
    boolean isTypeError();

    /** Return true if the status indicates invalid data. */
    @FFINameAlias("IsInvalid")
    boolean isInvalid();

    /** Return true if the status indicates an index out of bounds. */
    @FFINameAlias("IsIndexError")
    boolean isIndexError();

    /** Return true if the status indicates a yaml parse related failure. */
    @FFINameAlias("IsYamlError")
    boolean isYamlError();

    /** Return true if the status indicates an arrow-related failure. */
    @FFINameAlias("IsArrowError")
    boolean isArrowError();

    /** Return the StatusCode value attached to this status. */
    @CXXValue
    StatusCode code();

    /** Return the specific error message attached to this status. */
    @CXXReference
    StdString message();
}
