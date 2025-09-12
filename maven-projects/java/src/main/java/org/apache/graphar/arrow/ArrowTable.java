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

package org.apache.graphar.arrow;

import static org.apache.graphar.util.CppClassName.ARROW_TABLE;
import static org.apache.graphar.util.CppHeaderName.ARROW_API_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFILibrary;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.graphar.stdcxx.StdSharedPtr;
import org.apache.graphar.stdcxx.StdString;

@FFIGen
@FFITypeAlias(ARROW_TABLE)
@CXXHead(ARROW_API_H)
public interface ArrowTable extends CXXPointer {

    /**
     * Convert VectorSchemaRoot to C++ arrow::Table
     *
     * @param allocator Buffer allocator for allocating C data interface fields
     * @param vsr Vector schema root to export
     * @param provider Dictionary provider for dictionary encoded vectors (optional)
     * @return ArrowTbale shared pointer
     */
    static StdSharedPtr<ArrowTable> fromVectorSchemaRoot(
            BufferAllocator allocator, VectorSchemaRoot vsr, DictionaryProvider provider) {
        ArrowResult<StdSharedPtr<ArrowTable>> maybeTable = null;
        org.apache.arrow.c.ArrowArray arrowArray = ArrowArray.allocateNew(allocator);
        org.apache.arrow.c.ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        Data.exportVectorSchemaRoot(allocator, vsr, provider, arrowArray, arrowSchema);
        maybeTable =
                Static.INSTANCE.fromArrowArrayAndArrowSchema(
                        arrowArray.memoryAddress(), arrowSchema.memoryAddress());
        if (!maybeTable.ok()) {
            throw new RuntimeException(
                    "Error when convert C RecordBatch to C++ Table: "
                            + maybeTable.status().message().toJavaString());
        }
        return maybeTable.ValueOrDie();
    }

    long num_rows();

    @CXXValue
    StdString ToString();

    @FFIGen
    @FFILibrary(value = "arrow", namespace = "arrow")
    interface Static {
        Static INSTANCE = FFITypeFactory.getLibrary(ArrowTable.Static.class);

        /**
         * Convert C ArrowArray and ArrowSchema to C++ arrow::Table with JNI wrote manually
         *
         * @param arrayAddress Address of C ArrowArray
         * @param schemaAddress Address of C ArrowSchema
         * @return ArrowTbale shared pointer Result
         */
        @CXXValue
        ArrowResult<StdSharedPtr<ArrowTable>> fromArrowArrayAndArrowSchema(
                @FFITypeAlias("struct ArrowArray*") long arrayAddress,
                @FFITypeAlias("struct ArrowSchema*") long schemaAddress);
    }
}
