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

#include <jni.h>
#include <new>
#include <iostream>
#include "arrow/api.h"
#include "arrow/c/bridge.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jlong JNICALL Java_org_apache_graphar_arrow_ArrowTable_1Static_1cxx_10x58c7409_nativeFromArrowArrayAndArrowSchema(JNIEnv*, jclass, jlong rv_base, jlong arg0 /* arrayAddress0 */, jlong arg1 /* schemaAddress1 */) {
    auto maybeRecordBatch = arrow::ImportRecordBatch(reinterpret_cast<struct ArrowArray*>(arg0), reinterpret_cast<struct ArrowSchema*>(arg1));
    auto table = arrow::Table::FromRecordBatches({maybeRecordBatch.ValueOrDie()});
	return reinterpret_cast<jlong>(new((void*)rv_base) arrow::Result<std::shared_ptr<arrow::Table>>(table));
}

#ifdef __cplusplus
}
#endif
