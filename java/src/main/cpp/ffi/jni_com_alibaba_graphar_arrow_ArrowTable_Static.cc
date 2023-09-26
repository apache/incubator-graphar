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
jlong JNICALL Java_com_alibaba_graphar_arrow_ArrowTable_1Static_1cxx_10x58c7409_nativeFromArrowArrayAndArrowSchema(JNIEnv*, jclass, jlong rv_base, jlong arg0 /* arrayAddress0 */, jlong arg1 /* schemaAddress1 */) {
    auto maybeRecordBatch = arrow::ImportRecordBatch(reinterpret_cast<struct ArrowArray*>(arg0), reinterpret_cast<struct ArrowSchema*>(arg1));
    auto table = arrow::Table::FromRecordBatches({maybeRecordBatch.ValueOrDie()});
	return reinterpret_cast<jlong>(new((void*)rv_base) arrow::Result<std::shared_ptr<arrow::Table>>(table));
}

#ifdef __cplusplus
}
#endif
