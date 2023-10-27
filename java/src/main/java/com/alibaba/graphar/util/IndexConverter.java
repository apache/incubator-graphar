package com.alibaba.graphar.util;

import static com.alibaba.graphar.util.CppClassName.GAR_UTIL_INDEX_CONVERTER;
import static com.alibaba.graphar.util.CppHeaderName.GAR_UTIL_UTIL_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFITypeAlias;

/** The iterator for traversing a type of edges. */
@FFIGen
@FFITypeAlias(GAR_UTIL_INDEX_CONVERTER)
@CXXHead(GAR_UTIL_UTIL_H)
public interface IndexConverter extends CXXPointer {
  @FFIFactory
  interface Factory {
    // IndexConverter create(@FFITypeAlias("std::vector<GrapharChive::IdType>&&")
    // StdVector<Long> edgeChunkNums);
  }
}
