/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphar.util;

import com.alibaba.fastffi.CXXEnum;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeRefiner;

import static com.alibaba.graphar.util.CppClassName.GAR_STATUS_CODE;

@FFITypeAlias(GAR_STATUS_CODE)
@FFITypeRefiner("com.alibaba.graphar.util.StatusCode.get")
public enum StatusCode implements CXXEnum {
  // success status
  kOK,
  // error status for failed key lookups
  kKeyError,
  // error status for type errors
  kTypeError,
  // error status for invalid data
  kInvalid,
  // error status when an index is out of bounds
  kIndexError,
  // error status for out-of-memory conditions
  kOutOfMemory,
  // error status when some IO-related operation failed
  kIOError,
  // error status when some yaml parse related operation failed
  kYamlError,
  // error status when some arrow-related operation failed
  kArrowError,

  // error status for unknown errors
  kUnknownError;

  @Override
  public int getValue() {
    return ordinal();
  }

  public static StatusCode get(int value) {
    switch (value) {
      case 0:
        return kOK;
      case 1:
        return kKeyError;
      case 2:
        return kTypeError;
      case 3:
        return kInvalid;
      case 4:
        return kIndexError;
      case 5:
        return kOutOfMemory;
      case 6:
        return kIOError;
      case 7:
        return kYamlError;
      case 8:
        return kArrowError;
      case 9:
        return kUnknownError;
      default:
        throw new IllegalArgumentException("Oops: unknown value: " + value);
    }
  }
}
