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

package com.alibaba.graphar.types;

import static com.alibaba.graphar.util.CppClassName.GAR_VALIDATE_LEVEL;

import com.alibaba.fastffi.CXXEnum;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeRefiner;

/** The level for validating writing operations. */
@FFITypeAlias(GAR_VALIDATE_LEVEL)
@FFITypeRefiner("com.alibaba.graphar.types.ValidateLevel.get")
public enum ValidateLevel implements CXXEnum {
  /** To use the default validate level of the writer/builder. */
  default_validate,
  /** To skip the validation. */
  no_validate,
  /**
   * Weak validation: check if the index, count, adj_list type, property group and the size of the
   * table passed to the writer/builder are valid.
   */
  weak_validate,
  /**
   * Strong validation: except for the weak validation, also check if the schema (including each
   * property name and data type) of the input data passed to the writer/builder is consistent with
   * that defined in the info.
   */
  strong_validate;

  public static ValidateLevel get(int value) {
    switch (value) {
      case 0:
        return default_validate;
      case 1:
        return no_validate;
      case 2:
        return weak_validate;
      case 3:
        return strong_validate;
      default:
        throw new IllegalStateException("Unknown value for validate level: " + value);
    }
  }

  @Override
  public int getValue() {
    return ordinal();
  }
}
