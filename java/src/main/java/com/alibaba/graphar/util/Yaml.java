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

package com.alibaba.graphar.util;

import static com.alibaba.graphar.util.CppClassName.GAR_YAML;
import static com.alibaba.graphar.util.CppHeaderName.GAR_UTIL_YAML_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFILibrary;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphar.stdcxx.StdSharedPtr;
import com.alibaba.graphar.stdcxx.StdString;

/** A wrapper of ::Yaml::Node to provide functions to parse yaml. */
@FFIGen
@FFITypeAlias(GAR_YAML)
@CXXHead(GAR_UTIL_YAML_H)
public interface Yaml extends FFIPointer {

    /**
     * Loads the input string as Yaml instance.
     *
     * @return Status::YamlError if input string can not be loaded(malformed).
     */
    static Result<StdSharedPtr<Yaml>> load(StdString input) {
        return Static.INSTANCE.Load(input);
    }

    /**
     * Loads the input file as a single Yaml instance.
     *
     * @return Status::YamlError if the file can not be loaded(malformed).
     */
    static Result<StdSharedPtr<Yaml>> loadFile(StdString fileName) {
        return Static.INSTANCE.LoadFile(fileName);
    }

    @FFIGen
    @CXXHead(GAR_UTIL_YAML_H)
    @FFILibrary(value = GAR_YAML, namespace = GAR_YAML)
    interface Static {
        Static INSTANCE = FFITypeFactory.getLibrary(Yaml.Static.class);

        @CXXValue
        Result<StdSharedPtr<Yaml>> Load(@CXXReference StdString input);

        @CXXValue
        Result<StdSharedPtr<Yaml>> LoadFile(@CXXReference StdString fileName);
    }
}
