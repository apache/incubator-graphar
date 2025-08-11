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

package org.apache.graphar.info;

import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class VersionInfo {
    private int version;
    private List<String> userDefinedTypes;
    private final Map<Integer, List<String>> version2types =
            Map.of(1, List.of("bool", "int32", "int64", "float", "double", "string"));

    public VersionInfo(Integer version, List<String> userDefinedTypes) {
        this.version = version;
        this.userDefinedTypes = userDefinedTypes;
    }

    public int getVersion() {
        return version;
    }

    public List<String> getUserDefinedTypes() {
        return userDefinedTypes;
    }

    /** Dump version to string. */
    public String toString() {
        StringBuilder str = new StringBuilder("gar/v").append(version);

        if (userDefinedTypes != null && !userDefinedTypes.isEmpty()) {
            str.append(" (");
            // 使用 StringJoiner 更优雅地拼接带分隔符的字符串
            StringJoiner sj = new StringJoiner(",");
            for (String type : userDefinedTypes) {
                sj.add(type);
            }
            str.append(sj.toString()).append(")");
        }
        return str.toString();
    }

    /** Check if type is supported by version. */
    public boolean checkType(final String typeStr) {
        if (version2types == null || !version2types.containsKey(version)) {
            return false;
        }
        List<String> types = version2types.get(version);
        if (types.contains(typeStr)) {
            return true;
        }
        return userDefinedTypes != null && userDefinedTypes.contains(typeStr);
    }
}
