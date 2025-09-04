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

public class PathUtil {

    public static String resolvePath(String basePath, String subPath) {
        if (subPath == null || subPath.isEmpty()) {
            return basePath;
        }
        if (subPath.contains("://")) {
            return subPath;
        }
        if (!basePath.contains("://") && subPath.startsWith("/")) {
            return subPath;
        }
        return pathToDirectory(basePath) + subPath;
    }

    public static String pathToDirectory(String path) {
        if (path.startsWith("s3://") || path.startsWith("oss://")) {
            int t = path.indexOf('?');
            if (t != -1) {
                String prefix = path.substring(0, t);
                String suffix = path.substring(t);
                int lastSlashIdx = prefix.lastIndexOf('/');
                if (lastSlashIdx != -1) {
                    return prefix.substring(0, lastSlashIdx + 1) + suffix;
                }
            } else {
                int lastSlashIdx = path.lastIndexOf('/');
                if (lastSlashIdx != -1) {
                    return path.substring(0, lastSlashIdx + 1);
                }
                return path;
            }
        } else {
            int lastSlashIdx = path.lastIndexOf('/');
            if (lastSlashIdx != -1) {
                return path.substring(0, lastSlashIdx + 1);
            }
        }
        return path;
    }
}
