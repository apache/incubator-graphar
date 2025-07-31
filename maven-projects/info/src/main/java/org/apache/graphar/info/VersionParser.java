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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VersionParser {
    public static VersionInfo getVersion(String versionStr) {
        try {

            int parsedVersion = parserVersionImpl(versionStr);

            List<String> parsedTypes = parseUserDefineTypes(versionStr);

            return new VersionInfo(parsedVersion, parsedTypes);
        } catch (RuntimeException e) {

            throw new RuntimeException(
                    "Invalid version string: '" + versionStr + "'. Details: " + e.getMessage(), e);
        }
    }

    public static int parserVersionImpl(String versionStr) {

        if (versionStr == null || versionStr.isEmpty()) {
            throw new RuntimeException("Invalid version string: input cannot be null or empty.");
        }

        final Pattern versionRegex = Pattern.compile("gar/v(\\d+).*");

        final Matcher match = versionRegex.matcher(versionStr);

        if (match.matches()) {

            if (match.groupCount() != 1) {
                throw new RuntimeException("Invalid version string: " + versionStr);
            }

            try {
                return Integer.parseInt(match.group(1));
            } catch (NumberFormatException e) {

                throw new RuntimeException(
                        "Invalid version string: Could not parse version number from " + versionStr,
                        e);
            }
        } else {

            throw new RuntimeException(
                    "Invalid version string: Does not match 'gar/v(\\d+).*' format for "
                            + versionStr);
        }
    }

    public static List<String> parseUserDefineTypes(String versionStr) {
        List<String> userDefineTypes = new ArrayList<>();

        final Pattern userDefineTypesRegex = Pattern.compile("gar/v\\d+ *\\((.*)\\).*");
        final Matcher match = userDefineTypesRegex.matcher(versionStr);

        if (match.matches()) {

            if (match.groupCount() != 1) {
                throw new RuntimeException("Invalid version string: " + versionStr);
            }

            String typesStr = match.group(1);

            String[] typesArray = typesStr.split(",", -1);

            for (String type : typesArray) {

                String trimmedType = type.trim();

                if (!trimmedType.isEmpty()) {
                    userDefineTypes.add(trimmedType);
                }
            }
        }

        return userDefineTypes;
    }
}
