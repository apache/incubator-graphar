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

package org.apache.graphar.info.type;

/** Defines how multiple values are handled for a given property key. */
public enum Cardinality {
    /** Single value property */
    SINGLE,
    /** List of values property */
    LIST,
    /** Set of values property (no duplicates) */
    SET;

    @Override
    public String toString() {
        switch (this) {
            case SINGLE:
                return "single";
            case LIST:
                return "list";
            case SET:
                return "set";
            default:
                throw new IllegalArgumentException("Unknown cardinality: " + this);
        }
    }

    public static Cardinality fromString(String cardinality) {
        if (cardinality == null) {
            return null;
        }
        switch (cardinality) {
            case "single":
                return SINGLE;
            case "list":
                return LIST;
            case "set":
                return SET;
            default:
                throw new IllegalArgumentException("Unknown cardinality: " + cardinality);
        }
    }
}
