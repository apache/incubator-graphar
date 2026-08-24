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

import java.util.Objects;

/** A GraphAr logical property type. */
public final class DataType {
    /** Boolean. */
    public static final DataType BOOL = new DataType("bool", null);

    /** Signed 32-bit integer. */
    public static final DataType INT32 = new DataType("int32", null);

    /** Signed 64-bit integer. */
    public static final DataType INT64 = new DataType("int64", null);

    /** 4-byte floating point value. */
    public static final DataType FLOAT = new DataType("float", null);

    /** 8-byte floating point value. */
    public static final DataType DOUBLE = new DataType("double", null);

    /** UTF8 variable-length string. */
    public static final DataType STRING = new DataType("string", null);

    /** Date value. */
    public static final DataType DATE = new DataType("date", null);

    /** Timestamp value. */
    public static final DataType TIMESTAMP = new DataType("timestamp", null);

    private final String typeName;
    private final DataType valueType;

    private DataType(String typeName, DataType valueType) {
        this.typeName = typeName;
        this.valueType = valueType;
    }

    /**
     * Creates a GraphAr list type. The GraphAr v1 metadata format supports lists of the five
     * physical element types that can be represented independently in storage metadata.
     */
    public static DataType listOf(DataType valueType) {
        if (valueType != INT32
                && valueType != INT64
                && valueType != FLOAT
                && valueType != DOUBLE
                && valueType != STRING) {
            throw new IllegalArgumentException("Unsupported GraphAr list value type: " + valueType);
        }
        return new DataType("list", valueType);
    }

    public static DataType fromString(String typeName) {
        if (typeName == null) {
            throw new IllegalArgumentException("Data type must not be null");
        }
        switch (typeName) {
            case "bool":
                return BOOL;
            case "int32":
                return INT32;
            case "int64":
                return INT64;
            case "float":
                return FLOAT;
            case "double":
                return DOUBLE;
            case "string":
                return STRING;
            case "date":
                return DATE;
            case "timestamp":
                return TIMESTAMP;
            default:
                if (typeName.startsWith("list<") && typeName.endsWith(">")) {
                    return listOf(fromString(typeName.substring(5, typeName.length() - 1)));
                }
                throw new IllegalArgumentException("Unknown data type: " + typeName);
        }
    }

    public boolean isList() {
        return valueType != null;
    }

    /** Returns the element type for a list, or {@code null} for a scalar type. */
    public DataType getValueType() {
        return valueType;
    }

    @Override
    public String toString() {
        return isList() ? "list<" + valueType + ">" : typeName;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof DataType)) {
            return false;
        }
        DataType that = (DataType) other;
        return typeName.equals(that.typeName) && Objects.equals(valueType, that.valueType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeName, valueType);
    }
}
