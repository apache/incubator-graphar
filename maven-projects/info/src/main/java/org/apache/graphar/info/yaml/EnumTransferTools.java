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

package org.apache.graphar.info.yaml;

import org.apache.graphar.proto.AdjListType;
import org.apache.graphar.proto.FileType;

public class EnumTransferTools {
    static String fileType2String(FileType fileType) {
        switch (fileType) {
            case CSV:
                return "csv";
            case PARQUET:
                return "parquet";
            case ORC:
                return "orc";
            case JSON:
                return "json";
            case AVRO:
                return "avro";
            case HDF5:
                return "hdf5";
            default:
                throw new IllegalArgumentException("Invalid fileType: " + fileType);
        }
    }

    static FileType string2FileType(String fileType) {
        switch (fileType) {
            case "csv":
                return FileType.CSV;
            case "parquet":
                return FileType.PARQUET;
            case "orc":
                return FileType.ORC;
            case "json":
                return FileType.JSON;
            case "avro":
                return FileType.AVRO;
            case "hdf5":
                return FileType.HDF5;
            default:
                throw new IllegalArgumentException("Invalid fileType: " + fileType);
        }
    }

    static String dataType2String(org.apache.graphar.proto.DataType dataType) {
        switch (dataType) {
            case BOOL:
                return "bool";
            case INT32:
                return "int32";
            case INT64:
                return "int64";
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case STRING:
                return "string";
            case LIST:
                return "list";
            case DATE:
                return "date";
            case TIMESTAMP:
                return "timestamp";
            case TIME:
                return "time";
            default:
                throw new IllegalArgumentException("Invalid dataType: " + dataType);
        }
    }

    static org.apache.graphar.proto.DataType string2DataType(String dataType) {
        switch (dataType) {
            case "bool":
                return org.apache.graphar.proto.DataType.BOOL;
            case "int32":
                return org.apache.graphar.proto.DataType.INT32;
            case "int64":
                return org.apache.graphar.proto.DataType.INT64;
            case "float":
                return org.apache.graphar.proto.DataType.FLOAT;
            case "double":
                return org.apache.graphar.proto.DataType.DOUBLE;
            case "string":
                return org.apache.graphar.proto.DataType.STRING;
            case "list":
                return org.apache.graphar.proto.DataType.LIST;
            case "date":
                return org.apache.graphar.proto.DataType.DATE;
            case "timestamp":
                return org.apache.graphar.proto.DataType.TIMESTAMP;
            case "time":
                return org.apache.graphar.proto.DataType.TIME;
            default:
                throw new IllegalArgumentException("Invalid dataType: " + dataType);
        }
    }

    public static AdjListType orderedAndAlignedBy2AdjListType(boolean ordered, String alignedBy) {
        switch (alignedBy) {
            case "src":
                return ordered ? AdjListType.ORDERED_BY_SOURCE : AdjListType.UNORDERED_BY_SOURCE;
            case "dst":
                return ordered ? AdjListType.ORDERED_BY_TARGET : AdjListType.UNORDERED_BY_TARGET;
            default:
                throw new IllegalArgumentException("Invalid alignedBy: " + alignedBy);
        }
    }
}
