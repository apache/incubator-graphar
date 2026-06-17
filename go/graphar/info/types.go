// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package info

// DataType describes the logical type of a property value.
// The set of values mirrors the Java/C++ DataType enums.
type DataType string

const (
	DataTypeBool      DataType = "bool"
	DataTypeInt32     DataType = "int32"
	DataTypeInt64     DataType = "int64"
	DataTypeFloat     DataType = "float"
	DataTypeDouble    DataType = "double"
	DataTypeString    DataType = "string"
	DataTypeList      DataType = "list"
	DataTypeDate      DataType = "date"
	DataTypeTimestamp DataType = "timestamp"
)

// IsValid checks if the DataType is supported by GraphAr.
func (t DataType) IsValid() bool {
	switch t {
	case DataTypeBool, DataTypeInt32, DataTypeInt64, DataTypeFloat, DataTypeDouble,
		DataTypeString, DataTypeList, DataTypeDate, DataTypeTimestamp:
		return true
	default:
		return false
	}
}

// FileType describes the file format of the data.
type FileType string

const (
	FileTypeCSV     FileType = "csv"
	FileTypeParquet FileType = "parquet"
	FileTypeORC     FileType = "orc"
)

// IsValid checks if the FileType is supported by GraphAr.
func (t FileType) IsValid() bool {
	switch t {
	case FileTypeCSV, FileTypeParquet, FileTypeORC:
		return true
	default:
		return false
	}
}

// Cardinality defines how multiple values are handled for a given property key.
type Cardinality string

const (
	CardinalitySingle Cardinality = "single"
	CardinalityList   Cardinality = "list"
	CardinalitySet    Cardinality = "set"
)

// IsValid checks if the Cardinality is supported by GraphAr.
func (c Cardinality) IsValid() bool {
	switch c {
	case CardinalitySingle, CardinalityList, CardinalitySet:
		return true
	default:
		return false
	}
}

// AdjListType mirrors the Java AdjListType enum.
type AdjListType string

const (
	AdjListUnorderedBySource AdjListType = "unordered_by_source"
	AdjListUnorderedByDest   AdjListType = "unordered_by_dest"
	AdjListOrderedBySource   AdjListType = "ordered_by_source"
	AdjListOrderedByDest     AdjListType = "ordered_by_dest"
)

// IsValid checks if the AdjListType is supported by GraphAr.
func (t AdjListType) IsValid() bool {
	switch t {
	case AdjListUnorderedBySource, AdjListUnorderedByDest,
		AdjListOrderedBySource, AdjListOrderedByDest:
		return true
	default:
		return false
	}
}

// regularSeparator is the regular separator used when concatenating labels/types,
// consistent with GeneralParams.regularSeparator in Java/C++.
const regularSeparator = "__"

const (
	// pathAdjList is the directory name for adjacent list data.
	pathAdjList = "adj_list/"
	// pathOffset is the directory name for offset data.
	pathOffset = "offset/"
	// fileVertexCount is the filename for vertex count metadata.
	fileVertexCount = "vertex_count"
	// fileEdgeCountPrefix is the prefix for edge count metadata files.
	fileEdgeCountPrefix = "edge_count"
	// chunkPrefix is the prefix for chunk data files.
	chunkPrefix = "chunk"
)

const (
	// alignedBySrc indicates data is aligned by source vertex.
	alignedBySrc = "src"
	// alignedByDst indicates data is aligned by destination vertex.
	alignedByDst = "dst"
)

const (
	// versionGarV1 is the legacy version string for GraphAr v1.
	versionGarV1 = "gar/v1"
)
