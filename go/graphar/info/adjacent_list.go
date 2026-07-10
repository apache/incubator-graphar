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

import (
	"strings"

	"github.com/apache/incubator-graphar/go/graphar/types"
)

// AdjacentList describes one layout (CSR/CSC/COO-style) of an edge type.
type AdjacentList struct {
	// Type is the layout category. Required and unique within an EdgeInfo.
	Type types.AdjListType
	// FileType is the on-disk physical format.
	FileType types.FileType
	// Prefix is the subdirectory under the edge's prefix; when empty the
	// Type.String() with a trailing slash is used.
	Prefix string
}

// EffectivePrefix returns the explicit Prefix, or Type.String() when empty,
// always with exactly one trailing slash for safe path concatenation.
func (a AdjacentList) EffectivePrefix() string {
	if a.Prefix != "" {
		return strings.TrimRight(a.Prefix, "/") + "/"
	}
	return a.Type.String() + "/"
}

// Validate returns a ValidationError if a is malformed. file_type must be
// csv / parquet / orc; json is rejected, as it is for property groups.
func (a AdjacentList) Validate() error {
	switch a.Type {
	case types.UnorderedBySource, types.UnorderedByDest,
		types.OrderedBySource, types.OrderedByDest:
		// valid
	default:
		return newValidationError(ErrInvalidEdgeInfo, "adj_lists.type",
			"unknown adjacency list type %d", a.Type)
	}
	if a.FileType.IsZero() {
		return newValidationError(ErrInvalidEdgeInfo, "adj_lists.file_type",
			"adjacency list file type must be set")
	}
	if a.FileType == types.FileTypeJSON {
		return newValidationError(ErrInvalidEdgeInfo, "adj_lists.file_type",
			"json file type is not supported in adjacency lists")
	}
	return nil
}

// Equal reports whether two adjacency lists are equivalent.
func (a AdjacentList) Equal(other AdjacentList) bool {
	return a.Type == other.Type &&
		a.FileType == other.FileType &&
		a.Prefix == other.Prefix
}
