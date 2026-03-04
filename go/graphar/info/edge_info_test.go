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
	"testing"
)

func TestEdgeInfo(t *testing.T) {
	e := &EdgeInfo{
		SrcType:      "person",
		EdgeType:     "knows",
		DstType:      "person",
		ChunkSize:    100,
		SrcChunkSize: 100,
		DstChunkSize: 100,
		Directed:     true,
		Prefix:       "person_knows_person/",
		Adjacent: []*AdjacentList{
			{
				Type:     AdjListOrderedBySource,
				FileType: FileTypeParquet,
			},
		},
		Version: VersionInfo{Major: 0, Minor: 1, Patch: 0},
	}

	t.Run("concat", func(t *testing.T) {
		got := concat("a", "b", "c")
		expected := "a" + regularSeparator + "b" + regularSeparator + "c"
		if got != expected {
			t.Errorf("concat() = %v, want %v", got, expected)
		}
	})

	t.Run("key", func(t *testing.T) {
		got := e.Key()
		expected := "person" + regularSeparator + "knows" + regularSeparator + "person"
		if got != expected {
			t.Errorf("Key() = %v, want %v", got, expected)
		}
	})

	t.Run("init", func(t *testing.T) {
		e.PropertyGroups = []*PropertyGroup{
			{
				Properties: []*Property{{Name: "weight", DataType: DataTypeDouble}},
				FileType:   FileTypeParquet,
			},
		}
		if err := e.Init(); err != nil {
			t.Fatalf("init failed: %v", err)
		}
		if !e.pgroups.hasProperty("weight") {
			t.Errorf("expected to have property weight after init")
		}
	})

	t.Run("validate", func(t *testing.T) {
		if err := e.Validate(); err != nil {
			t.Errorf("validate failed: %v", err)
		}

		// empty types
		eErr := *e
		eErr.SrcType = ""
		if err := eErr.Validate(); err == nil {
			t.Error("expected error for empty src/edge/dst type")
		}

		// non-positive chunk sizes
		eErr = *e
		eErr.ChunkSize = 0
		if err := eErr.Validate(); err == nil {
			t.Error("expected error for non-positive chunk_size")
		}

		// base uri/prefix empty
		eErr = *e
		eErr.Prefix = ""
		eErr.BaseURI = nil
		if err := eErr.Validate(); err == nil {
			t.Error("expected error for empty base uri/prefix")
		}

		// missing Adjacent
		eErr = *e
		eErr.Adjacent = nil
		if err := eErr.Validate(); err == nil {
			t.Error("expected error for missing adjacent list definitions")
		}

		// adjacent list with empty type
		eErr = *e
		eErr.Adjacent = []*AdjacentList{{Type: ""}}
		if err := eErr.Validate(); err == nil {
			t.Error("expected error for adjacent list with empty type")
		}

		// property with empty name
		eErr = *e
		eErr.PropertyGroups = []*PropertyGroup{{Properties: []*Property{{Name: "", DataType: DataTypeInt64}}, FileType: FileTypeParquet}}
		if err := eErr.Validate(); err == nil {
			t.Error("expected error for empty property name")
		}

		// duplicated property name
		eErr = *e
		eErr.PropertyGroups = []*PropertyGroup{{
			Properties: []*Property{
				{Name: "p", DataType: DataTypeInt64},
				{Name: "p", DataType: DataTypeInt64},
			},
			FileType: FileTypeParquet,
		}}
		if err := eErr.Validate(); err == nil {
			t.Error("expected error for duplicated property")
		}

		// cardinality must be single
		eErr = *e
		eErr.PropertyGroups = []*PropertyGroup{{
			Properties: []*Property{
				{Name: "p", DataType: DataTypeInt64, Cardinality: CardinalityList},
			},
			FileType: FileTypeParquet,
		}}
		if err := eErr.Validate(); err == nil {
			t.Error("expected error for non-single cardinality")
		}
	})
}
