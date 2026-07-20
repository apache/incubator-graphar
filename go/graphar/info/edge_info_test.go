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
	"errors"
	"testing"

	"github.com/apache/incubator-graphar/go/graphar/types"
)

func basicAdjLists() []AdjacentList {
	return []AdjacentList{
		{Type: types.OrderedBySource, FileType: types.FileTypeParquet},
		{Type: types.OrderedByDest, FileType: types.FileTypeParquet},
	}
}

func TestNewEdgeInfoRejectsZeroVersion(t *testing.T) {
	t.Parallel()
	_, err := NewEdgeInfo("person", "knows", "person", 1024, 100, 100, true, basicAdjLists(),
		WithEdgeVersion(types.InfoVersion{}))
	if !errors.Is(err, ErrInvalidEdgeInfo) {
		t.Errorf("expected ErrInvalidEdgeInfo for zero version, got %v", err)
	}
}

func TestNewEdgeInfoDefaults(t *testing.T) {
	t.Parallel()
	e, err := NewEdgeInfo("person", "knows", "person", 1024, 100, 100, true, basicAdjLists())
	if err != nil {
		t.Fatalf("NewEdgeInfo: %v", err)
	}
	if e.SrcType() != "person" || e.EdgeType() != "knows" || e.DstType() != "person" {
		t.Errorf("triplet mismatch: %s/%s/%s", e.SrcType(), e.EdgeType(), e.DstType())
	}
	src, edge, dst := e.Triplet()
	if src != "person" || edge != "knows" || dst != "person" {
		t.Errorf("Triplet() = %s/%s/%s", src, edge, dst)
	}
	if e.ChunkSize() != 1024 || e.SrcChunkSize() != 100 || e.DstChunkSize() != 100 {
		t.Errorf("chunk sizes mismatch")
	}
	if !e.Directed() {
		t.Errorf("Directed should be true")
	}
	if e.Prefix() != "edge/person_knows_person/" {
		t.Errorf("default prefix = %q", e.Prefix())
	}
	if e.Version().Version != types.DefaultVersion {
		t.Errorf("default version mismatch")
	}
	if len(e.AdjLists()) != 2 {
		t.Errorf("AdjLists len = %d", len(e.AdjLists()))
	}
	if got := e.AdjListTypes(); len(got) != 2 || got[0] != types.OrderedBySource || got[1] != types.OrderedByDest {
		t.Errorf("AdjListTypes = %v", got)
	}
	if _, ok := e.AdjList(types.OrderedBySource); !ok {
		t.Errorf("AdjList lookup failed")
	}
	if _, ok := e.AdjList(types.UnorderedByDest); ok {
		t.Errorf("AdjList returned ok for missing type")
	}
	if e.PropertyGroups() != nil {
		t.Errorf("PropertyGroups should be nil when not provided")
	}
}

func TestNewEdgeInfoWithProperties(t *testing.T) {
	t.Parallel()
	groups := []PropertyGroup{{
		Properties: []Property{{Name: "since", Type: types.Int64(), Cardinality: types.CardinalitySingle}},
		FileType:   types.FileTypeParquet,
	}}
	e, err := NewEdgeInfo("person", "knows", "person", 1024, 100, 100, false, basicAdjLists(),
		WithEdgePrefix("custom/"),
		WithEdgePropertyGroups(groups),
		WithEdgeVersion(types.NewInfoVersion(2)),
	)
	if err != nil {
		t.Fatalf("NewEdgeInfo: %v", err)
	}
	if e.Prefix() != "custom/" {
		t.Errorf("prefix override failed: %q", e.Prefix())
	}
	if !e.HasProperty("since") {
		t.Errorf("HasProperty(since) returned false")
	}
	if e.PropertyGroups() == nil {
		t.Fatalf("PropertyGroups nil")
	}
	if e.Version().Version != 2 {
		t.Errorf("version override failed")
	}
}

func TestNewEdgeInfoRejectsListCardinality(t *testing.T) {
	t.Parallel()
	groups := []PropertyGroup{{
		Properties: []Property{{Name: "tags", Type: types.String(), Cardinality: types.CardinalityList}},
		FileType:   types.FileTypeParquet,
	}}
	_, err := NewEdgeInfo("person", "knows", "person", 1024, 100, 100, false, basicAdjLists(),
		WithEdgePropertyGroups(groups))
	if !errors.Is(err, ErrInvalidEdgeInfo) {
		t.Errorf("expected ErrInvalidEdgeInfo for list cardinality on edge, got %v", err)
	}
}

func TestNewEdgeInfoValidationFailures(t *testing.T) {
	t.Parallel()
	good := basicAdjLists()
	cases := []struct {
		name string
		fn   func() (*EdgeInfo, error)
		want error
	}{
		{"empty src", func() (*EdgeInfo, error) {
			return NewEdgeInfo("", "k", "d", 1, 1, 1, false, good)
		}, ErrInvalidEdgeInfo},
		{"empty edge", func() (*EdgeInfo, error) {
			return NewEdgeInfo("s", "", "d", 1, 1, 1, false, good)
		}, ErrInvalidEdgeInfo},
		{"empty dst", func() (*EdgeInfo, error) {
			return NewEdgeInfo("s", "k", "", 1, 1, 1, false, good)
		}, ErrInvalidEdgeInfo},
		{"chunk_size 0", func() (*EdgeInfo, error) {
			return NewEdgeInfo("s", "k", "d", 0, 1, 1, false, good)
		}, ErrInvalidEdgeInfo},
		{"src_chunk_size 0", func() (*EdgeInfo, error) {
			return NewEdgeInfo("s", "k", "d", 1, 0, 1, false, good)
		}, ErrInvalidEdgeInfo},
		{"dst_chunk_size 0", func() (*EdgeInfo, error) {
			return NewEdgeInfo("s", "k", "d", 1, 1, 0, false, good)
		}, ErrInvalidEdgeInfo},
		{"no adj lists", func() (*EdgeInfo, error) {
			return NewEdgeInfo("s", "k", "d", 1, 1, 1, false, nil)
		}, ErrInvalidEdgeInfo},
		{"duplicate adj list types", func() (*EdgeInfo, error) {
			dup := []AdjacentList{
				{Type: types.OrderedBySource, FileType: types.FileTypeParquet},
				{Type: types.OrderedBySource, FileType: types.FileTypeCSV},
			}
			return NewEdgeInfo("s", "k", "d", 1, 1, 1, false, dup)
		}, ErrInvalidEdgeInfo},
		{"invalid adj list", func() (*EdgeInfo, error) {
			bad := []AdjacentList{{FileType: types.FileTypeCSV}} // missing Type
			return NewEdgeInfo("s", "k", "d", 1, 1, 1, false, bad)
		}, ErrInvalidEdgeInfo},
		{"invalid property group", func() (*EdgeInfo, error) {
			groups := []PropertyGroup{{FileType: types.FileTypeParquet}} // empty properties
			return NewEdgeInfo("s", "k", "d", 1, 1, 1, false, good, WithEdgePropertyGroups(groups))
		}, ErrInvalidPropertyGroup},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := c.fn()
			if !errors.Is(err, c.want) {
				t.Errorf("got %v, want wrap of %v", err, c.want)
			}
		})
	}
}

func TestEdgeInfoAdjListsSorted(t *testing.T) {
	t.Parallel()
	mixed := []AdjacentList{
		{Type: types.OrderedByDest, FileType: types.FileTypeParquet},
		{Type: types.UnorderedBySource, FileType: types.FileTypeParquet},
		{Type: types.OrderedBySource, FileType: types.FileTypeParquet},
	}
	e, err := NewEdgeInfo("s", "k", "d", 1, 1, 1, false, mixed)
	if err != nil {
		t.Fatalf("NewEdgeInfo: %v", err)
	}
	got := e.AdjLists()
	for i := 1; i < len(got); i++ {
		if got[i-1].Type > got[i].Type {
			t.Errorf("AdjLists not sorted: %v", got)
			break
		}
	}
	types_ := e.AdjListTypes()
	for i := 1; i < len(types_); i++ {
		if types_[i-1] > types_[i] {
			t.Errorf("AdjListTypes not sorted: %v", types_)
			break
		}
	}
}
