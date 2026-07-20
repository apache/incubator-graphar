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

func vertexGroupsOK() []PropertyGroup {
	return []PropertyGroup{
		{
			Properties: []Property{
				{Name: "id", Type: types.Int64(), IsPrimary: true, Cardinality: types.CardinalitySingle},
			},
			FileType: types.FileTypeParquet,
		},
		{
			Properties: []Property{
				{Name: "name", Type: types.String(), Cardinality: types.CardinalitySingle},
			},
			FileType: types.FileTypeParquet,
		},
	}
}

func TestNewVertexInfoRejectsZeroVersion(t *testing.T) {
	t.Parallel()
	_, err := NewVertexInfo("person", 100, vertexGroupsOK(),
		WithVertexVersion(types.InfoVersion{}))
	if !errors.Is(err, ErrInvalidVertexInfo) {
		t.Errorf("expected ErrInvalidVertexInfo for zero version, got %v", err)
	}
}

func TestNewVertexInfoDefaults(t *testing.T) {
	t.Parallel()
	v, err := NewVertexInfo("person", 100, vertexGroupsOK())
	if err != nil {
		t.Fatalf("NewVertexInfo: %v", err)
	}
	if v.Type() != "person" || v.ChunkSize() != 100 {
		t.Errorf("getters mismatch: %+v", v)
	}
	if v.Prefix() != "vertex/person/" {
		t.Errorf("default prefix = %q", v.Prefix())
	}
	if v.Version().Version != types.DefaultVersion {
		t.Errorf("default version = %v", v.Version())
	}
	if !v.HasProperty("id") || !v.HasProperty("name") {
		t.Errorf("HasProperty missing expected entries")
	}
	if dt, ok := v.PropertyType("id"); !ok || !dt.Equal(types.Int64()) {
		t.Errorf("PropertyType(id) = %v ok=%v", dt, ok)
	}
	if _, ok := v.PropertyType("nope"); ok {
		t.Errorf("PropertyType(nope) returned ok=true")
	}
}

func TestNewVertexInfoOptions(t *testing.T) {
	t.Parallel()
	v, err := NewVertexInfo("person", 100, vertexGroupsOK(),
		WithVertexPrefix("v/p/"),
		WithVertexLabels("a", "b"),
		WithVertexVersion(types.NewInfoVersion(2)),
	)
	if err != nil {
		t.Fatalf("NewVertexInfo: %v", err)
	}
	if v.Prefix() != "v/p/" {
		t.Errorf("prefix override failed: %q", v.Prefix())
	}
	labels := v.Labels()
	if len(labels) != 2 || labels[0] != "a" || labels[1] != "b" {
		t.Errorf("Labels = %v", labels)
	}
	// Labels return must be a copy.
	labels[0] = "mutated"
	if v.Labels()[0] != "a" {
		t.Errorf("Labels() did not return a copy")
	}
	if v.Version().Version != 2 {
		t.Errorf("version = %v", v.Version())
	}
}

func TestNewVertexInfoValidationFailures(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		fn   func() (*VertexInfo, error)
		want error
	}{
		{
			"empty type",
			func() (*VertexInfo, error) { return NewVertexInfo("", 100, vertexGroupsOK()) },
			ErrInvalidVertexInfo,
		},
		{
			"chunk size zero",
			func() (*VertexInfo, error) { return NewVertexInfo("p", 0, vertexGroupsOK()) },
			ErrInvalidVertexInfo,
		},
		{
			"chunk size negative",
			func() (*VertexInfo, error) { return NewVertexInfo("p", -1, vertexGroupsOK()) },
			ErrInvalidVertexInfo,
		},
		{
			"empty property groups",
			func() (*VertexInfo, error) { return NewVertexInfo("p", 100, nil) },
			ErrInvalidPropertyGroup,
		},
		{
			"empty label",
			func() (*VertexInfo, error) {
				return NewVertexInfo("p", 100, vertexGroupsOK(), WithVertexLabels(""))
			},
			ErrInvalidVertexInfo,
		},
		{
			"duplicate labels",
			func() (*VertexInfo, error) {
				return NewVertexInfo("p", 100, vertexGroupsOK(), WithVertexLabels("a", "a"))
			},
			ErrInvalidVertexInfo,
		},
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

func TestVertexInfoAcceptsZeroOrMultiplePrimaries(t *testing.T) {
	t.Parallel()
	// The Go SDK matches cpp VertexInfo::is_validated which imposes no
	// "exactly one primary" rule; a vertex with zero primaries or with
	// multiple primary properties (across groups) must be accepted to
	// preserve cross-language interop.
	zeroPrimary := []PropertyGroup{{
		Properties: []Property{
			{Name: "name", Type: types.String(), Cardinality: types.CardinalitySingle},
		},
		FileType: types.FileTypeParquet,
	}}
	if _, err := NewVertexInfo("p", 100, zeroPrimary); err != nil {
		t.Errorf("vertex with no primary should be valid: %v", err)
	}

	twoPrimaries := []PropertyGroup{
		{
			Properties: []Property{{Name: "id1", Type: types.Int64(), IsPrimary: true}},
			FileType:   types.FileTypeParquet,
		},
		{
			Properties: []Property{{Name: "id2", Type: types.Int64(), IsPrimary: true}},
			FileType:   types.FileTypeParquet,
		},
	}
	if _, err := NewVertexInfo("p", 100, twoPrimaries); err != nil {
		t.Errorf("vertex with two primaries should be valid (matches cpp): %v", err)
	}
}

func TestVertexInfoAddPropertyGroup(t *testing.T) {
	t.Parallel()
	v, err := NewVertexInfo("person", 100, vertexGroupsOK())
	if err != nil {
		t.Fatalf("NewVertexInfo: %v", err)
	}
	extra := PropertyGroup{
		Properties: []Property{{Name: "age", Type: types.Int32(), Cardinality: types.CardinalitySingle}},
		FileType:   types.FileTypeParquet,
	}
	v2, err := v.AddPropertyGroup(extra)
	if err != nil {
		t.Fatalf("AddPropertyGroup: %v", err)
	}
	if v2 == v {
		t.Error("AddPropertyGroup must return a fresh value")
	}
	if !v2.HasProperty("age") {
		t.Error("new vertex info missing added property")
	}
	if v.HasProperty("age") {
		t.Error("original vertex info was mutated")
	}
}

func TestVertexInfoAddPropertyGroupRejectsDuplicate(t *testing.T) {
	t.Parallel()
	v, err := NewVertexInfo("person", 100, vertexGroupsOK())
	if err != nil {
		t.Fatalf("NewVertexInfo: %v", err)
	}
	dup := PropertyGroup{
		Properties: []Property{{Name: "id", Type: types.Int64(), IsPrimary: true}},
		FileType:   types.FileTypeParquet,
	}
	_, err = v.AddPropertyGroup(dup)
	if !errors.Is(err, ErrInvalidPropertyGroup) {
		t.Errorf("expected duplicate-property failure, got %v", err)
	}
}

// TestWithVersionClonesUserDefinedTypes guards the setter half of the
// immutability contract: mutating the caller's slice after construction must
// not leak into the stored InfoVersion.
func TestWithVersionClonesUserDefinedTypes(t *testing.T) {
	t.Parallel()
	check := func(name string, build func(uts []string) func() types.InfoVersion) {
		uts := []string{"Vector"}
		read := build(uts)
		uts[0] = "Hacked"
		if got := read().UserDefinedTypes; len(got) != 1 || got[0] != "Vector" {
			t.Errorf("%s: setter did not clone, stored %v", name, got)
		}
	}
	check("vertex", func(uts []string) func() types.InfoVersion {
		v, err := NewVertexInfo("p", 1, vertexGroupsOK(),
			WithVertexVersion(types.InfoVersion{Version: 1, UserDefinedTypes: uts}))
		if err != nil {
			t.Fatalf("NewVertexInfo: %v", err)
		}
		return v.Version
	})
	check("edge", func(uts []string) func() types.InfoVersion {
		e, err := NewEdgeInfo("a", "k", "b", 1, 1, 1, false, basicAdjLists(),
			WithEdgeVersion(types.InfoVersion{Version: 1, UserDefinedTypes: uts}))
		if err != nil {
			t.Fatalf("NewEdgeInfo: %v", err)
		}
		return e.Version
	})
	check("graph", func(uts []string) func() types.InfoVersion {
		g, err := NewGraphInfo("g",
			WithGraphVersion(types.InfoVersion{Version: 1, UserDefinedTypes: uts}))
		if err != nil {
			t.Fatalf("NewGraphInfo: %v", err)
		}
		return g.Version
	})
}
