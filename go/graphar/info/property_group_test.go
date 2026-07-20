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

func makeProp(name string, dt types.DataType, primary bool) Property {
	return Property{Name: name, Type: dt, IsPrimary: primary, Cardinality: types.CardinalitySingle}
}

func TestPropertyGroupEffectivePrefix(t *testing.T) {
	t.Parallel()
	g := PropertyGroup{
		Properties: []Property{makeProp("a", types.Int32(), false), makeProp("b", types.Int32(), false)},
		FileType:   types.FileTypeParquet,
	}
	if got := g.EffectivePrefix(); got != "a_b/" {
		t.Errorf("EffectivePrefix() = %q, want a_b/", got)
	}
	g.Prefix = "name_age/"
	if got := g.EffectivePrefix(); got != "name_age/" {
		t.Errorf("explicit prefix not honoured: %q", got)
	}
	// An explicit prefix without a trailing slash is normalised to one.
	g.Prefix = "name_age"
	if got := g.EffectivePrefix(); got != "name_age/" {
		t.Errorf("prefix trailing slash not normalised: %q", got)
	}
	// Empty group: EffectivePrefix returns empty so we don't synthesise "/"
	empty := PropertyGroup{FileType: types.FileTypeCSV}
	if got := empty.EffectivePrefix(); got != "" {
		t.Errorf("empty group EffectivePrefix = %q", got)
	}
}

func TestPropertyGroupValidate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		group PropertyGroup
		want  error
	}{
		{
			name:  "empty properties",
			group: PropertyGroup{FileType: types.FileTypeParquet},
			want:  ErrInvalidPropertyGroup,
		},
		{
			name: "invalid inner property",
			group: PropertyGroup{
				Properties: []Property{{Type: types.Int32()}}, // missing name
				FileType:   types.FileTypeParquet,
			},
			want: ErrInvalidProperty,
		},
		{
			name: "duplicate property name",
			group: PropertyGroup{
				Properties: []Property{makeProp("x", types.Int32(), false), makeProp("x", types.Int64(), false)},
				FileType:   types.FileTypeParquet,
			},
			want: ErrInvalidPropertyGroup,
		},
		{
			name: "json file type rejected",
			group: PropertyGroup{
				Properties: []Property{makeProp("a", types.Int32(), false)},
				FileType:   types.FileTypeJSON,
			},
			want: ErrInvalidPropertyGroup,
		},
		{
			name: "csv rejects list data type",
			group: PropertyGroup{
				Properties: []Property{makeProp("t", types.List(types.Int32()), false)},
				FileType:   types.FileTypeCSV,
			},
			want: ErrInvalidPropertyGroup,
		},
		{
			name: "csv rejects non-single cardinality",
			group: PropertyGroup{
				Properties: []Property{{Name: "t", Type: types.String(), Cardinality: types.CardinalityList}},
				FileType:   types.FileTypeCSV,
			},
			want: ErrInvalidPropertyGroup,
		},
		{
			name: "csv allows scalar single (control)",
			group: PropertyGroup{
				Properties: []Property{makeProp("a", types.Int32(), false)},
				FileType:   types.FileTypeCSV,
			},
			want: nil,
		},
		{
			name: "parquet allows list (control)",
			group: PropertyGroup{
				Properties: []Property{makeProp("t", types.List(types.Int32()), false)},
				FileType:   types.FileTypeParquet,
			},
			want: nil,
		},
		{
			name: "ok",
			group: PropertyGroup{
				Properties: []Property{makeProp("a", types.Int32(), false)},
				FileType:   types.FileTypeParquet,
			},
			want: nil,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.group.Validate()
			if c.want == nil {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				return
			}
			if !errors.Is(err, c.want) {
				t.Errorf("got %v, want wrap of %v", err, c.want)
			}
		})
	}
}

func TestPropertyGroupEqual(t *testing.T) {
	t.Parallel()
	base := PropertyGroup{
		Properties: []Property{makeProp("a", types.Int32(), false)},
		FileType:   types.FileTypeParquet,
		Prefix:     "p/",
	}
	if !base.Equal(base) {
		t.Error("reflexive failed")
	}
	other := base
	other.FileType = types.FileTypeCSV
	if base.Equal(other) {
		t.Error("different file types reported equal")
	}
	other = base
	other.Prefix = "q/"
	if base.Equal(other) {
		t.Error("different prefixes reported equal")
	}
	other = base
	other.Properties = []Property{makeProp("a", types.Int64(), false)}
	if base.Equal(other) {
		t.Error("different inner properties reported equal")
	}
	other = base
	other.Properties = append([]Property{makeProp("x", types.Int32(), false)}, other.Properties...)
	if base.Equal(other) {
		t.Error("different property lists (length) reported equal")
	}
}

func TestNewPropertyGroupsEmpty(t *testing.T) {
	t.Parallel()
	_, err := NewPropertyGroups(nil)
	if !errors.Is(err, ErrInvalidPropertyGroup) {
		t.Errorf("expected ErrInvalidPropertyGroup, got %v", err)
	}
	_, err = NewPropertyGroups([]PropertyGroup{})
	if !errors.Is(err, ErrInvalidPropertyGroup) {
		t.Errorf("expected ErrInvalidPropertyGroup, got %v", err)
	}
}

func TestNewPropertyGroupsDuplicateAcrossGroups(t *testing.T) {
	t.Parallel()
	g1 := PropertyGroup{
		Properties: []Property{makeProp("id", types.Int64(), true)},
		FileType:   types.FileTypeParquet,
	}
	g2 := PropertyGroup{
		Properties: []Property{makeProp("id", types.Int64(), false)},
		FileType:   types.FileTypeParquet,
	}
	_, err := NewPropertyGroups([]PropertyGroup{g1, g2})
	if !errors.Is(err, ErrInvalidPropertyGroup) {
		t.Errorf("expected ErrInvalidPropertyGroup, got %v", err)
	}
}

func TestPropertyGroupsLookup(t *testing.T) {
	t.Parallel()
	pg, err := NewPropertyGroups([]PropertyGroup{
		{Properties: []Property{makeProp("id", types.Int64(), true)}, FileType: types.FileTypeParquet},
		{Properties: []Property{
			makeProp("firstName", types.String(), false),
			{Name: "age", Type: types.Int32(), IsNullable: true, Cardinality: types.CardinalitySingle},
		}, FileType: types.FileTypeParquet},
	})
	if err != nil {
		t.Fatalf("NewPropertyGroups: %v", err)
	}
	if pg.Len() != 2 {
		t.Errorf("Len = %d, want 2", pg.Len())
	}
	if !pg.Has("id") || pg.Has("missing") {
		t.Errorf("Has incorrect")
	}
	if dt, ok := pg.PropertyType("firstName"); !ok || !dt.Equal(types.String()) {
		t.Errorf("PropertyType(firstName) = %v ok=%v", dt, ok)
	}
	if _, ok := pg.PropertyType("missing"); ok {
		t.Errorf("PropertyType for missing returned ok=true")
	}
	if !pg.IsPrimary("id") {
		t.Errorf("IsPrimary(id) should be true")
	}
	if pg.IsPrimary("missing") {
		t.Errorf("IsPrimary(missing) should be false")
	}
	if !pg.IsNullable("age") {
		t.Errorf("IsNullable(age) should be true")
	}
	if pg.IsNullable("missing") {
		t.Errorf("IsNullable(missing) should be false")
	}

	p, idx, ok := pg.Property("id")
	if !ok || idx != 0 || p.Name != "id" {
		t.Errorf("Property(id) = (%+v, %d, %v)", p, idx, ok)
	}
	if _, _, ok := pg.Property("nope"); ok {
		t.Errorf("Property(nope) returned ok=true")
	}

	if g, ok := pg.GroupOf("firstName"); !ok || g.FileType != types.FileTypeParquet {
		t.Errorf("GroupOf(firstName) = (%+v, %v)", g, ok)
	}
	if _, ok := pg.GroupOf("nope"); ok {
		t.Errorf("GroupOf(nope) returned ok=true")
	}

	names := pg.Names()
	if len(names) != 3 || names[0] != "id" || names[1] != "firstName" || names[2] != "age" {
		t.Errorf("Names = %v", names)
	}

	groups := pg.Groups()
	if len(groups) != 2 {
		t.Errorf("Groups len = %d", len(groups))
	}
	if !pg.HasGroup(groups[0]) {
		t.Errorf("HasGroup self-check failed")
	}
	stranger := PropertyGroup{Properties: []Property{makeProp("z", types.Int32(), false)}, FileType: types.FileTypeCSV}
	if pg.HasGroup(stranger) {
		t.Errorf("HasGroup returned true for stranger")
	}
}

func TestPropertyGroupsDefensiveCopy(t *testing.T) {
	t.Parallel()
	g := PropertyGroup{
		Properties: []Property{makeProp("id", types.Int64(), true)},
		FileType:   types.FileTypeParquet,
	}
	pg, err := NewPropertyGroups([]PropertyGroup{g})
	if err != nil {
		t.Fatalf("NewPropertyGroups: %v", err)
	}
	// Mutating the returned slice must not affect the receiver.
	out := pg.Groups()
	out[0] = PropertyGroup{}
	if !pg.HasGroup(g) {
		t.Errorf("PropertyGroups was mutated via Groups() copy")
	}
	names := pg.Names()
	names[0] = "mutated"
	if pg.Names()[0] == "mutated" {
		t.Errorf("Names() did not return a defensive copy")
	}
}

func TestPropertyGroupsDeepCopyOnConstruct(t *testing.T) {
	t.Parallel()
	// Mutating the caller's input AFTER NewPropertyGroups returned must not
	// be visible through the receiver - guards against the shallow-copy
	// aliasing surfaced by the audit.
	props := []Property{makeProp("id", types.Int64(), true)}
	in := []PropertyGroup{{Properties: props, FileType: types.FileTypeParquet}}
	pg, err := NewPropertyGroups(in)
	if err != nil {
		t.Fatalf("NewPropertyGroups: %v", err)
	}
	props[0].Name = "leaked"
	in[0].Properties[0].IsPrimary = false
	in[0].FileType = types.FileTypeCSV

	if !pg.IsPrimary("id") {
		t.Error("input mutation leaked through inner Properties slice")
	}
	if pg.Has("leaked") {
		t.Error("input mutation introduced a name into the receiver")
	}
	got, _ := pg.GroupOf("id")
	if got.FileType != types.FileTypeParquet {
		t.Errorf("outer-group field leaked: FileType=%v", got.FileType)
	}
}

func TestPropertyGroupsDeepCopyOnRead(t *testing.T) {
	t.Parallel()
	// Mutating the slice returned by Groups() - including each group's
	// Properties - must not affect the receiver.
	pg, err := NewPropertyGroups([]PropertyGroup{{
		Properties: []Property{makeProp("id", types.Int64(), true)},
		FileType:   types.FileTypeParquet,
	}})
	if err != nil {
		t.Fatalf("NewPropertyGroups: %v", err)
	}
	out := pg.Groups()
	out[0].Properties[0].IsPrimary = false
	out[0].Properties[0].Name = "renamed"

	if !pg.IsPrimary("id") {
		t.Error("mutation via Groups()[0].Properties[0] leaked into receiver")
	}
	if pg.Has("renamed") {
		t.Error("mutation introduced a new name into the receiver index")
	}
}

func TestPropertyGroupsGroupOfDefensiveCopy(t *testing.T) {
	t.Parallel()
	// Mutating the group returned by GroupOf() - including its Properties -
	// must not affect the receiver.
	pg, err := NewPropertyGroups([]PropertyGroup{{
		Properties: []Property{makeProp("id", types.Int64(), true)},
		FileType:   types.FileTypeParquet,
	}})
	if err != nil {
		t.Fatalf("NewPropertyGroups: %v", err)
	}
	got, ok := pg.GroupOf("id")
	if !ok {
		t.Fatal("GroupOf(id) returned ok=false")
	}
	got.Properties[0].IsPrimary = false
	got.Properties[0].Name = "renamed"

	if !pg.IsPrimary("id") {
		t.Error("mutation via GroupOf().Properties[0] leaked into receiver")
	}
	if pg.Has("renamed") {
		t.Error("mutation introduced a new name into the receiver index")
	}
}
