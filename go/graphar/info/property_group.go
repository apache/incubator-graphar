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
	"fmt"
	"slices"
	"strings"

	"github.com/apache/incubator-graphar/go/graphar/types"
)

// clonePropertyGroups returns an independent copy of in: the outer slice is
// fresh, and each PropertyGroup's Properties slice is cloned so callers cannot
// mutate stored state through aliased backing arrays.
func clonePropertyGroups(in []PropertyGroup) []PropertyGroup {
	out := make([]PropertyGroup, len(in))
	for i, g := range in {
		out[i] = g
		out[i].Properties = slices.Clone(g.Properties)
	}
	return out
}

// validateUserDefinedTypes checks that every user-defined property type in pg
// is declared in ver's UserDefinedTypes list, matching how a version records
// the custom types a document may use. A nil pg passes.
func validateUserDefinedTypes(pg *PropertyGroups, ver types.InfoVersion, sentinel error) error {
	if pg == nil {
		return nil
	}
	declared := make(map[string]struct{}, len(ver.UserDefinedTypes))
	for _, t := range ver.UserDefinedTypes {
		declared[t] = struct{}{}
	}
	for _, name := range pg.Names() {
		p, _, _ := pg.Property(name)
		if p.Type.ID() != types.UserDefinedType {
			continue
		}
		if _, ok := declared[p.Type.UserDefinedName()]; !ok {
			return newValidationError(sentinel, "property_groups.properties.data_type",
				"user-defined type %q of property %q is not declared in version %q",
				p.Type.UserDefinedName(), p.Name, ver.String())
		}
	}
	return nil
}

// PropertyGroup is a contiguous set of properties stored together in one
// physical file (per chunk).
type PropertyGroup struct {
	// Properties listed in declaration order. Must be non-empty.
	Properties []Property
	// FileType is the on-disk physical format.
	FileType types.FileType
	// Prefix is the subdirectory under the vertex/edge prefix that holds this
	// group's files. When empty, the prefix is derived by joining
	// the property names with '_'.
	Prefix string
}

// EffectivePrefix returns the explicit Prefix (normalised to one trailing
// slash) or, when empty, the property names joined by '_' plus a slash.
func (g PropertyGroup) EffectivePrefix() string {
	if g.Prefix != "" {
		return strings.TrimRight(g.Prefix, "/") + "/"
	}
	names := make([]string, 0, len(g.Properties))
	for _, p := range g.Properties {
		names = append(names, p.Name)
	}
	if len(names) == 0 {
		return ""
	}
	return strings.Join(names, "_") + "/"
}

// Validate checks the group is well-formed: a valid FileType (json rejected),
// non-empty unique-named properties, and the CSV-only restriction against list
// types and non-single cardinality.
func (g PropertyGroup) Validate() error {
	if g.FileType.IsZero() {
		return newValidationError(ErrInvalidPropertyGroup, "file_type",
			"property group must specify a file type")
	}
	if g.FileType == types.FileTypeJSON {
		return newValidationError(ErrInvalidPropertyGroup, "file_type",
			"json file type is not supported in property groups")
	}
	if len(g.Properties) == 0 {
		return newValidationError(ErrInvalidPropertyGroup, "properties",
			"property group must have at least one property")
	}
	seen := make(map[string]struct{}, len(g.Properties))
	for i, p := range g.Properties {
		if err := p.Validate(); err != nil {
			return fmt.Errorf("properties[%d]: %w", i, err)
		}
		if _, dup := seen[p.Name]; dup {
			return newValidationError(ErrInvalidPropertyGroup,
				fmt.Sprintf("properties[%d].name", i),
				"duplicate property name %q within group", p.Name)
		}
		seen[p.Name] = struct{}{}
		// CSV can't hold lists or multi-value cardinality.
		if g.FileType == types.FileTypeCSV {
			if p.Type.ID() == types.ListType {
				return newValidationError(ErrInvalidPropertyGroup,
					fmt.Sprintf("properties[%d].data_type", i),
					"property %q: csv file type does not support list data type", p.Name)
			}
			if p.Cardinality != types.CardinalitySingle {
				return newValidationError(ErrInvalidPropertyGroup,
					fmt.Sprintf("properties[%d].cardinality", i),
					"property %q: csv file type requires cardinality single, got %s",
					p.Name, p.Cardinality)
			}
		}
	}
	return nil
}

// Equal reports whether two property groups carry the same fields in the
// same order.
func (g PropertyGroup) Equal(other PropertyGroup) bool {
	return g.FileType == other.FileType && g.Prefix == other.Prefix &&
		slices.EqualFunc(g.Properties, other.Properties, Property.Equal)
}

// PropertyGroups is a read-only collection of PropertyGroup with name-based
// indexes. Build via NewPropertyGroups, which validates the input and
// constructs the lookup tables once.
type PropertyGroups struct {
	groups []PropertyGroup
	// byName maps property name → index into groups.
	byName map[string]int
	// names is the flat list of property names for ordered iteration.
	names []string
}

// buildPropertyGroups clones the groups and builds the name index without
// validating contents (the liberal load path). It errors only on a duplicate
// property name, which the index cannot represent; empty input is allowed.
func buildPropertyGroups(groups []PropertyGroup) (*PropertyGroups, error) {
	cp := clonePropertyGroups(groups)
	byName := make(map[string]int)
	names := make([]string, 0)
	for i, g := range cp {
		for _, p := range g.Properties {
			if prev, dup := byName[p.Name]; dup {
				return nil, newValidationError(ErrInvalidPropertyGroup,
					fmt.Sprintf("property_groups[%d].properties.name", i),
					"property %q already declared in group %d", p.Name, prev)
			}
			byName[p.Name] = i
			names = append(names, p.Name)
		}
	}
	return &PropertyGroups{groups: cp, byName: byName, names: names}, nil
}

// NewPropertyGroups validates the groups and builds a queryable index. A nil
// or empty input is rejected. This is the strict programmatic constructor; the
// load path uses buildPropertyGroups plus an explicit Validate.
func NewPropertyGroups(groups []PropertyGroup) (*PropertyGroups, error) {
	if len(groups) == 0 {
		return nil, newValidationError(ErrInvalidPropertyGroup, "property_groups",
			"at least one property group is required")
	}
	pg, err := buildPropertyGroups(groups)
	if err != nil {
		return nil, err
	}
	if err := pg.Validate(); err != nil {
		return nil, err
	}
	return pg, nil
}

// Validate checks every contained property group. It does not enforce
// non-emptiness - an empty set is valid at this level; vertex/edge-specific
// rules live on their respective Validate methods.
func (pg *PropertyGroups) Validate() error {
	for i, g := range pg.groups {
		if err := g.Validate(); err != nil {
			return fmt.Errorf("property_groups[%d]: %w", i, err)
		}
	}
	return nil
}

// Groups returns an independent copy of the underlying groups. Callers may
// mutate the returned slice - including each group's Properties - without
// affecting the receiver.
func (pg *PropertyGroups) Groups() []PropertyGroup {
	return clonePropertyGroups(pg.groups)
}

// Names returns property names in declaration order.
func (pg *PropertyGroups) Names() []string {
	return slices.Clone(pg.names)
}

// Has reports whether a property with the given name exists.
func (pg *PropertyGroups) Has(name string) bool {
	_, ok := pg.byName[name]
	return ok
}

// Property returns the named property and the group index it belongs to.
// Reports false when the name is unknown.
func (pg *PropertyGroups) Property(name string) (Property, int, bool) {
	idx, ok := pg.byName[name]
	if !ok {
		return Property{}, -1, false
	}
	for _, p := range pg.groups[idx].Properties {
		if p.Name == name {
			return p, idx, true
		}
	}
	return Property{}, -1, false
}

// PropertyType returns the data type of the named property.
func (pg *PropertyGroups) PropertyType(name string) (types.DataType, bool) {
	p, _, ok := pg.Property(name)
	if !ok {
		return types.DataType{}, false
	}
	return p.Type, true
}

// IsPrimary reports whether the named property is part of the primary key.
// Returns false if the name is unknown.
func (pg *PropertyGroups) IsPrimary(name string) bool {
	p, _, ok := pg.Property(name)
	return ok && p.IsPrimary
}

// IsNullable reports whether the named property is nullable. Returns false
// if the name is unknown.
func (pg *PropertyGroups) IsNullable(name string) bool {
	p, _, ok := pg.Property(name)
	return ok && p.IsNullable
}

// GroupOf returns the group that contains the named property. The returned
// group is an independent copy; mutating it (including its Properties) does
// not affect the receiver.
func (pg *PropertyGroups) GroupOf(name string) (PropertyGroup, bool) {
	idx, ok := pg.byName[name]
	if !ok {
		return PropertyGroup{}, false
	}
	g := pg.groups[idx]
	g.Properties = slices.Clone(g.Properties)
	return g, true
}

// Len returns the number of groups.
func (pg *PropertyGroups) Len() int { return len(pg.groups) }

// HasGroup reports whether g (by structural equality) is among the groups.
func (pg *PropertyGroups) HasGroup(g PropertyGroup) bool {
	for _, candidate := range pg.groups {
		if candidate.Equal(g) {
			return true
		}
	}
	return false
}
