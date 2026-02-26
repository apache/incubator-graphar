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
	"net/url"
	"reflect"
)

// Property describes a single property in a PropertyGroup.
type Property struct {
	Name        string      `yaml:"name"`
	DataType    DataType    `yaml:"data_type"`
	Cardinality Cardinality `yaml:"cardinality,omitempty"`
	Primary     bool        `yaml:"is_primary"`
	Nullable    bool        `yaml:"is_nullable"`
}

// PropertyGroup groups properties stored together with the same file type and base URI.
type PropertyGroup struct {
	Properties []*Property `yaml:"properties"`
	FileType   FileType    `yaml:"file_type"`
	// Prefix where this group's files are stored, relative to the vertex/edge base URI.
	Prefix string `yaml:"prefix"`

	baseURI *url.URL `yaml:"-"`
}

func (pg *PropertyGroup) Validate() error {
	if pg.FileType == "" {
		return fmt.Errorf("%w: property group file type is empty", ErrMissingField)
	}
	if !pg.FileType.IsValid() {
		return fmt.Errorf("%w: invalid file type %q", ErrInvalid, pg.FileType)
	}
	if len(pg.Properties) == 0 {
		return fmt.Errorf("%w: property group has no properties", ErrMissingField)
	}
	for _, p := range pg.Properties {
		if p.Name == "" {
			return fmt.Errorf("%w: property name is empty", ErrMissingField)
		}
		if p.DataType != "" && !p.DataType.IsValid() {
			return fmt.Errorf("%w: invalid data type %q for property %q", ErrInvalid, p.DataType, p.Name)
		}
		if p.Cardinality != "" && !p.Cardinality.IsValid() {
			return fmt.Errorf("%w: invalid cardinality %q for property %q", ErrInvalid, p.Cardinality, p.Name)
		}
	}
	return nil
}

func (pg *PropertyGroup) Init() error {
	if pg.Prefix != "" && pg.baseURI == nil {
		u, err := url.Parse(pg.Prefix)
		if err != nil {
			return fmt.Errorf("%w: invalid property group prefix %q: %v", ErrInvalid, pg.Prefix, err)
		}
		pg.baseURI = u
	}
	return nil
}

func (pg *PropertyGroup) GetPrefix() *url.URL {
	return pg.baseURI
}

func (pg *PropertyGroup) HasProperty(propertyName string) bool {
	for _, p := range pg.Properties {
		if p.Name == propertyName {
			return true
		}
	}
	return false
}

// propertyGroups is an internal helper mirroring the Java PropertyGroups class.
type propertyGroups struct {
	groups     []*PropertyGroup
	propByName map[string]*Property
	groupByKey map[string]*PropertyGroup
}

// newPropertyGroups builds a propertyGroups helper from a list of groups.
func newPropertyGroups(groups []*PropertyGroup) propertyGroups {
	pgs := propertyGroups{
		groups:     make([]*PropertyGroup, len(groups)),
		propByName: make(map[string]*Property),
		groupByKey: make(map[string]*PropertyGroup),
	}
	copy(pgs.groups, groups)

	for _, g := range groups {
		for _, p := range g.Properties {
			pgs.propByName[p.Name] = p
			pgs.groupByKey[p.Name] = g
		}
	}
	return pgs
}

// hasProperty reports whether a property with the given name exists.
func (pgs propertyGroups) hasProperty(name string) bool {
	_, ok := pgs.propByName[name]
	return ok
}

// hasPropertyGroup reports whether an identical group exists.
func (pgs propertyGroups) hasPropertyGroup(group *PropertyGroup) bool {
	for _, g := range pgs.groups {
		if reflect.DeepEqual(g, group) {
			return true
		}
	}
	return false
}

// propertyGroupNum returns the number of property groups.
func (pgs propertyGroups) propertyGroupNum() int {
	return len(pgs.groups)
}

// cardinality returns the cardinality of a named property.
func (pgs propertyGroups) cardinality(name string) (Cardinality, error) {
	p, ok := pgs.propByName[name]
	if !ok {
		return "", fmt.Errorf("%w: property %s not found", ErrNotFound, name)
	}
	return p.Cardinality, nil
}

// propertyType returns the DataType of a named property.
func (pgs propertyGroups) propertyType(name string) (DataType, error) {
	p, ok := pgs.propByName[name]
	if !ok {
		return "", fmt.Errorf("%w: property %s not found", ErrNotFound, name)
	}
	return p.DataType, nil
}

// isPrimaryKey reports whether the given property is a primary key.
func (pgs propertyGroups) isPrimaryKey(name string) (bool, error) {
	p, ok := pgs.propByName[name]
	if !ok {
		return false, fmt.Errorf("%w: property %s not found", ErrNotFound, name)
	}
	return p.Primary, nil
}

// isNullableKey reports whether the given property is nullable.
func (pgs propertyGroups) isNullableKey(name string) (bool, error) {
	p, ok := pgs.propByName[name]
	if !ok {
		return false, fmt.Errorf("%w: property %s not found", ErrNotFound, name)
	}
	return p.Nullable, nil
}

// propertyGroupList returns all property groups.
func (pgs propertyGroups) propertyGroupList() []*PropertyGroup {
	out := make([]*PropertyGroup, len(pgs.groups))
	copy(out, pgs.groups)
	return out
}

// propertyGroupFor returns the group that contains the given property.
func (pgs propertyGroups) propertyGroupFor(name string) (*PropertyGroup, error) {
	g, ok := pgs.groupByKey[name]
	if !ok {
		return nil, fmt.Errorf("%w: property group for %s not found", ErrNotFound, name)
	}
	return g, nil
}
