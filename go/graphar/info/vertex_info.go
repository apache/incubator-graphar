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
	"strings"
)

// VertexInfo describes metadata for a vertex type.
type VertexInfo struct {
	Type           string           `yaml:"type"`
	ChunkSize      int64            `yaml:"chunk_size"`
	PropertyGroups []*PropertyGroup `yaml:"property_groups"`
	Labels         []string         `yaml:"labels,omitempty"`
	BaseURI        *url.URL         `yaml:"-"`
	Prefix         string           `yaml:"prefix"` // serialized form of BaseURI
	Version        VersionInfo      `yaml:"version"`

	pgroups propertyGroups `yaml:"-"`
}

func (v *VertexInfo) GetPrefix() *url.URL {
	return v.BaseURI
}

func (v *VertexInfo) HasProperty(propertyName string) bool {
	return v.pgroups.hasProperty(propertyName)
}

func (v *VertexInfo) IsPrimaryKey(propertyName string) (bool, error) {
	return v.pgroups.isPrimaryKey(propertyName)
}

func (v *VertexInfo) IsNullableKey(propertyName string) (bool, error) {
	return v.pgroups.isNullableKey(propertyName)
}

func (v *VertexInfo) GetPropertyType(propertyName string) (DataType, error) {
	return v.pgroups.propertyType(propertyName)
}

func (v *VertexInfo) GetPropertyCardinality(propertyName string) (Cardinality, error) {
	return v.pgroups.cardinality(propertyName)
}

func (v *VertexInfo) GetPropertyGroup(propertyName string) (*PropertyGroup, error) {
	return v.pgroups.propertyGroupFor(propertyName)
}

func (v *VertexInfo) GetPropertyGroupURI(pg *PropertyGroup) (*url.URL, error) {
	base := v.GetPrefix()
	if base == nil {
		return nil, fmt.Errorf("%w: vertex base uri is nil", ErrInvalid)
	}
	rel := pg.GetPrefix()
	if rel == nil {
		return nil, fmt.Errorf("%w: property group prefix is nil", ErrInvalid)
	}
	return base.ResolveReference(rel), nil
}

func (v *VertexInfo) GetPropertyGroupChunkURI(pg *PropertyGroup, chunkIndex int64) (*url.URL, error) {
	uri, err := v.GetPropertyGroupURI(pg)
	if err != nil {
		return nil, err
	}
	return uri.JoinPath(fmt.Sprintf("%s%d", chunkPrefix, chunkIndex)), nil
}

func (v *VertexInfo) GetVerticesNumFileURI() (*url.URL, error) {
	base := v.GetPrefix()
	if base == nil {
		return nil, fmt.Errorf("%w: vertex base uri is nil", ErrInvalid)
	}
	return base.JoinPath(fileVertexCount), nil
}

// Init builds internal indices after unmarshalling or manual construction.
func (v *VertexInfo) Init() error {
	if v.Prefix != "" && v.BaseURI == nil {
		p := v.Prefix
		if !strings.HasSuffix(p, "/") {
			p += "/"
		}
		u, err := url.Parse(p)
		if err != nil {
			return fmt.Errorf("%w: invalid vertex base uri %q: %v", ErrInvalid, v.Prefix, err)
		}
		v.BaseURI = u
	}
	v.pgroups = newPropertyGroups(v.PropertyGroups)
	for _, g := range v.PropertyGroups {
		if err := g.Init(); err != nil {
			return err
		}
	}
	return nil
}

// Validate checks that the VertexInfo is structurally valid.
func (v *VertexInfo) Validate() error {
	if v.Type == "" {
		return fmt.Errorf("%w: vertex type is empty", ErrMissingField)
	}
	if v.ChunkSize <= 0 {
		return fmt.Errorf("%w: vertex %s has non-positive chunk_size %d", ErrInvalid, v.Type, v.ChunkSize)
	}
	if v.BaseURI == nil && v.Prefix == "" {
		return fmt.Errorf("%w: vertex base uri/prefix is empty", ErrMissingField)
	}
	// property groups non-empty and unique property names
	seen := make(map[string]struct{})
	for _, g := range v.PropertyGroups {
		if err := g.Validate(); err != nil {
			return fmt.Errorf("%w: vertex %s property group: %v", ErrInvalid, v.Type, err)
		}
		for _, p := range g.Properties {
			if _, ok := seen[p.Name]; ok {
				return fmt.Errorf("%w: vertex %s has duplicated property %q", ErrDuplicate, v.Type, p.Name)
			}
			seen[p.Name] = struct{}{}
		}
	}
	return nil
}

// PropertyGroupFor returns the property group containing the given property.
func (v *VertexInfo) PropertyGroupFor(name string) (*PropertyGroup, error) {
	return v.pgroups.propertyGroupFor(name)
}
