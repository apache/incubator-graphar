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

// AdjacentList describes one adjacency list layout of an edge type.
type AdjacentList struct {
	Type     AdjListType `yaml:"type"`
	FileType FileType    `yaml:"file_type"`
	Prefix   string      `yaml:"prefix"`
	Ordered  bool        `yaml:"ordered,omitempty"`
	Aligned  string      `yaml:"aligned_by,omitempty"` // "src" or "dst"

	baseURI *url.URL `yaml:"-"`
}

func (a *AdjacentList) Validate() error {
	if a.FileType == "" {
		return fmt.Errorf("%w: adjacent list file type is empty", ErrMissingField)
	}
	if !a.FileType.IsValid() {
		return fmt.Errorf("%w: invalid file type %q for adjacent list %q", ErrInvalid, a.FileType, a.Type)
	}
	return nil
}

func (a *AdjacentList) GetPrefix() *url.URL {
	return a.baseURI
}

// EdgeInfo describes metadata for an edge type.
type EdgeInfo struct {
	SrcType        string           `yaml:"src_type"`
	EdgeType       string           `yaml:"edge_type"`
	DstType        string           `yaml:"dst_type"`
	ChunkSize      int64            `yaml:"chunk_size"`
	SrcChunkSize   int64            `yaml:"src_chunk_size"`
	DstChunkSize   int64            `yaml:"dst_chunk_size"`
	Directed       bool             `yaml:"directed"`
	BaseURI        *url.URL         `yaml:"-"`
	Prefix         string           `yaml:"prefix"`
	Adjacent       []*AdjacentList  `yaml:"adj_lists"`
	PropertyGroups []*PropertyGroup `yaml:"property_groups"`
	Version        VersionInfo      `yaml:"version"`

	pgroups propertyGroups                `yaml:"-"`
	adjMap  map[AdjListType]*AdjacentList `yaml:"-"`
}

func (e *EdgeInfo) IsDirected() bool {
	return e.Directed
}

func (e *EdgeInfo) GetPrefix() *url.URL {
	return e.BaseURI
}

func (e *EdgeInfo) HasAdjListType(adjListType AdjListType) bool {
	_, ok := e.adjMap[adjListType]
	return ok
}

func (e *EdgeInfo) HasProperty(propertyName string) bool {
	return e.pgroups.hasProperty(propertyName)
}

func (e *EdgeInfo) IsPrimaryKey(propertyName string) (bool, error) {
	return e.pgroups.isPrimaryKey(propertyName)
}

func (e *EdgeInfo) IsNullableKey(propertyName string) (bool, error) {
	return e.pgroups.isNullableKey(propertyName)
}

func (e *EdgeInfo) GetPropertyType(propertyName string) (DataType, error) {
	return e.pgroups.propertyType(propertyName)
}

func (e *EdgeInfo) GetPropertyGroup(propertyName string) (*PropertyGroup, error) {
	return e.pgroups.propertyGroupFor(propertyName)
}

func (e *EdgeInfo) GetAdjacentList(adjListType AdjListType) (*AdjacentList, error) {
	adj, ok := e.adjMap[adjListType]
	if !ok {
		return nil, fmt.Errorf("%w: adjacent list type %s not found", ErrNotFound, adjListType)
	}
	return adj, nil
}

func (e *EdgeInfo) GetAdjacentListURI(adjListType AdjListType) (*url.URL, error) {
	adj, err := e.GetAdjacentList(adjListType)
	if err != nil {
		return nil, err
	}
	base := e.GetPrefix()
	if base == nil {
		return nil, fmt.Errorf("%w: edge base uri is nil", ErrInvalid)
	}
	rel := adj.GetPrefix()
	if rel == nil {
		return nil, fmt.Errorf("%w: adjacent list prefix is nil", ErrInvalid)
	}
	return base.ResolveReference(rel).JoinPath(pathAdjList), nil
}

func (e *EdgeInfo) GetAdjacentListChunkURI(adjListType AdjListType, vertexChunkIndex int64) (*url.URL, error) {
	uri, err := e.GetAdjacentListURI(adjListType)
	if err != nil {
		return nil, err
	}
	return uri.JoinPath(fmt.Sprintf("%s%d", chunkPrefix, vertexChunkIndex)), nil
}

func (e *EdgeInfo) GetOffsetURI(adjListType AdjListType) (*url.URL, error) {
	uri, err := e.GetAdjacentListURI(adjListType)
	if err != nil {
		return nil, err
	}
	return uri.JoinPath(pathOffset), nil
}

func (e *EdgeInfo) GetOffsetChunkURI(adjListType AdjListType, vertexChunkIndex int64) (*url.URL, error) {
	uri, err := e.GetOffsetURI(adjListType)
	if err != nil {
		return nil, err
	}
	return uri.JoinPath(fmt.Sprintf("%s%d", chunkPrefix, vertexChunkIndex)), nil
}

func (e *EdgeInfo) GetVerticesNumFileURI(adjListType AdjListType) (*url.URL, error) {
	uri, err := e.GetAdjacentListURI(adjListType)
	if err != nil {
		return nil, err
	}
	return uri.JoinPath(fileVertexCount), nil
}

func (e *EdgeInfo) GetEdgesNumFileURI(adjListType AdjListType, vertexChunkIndex int64) (*url.URL, error) {
	uri, err := e.GetAdjacentListURI(adjListType)
	if err != nil {
		return nil, err
	}
	return uri.JoinPath(fmt.Sprintf("%s%d", fileEdgeCountPrefix, vertexChunkIndex)), nil
}

func (e *EdgeInfo) GetPropertyGroupURI(pg *PropertyGroup) (*url.URL, error) {
	base := e.GetPrefix()
	if base == nil {
		return nil, fmt.Errorf("%w: edge base uri is nil", ErrInvalid)
	}
	rel := pg.GetPrefix()
	if rel == nil {
		return nil, fmt.Errorf("%w: property group prefix is nil", ErrInvalid)
	}
	return base.ResolveReference(rel), nil
}

func (e *EdgeInfo) GetPropertyGroupChunkURI(pg *PropertyGroup, chunkIndex int64) (*url.URL, error) {
	uri, err := e.GetPropertyGroupURI(pg)
	if err != nil {
		return nil, err
	}
	return uri.JoinPath(fmt.Sprintf("%s%d", chunkPrefix, chunkIndex)), nil
}

// concat builds the concatenated edge label, mirroring Java/C++.
func concat(srcLabel, edgeLabel, dstLabel string) string {
	return srcLabel + regularSeparator + edgeLabel + regularSeparator + dstLabel
}

// Key returns the concat key of this edge info.
func (e *EdgeInfo) Key() string {
	return concat(e.SrcType, e.EdgeType, e.DstType)
}

// Init builds internal indices after unmarshalling or manual construction.
func (e *EdgeInfo) Init() error {
	if e.Prefix != "" && e.BaseURI == nil {
		p := e.Prefix
		if !strings.HasSuffix(p, "/") {
			p += "/"
		}
		u, err := url.Parse(p)
		if err != nil {
			return fmt.Errorf("%w: invalid edge base uri %q: %v", ErrInvalid, e.Prefix, err)
		}
		e.BaseURI = u
	}
	e.adjMap = make(map[AdjListType]*AdjacentList)
	for i := range e.Adjacent {
		adj := e.Adjacent[i]
		if adj.Prefix != "" && adj.baseURI == nil {
			u, err := url.Parse(adj.Prefix)
			if err != nil {
				return fmt.Errorf("%w: invalid adjacent list prefix %q: %v", ErrInvalid, adj.Prefix, err)
			}
			adj.baseURI = u
		}
		if adj.Type == "" {
			switch {
			case adj.Ordered && adj.Aligned == alignedBySrc:
				adj.Type = AdjListOrderedBySource
			case adj.Ordered && adj.Aligned == alignedByDst:
				adj.Type = AdjListOrderedByDest
			case !adj.Ordered && adj.Aligned == alignedBySrc:
				adj.Type = AdjListUnorderedBySource
			case !adj.Ordered && adj.Aligned == alignedByDst:
				adj.Type = AdjListUnorderedByDest
			}
		}
		if adj.Type != "" {
			e.adjMap[adj.Type] = adj
		}
	}
	for _, g := range e.PropertyGroups {
		if err := g.Init(); err != nil {
			return err
		}
	}
	e.pgroups = newPropertyGroups(e.PropertyGroups)
	return nil
}

// Validate checks that the EdgeInfo is structurally valid.
func (e *EdgeInfo) Validate() error {
	if e.SrcType == "" || e.EdgeType == "" || e.DstType == "" {
		return fmt.Errorf("%w: edge src/edge/dst type must be non-empty", ErrMissingField)
	}
	if e.ChunkSize <= 0 || e.SrcChunkSize <= 0 || e.DstChunkSize <= 0 {
		return fmt.Errorf("%w: edge %s has non-positive chunk sizes", ErrInvalid, e.Key())
	}
	if e.BaseURI == nil && e.Prefix == "" {
		return fmt.Errorf("%w: edge base uri/prefix is empty", ErrMissingField)
	}
	if len(e.Adjacent) == 0 {
		return fmt.Errorf("%w: edge %s has no adjacent list definitions", ErrMissingField, e.Key())
	}
	for _, a := range e.Adjacent {
		if a.Type == "" {
			return fmt.Errorf("%w: edge %s has adjacent list with empty type", ErrMissingField, e.Key())
		}
		if !a.Type.IsValid() {
			return fmt.Errorf("%w: invalid adjacent list type %q for edge %s", ErrInvalid, a.Type, e.Key())
		}
		if err := a.Validate(); err != nil {
			return fmt.Errorf("%w: edge %s adjacent list %q: %v", ErrInvalid, e.Key(), a.Type, err)
		}
	}

	// property groups non-empty and unique, with SINGLE cardinality (Java rule)
	seen := make(map[string]struct{})
	for _, g := range e.PropertyGroups {
		if err := g.Validate(); err != nil {
			return fmt.Errorf("%w: edge %s property group: %v", ErrInvalid, e.Key(), err)
		}
		for _, p := range g.Properties {
			if p.Cardinality != "" && p.Cardinality != CardinalitySingle {
				return fmt.Errorf("%w: edge %s property %s must have single cardinality", ErrInvalid, e.Key(), p.Name)
			}
			if _, ok := seen[p.Name]; ok {
				return fmt.Errorf("%w: edge %s has duplicated property %q", ErrDuplicate, e.Key(), p.Name)
			}
			seen[p.Name] = struct{}{}
		}
	}
	return nil
}
