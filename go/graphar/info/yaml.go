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
	"strings"

	"github.com/apache/incubator-graphar/go/graphar/types"
)

// The DTO types in this file are the only place where YAML field names live.
// The exported Info types do not carry yaml tags so on-disk evolution does
// not leak into the API.

type propertyYAML struct {
	Name     string `yaml:"name"`
	DataType string `yaml:"data_type"`
	// IsPrimary has no omitempty: absent and false are not interchangeable on
	// disk, so the field is always emitted.
	IsPrimary bool `yaml:"is_primary"`
	// IsNullable is tri-state on the wire (present-true / present-false /
	// absent). Absent means true for non-primary properties, so it is a *bool
	// and the default is applied in toDomain when nil.
	IsNullable  *bool  `yaml:"is_nullable,omitempty"`
	Cardinality string `yaml:"cardinality,omitempty"`
}

type propertyGroupYAML struct {
	Prefix     string         `yaml:"prefix,omitempty"`
	FileType   string         `yaml:"file_type"`
	Properties []propertyYAML `yaml:"properties"`
}

type vertexInfoYAML struct {
	Type string `yaml:"type,omitempty"`
	// Label is an alias for Type accepted on read for older fixtures that use
	// `label`. Read-only; write always uses `type`.
	Label          string              `yaml:"label,omitempty"`
	ChunkSize      int64               `yaml:"chunk_size"`
	Prefix         string              `yaml:"prefix,omitempty"`
	Labels         []string            `yaml:"labels,omitempty"`
	PropertyGroups []propertyGroupYAML `yaml:"property_groups"`
	Version        string              `yaml:"version"`
}

type adjacentListYAML struct {
	// Type is the modern explicit form ("ordered_by_source", ...). When
	// missing, Ordered + AlignedBy are used to derive it.
	Type      string `yaml:"type,omitempty"`
	Ordered   *bool  `yaml:"ordered,omitempty"`
	AlignedBy string `yaml:"aligned_by,omitempty"`
	Prefix    string `yaml:"prefix,omitempty"`
	FileType  string `yaml:"file_type"`
	// IgnoredPropertyGroups names the legacy per-adj-list property_groups field
	// (ldbc, nebula fixtures) so it is dropped explicitly rather than silently.
	// It is not propagated to the domain model and never emitted.
	IgnoredPropertyGroups []propertyGroupYAML `yaml:"property_groups,omitempty"`
}

type edgeInfoYAML struct {
	SrcType  string `yaml:"src_type,omitempty"`
	EdgeType string `yaml:"edge_type,omitempty"`
	DstType  string `yaml:"dst_type,omitempty"`
	// SrcLabel/EdgeLabel/DstLabel are legacy `*_label` spellings of the triplet
	// fields, accepted on read only.
	SrcLabel       string              `yaml:"src_label,omitempty"`
	EdgeLabel      string              `yaml:"edge_label,omitempty"`
	DstLabel       string              `yaml:"dst_label,omitempty"`
	ChunkSize      int64               `yaml:"chunk_size"`
	SrcChunkSize   int64               `yaml:"src_chunk_size"`
	DstChunkSize   int64               `yaml:"dst_chunk_size"`
	Directed       bool                `yaml:"directed"`
	Prefix         string              `yaml:"prefix,omitempty"`
	AdjLists       []adjacentListYAML  `yaml:"adj_lists"`
	PropertyGroups []propertyGroupYAML `yaml:"property_groups,omitempty"`
	Version        string              `yaml:"version"`
}

type graphInfoYAML struct {
	Name      string          `yaml:"name"`
	Prefix    string          `yaml:"prefix,omitempty"`
	Vertices  []string        `yaml:"vertices,omitempty"`
	Edges     []string        `yaml:"edges,omitempty"`
	Labels    []string        `yaml:"labels,omitempty"`
	Version   string          `yaml:"version"`
	ExtraInfo []extraInfoYAML `yaml:"extra_info,omitempty"`
}

// extraInfoYAML is one entry of the graph document's extra_info sequence,
// which is stored on disk as a list of {key, value} objects, not a plain map.
type extraInfoYAML struct {
	Key   string `yaml:"key"`
	Value string `yaml:"value"`
}

// --- DTO → domain ---

func (y propertyYAML) toDomain(field string) (Property, error) {
	dt, err := types.ParseDataType(y.DataType)
	if err != nil {
		// Liberal read: an unrecognised name becomes a user-defined type, checked
		// against the version's declared list by Validate. A name carrying < or >
		// is a malformed list spelling, not a custom type, so keep the error.
		name := strings.TrimSpace(y.DataType)
		if name == "" || strings.ContainsAny(name, "<>") {
			return Property{}, fmt.Errorf("%s.data_type: %w", field, err)
		}
		dt = types.UserDefined(name)
	}
	card := types.CardinalitySingle
	if y.Cardinality != "" {
		card, err = types.ParseCardinality(y.Cardinality)
		if err != nil {
			return Property{}, fmt.Errorf("%s.cardinality: %w", field, err)
		}
	}
	// Absent is_nullable defaults to !is_primary; an explicit value is kept
	// verbatim (even a contradictory primary+nullable, which Validate flags).
	isNullable := !y.IsPrimary
	if y.IsNullable != nil {
		isNullable = *y.IsNullable
	}
	return Property{
		Name:        y.Name,
		Type:        dt,
		IsPrimary:   y.IsPrimary,
		IsNullable:  isNullable,
		Cardinality: card,
	}, nil
}

func (y propertyGroupYAML) toDomain(field string) (PropertyGroup, error) {
	ft, err := types.ParseFileType(y.FileType)
	if err != nil {
		return PropertyGroup{}, fmt.Errorf("%s.file_type: %w", field, err)
	}
	props := make([]Property, 0, len(y.Properties))
	for i, py := range y.Properties {
		p, err := py.toDomain(fmt.Sprintf("%s.properties[%d]", field, i))
		if err != nil {
			return PropertyGroup{}, err
		}
		props = append(props, p)
	}
	return PropertyGroup{
		Properties: props,
		FileType:   ft,
		Prefix:     y.Prefix,
	}, nil
}

func (y adjacentListYAML) toDomain(field string) (AdjacentList, error) {
	ft, err := types.ParseFileType(y.FileType)
	if err != nil {
		return AdjacentList{}, fmt.Errorf("%s.file_type: %w", field, err)
	}
	var t types.AdjListType
	switch {
	case y.Type != "":
		t, err = types.ParseAdjListType(y.Type)
		if err != nil {
			return AdjacentList{}, fmt.Errorf("%s.type: %w", field, err)
		}
	case y.Ordered != nil && y.AlignedBy != "":
		t, err = types.AdjListTypeFromOrderedAligned(*y.Ordered, y.AlignedBy)
		if err != nil {
			return AdjacentList{}, fmt.Errorf("%s: %w", field, err)
		}
	default:
		return AdjacentList{}, newValidationError(ErrInvalidEdgeInfo, field,
			"adj_list must specify either type or (ordered + aligned_by)")
	}
	return AdjacentList{Type: t, FileType: ft, Prefix: y.Prefix}, nil
}

func vertexInfoFromYAML(y vertexInfoYAML) (*VertexInfo, error) {
	groups := make([]PropertyGroup, 0, len(y.PropertyGroups))
	for i, gy := range y.PropertyGroups {
		g, err := gy.toDomain(fmt.Sprintf("vertex_info.property_groups[%d]", i))
		if err != nil {
			return nil, err
		}
		groups = append(groups, g)
	}
	// prefer type, fall back to the legacy label field
	typeName := y.Type
	if typeName == "" {
		typeName = y.Label
	}
	// Minimal completeness: a vertex document must name its type, otherwise the
	// input was some other (mis-filed) document decoded into a zero DTO.
	if typeName == "" {
		return nil, fmt.Errorf("%w: vertex info document has no type", ErrInvalidYAML)
	}
	opts := []VertexInfoOption{}
	if y.Prefix != "" {
		opts = append(opts, WithVertexPrefix(y.Prefix))
	}
	if len(y.Labels) > 0 {
		opts = append(opts, WithVertexLabels(y.Labels...))
	}
	if y.Version != "" {
		ver, err := types.ParseInfoVersion(y.Version)
		if err != nil {
			return nil, fmt.Errorf("vertex_info.version: %w", err)
		}
		opts = append(opts, WithVertexVersion(ver))
	}
	return newVertexInfoUnvalidated(typeName, y.ChunkSize, groups, opts...)
}

func edgeInfoFromYAML(y edgeInfoYAML) (*EdgeInfo, error) {
	adjLists := make([]AdjacentList, 0, len(y.AdjLists))
	for i, ay := range y.AdjLists {
		a, err := ay.toDomain(fmt.Sprintf("edge_info.adj_lists[%d]", i))
		if err != nil {
			return nil, err
		}
		adjLists = append(adjLists, a)
	}
	// Resolve the triplet, preferring *_type and falling back to legacy *_label.
	src := y.SrcType
	if src == "" {
		src = y.SrcLabel
	}
	edge := y.EdgeType
	if edge == "" {
		edge = y.EdgeLabel
	}
	dst := y.DstType
	if dst == "" {
		dst = y.DstLabel
	}
	// Minimal completeness: an edge document must name its full triplet, else
	// the input was some other (mis-filed) document decoded into a zero DTO.
	if src == "" || edge == "" || dst == "" {
		return nil, fmt.Errorf("%w: edge info document is missing src/edge/dst type", ErrInvalidYAML)
	}
	opts := []EdgeInfoOption{}
	if y.Prefix != "" {
		opts = append(opts, WithEdgePrefix(y.Prefix))
	}
	if y.Version != "" {
		ver, err := types.ParseInfoVersion(y.Version)
		if err != nil {
			return nil, fmt.Errorf("edge_info.version: %w", err)
		}
		opts = append(opts, WithEdgeVersion(ver))
	}
	if len(y.PropertyGroups) > 0 {
		groups := make([]PropertyGroup, 0, len(y.PropertyGroups))
		for i, gy := range y.PropertyGroups {
			g, err := gy.toDomain(fmt.Sprintf("edge_info.property_groups[%d]", i))
			if err != nil {
				return nil, err
			}
			groups = append(groups, g)
		}
		opts = append(opts, WithEdgePropertyGroups(groups))
	}
	return newEdgeInfoUnvalidated(
		src, edge, dst,
		y.ChunkSize, y.SrcChunkSize, y.DstChunkSize,
		y.Directed,
		adjLists,
		opts...,
	)
}

// --- domain → DTO ---

func propertyToYAML(p Property) propertyYAML {
	y := propertyYAML{
		Name:      p.Name,
		DataType:  p.Type.String(),
		IsPrimary: p.IsPrimary,
	}
	// Emit is_nullable only when it diverges from the default (!IsPrimary), so
	// fixtures that omit it round-trip unchanged.
	defaultNullable := !p.IsPrimary
	if p.IsNullable != defaultNullable {
		v := p.IsNullable
		y.IsNullable = &v
	}
	if p.Cardinality != types.CardinalitySingle {
		y.Cardinality = p.Cardinality.String()
	}
	return y
}

func propertyGroupToYAML(g PropertyGroup) propertyGroupYAML {
	props := make([]propertyYAML, len(g.Properties))
	for i, p := range g.Properties {
		props[i] = propertyToYAML(p)
	}
	return propertyGroupYAML{
		Prefix:     g.Prefix,
		FileType:   g.FileType.String(),
		Properties: props,
	}
}

func adjacentListToYAML(a AdjacentList) adjacentListYAML {
	// Emit the (ordered, aligned_by) form: it is the only adj_list spelling the
	// readers accept. A `type` key is ignored on read, so emitting it instead
	// would be silently mis-decoded to unordered_by_dest.
	ordered := a.Type.IsOrdered()
	side, _ := a.Type.AlignedBy()
	return adjacentListYAML{
		Ordered:   &ordered,
		AlignedBy: side,
		Prefix:    a.Prefix,
		FileType:  a.FileType.String(),
	}
}

func vertexInfoToYAML(v *VertexInfo) vertexInfoYAML {
	var src []PropertyGroup
	if pg := v.PropertyGroups(); pg != nil {
		src = pg.groups // in-package read; DTO conversion copies out
	}
	groups := make([]propertyGroupYAML, 0, len(src))
	for _, g := range src {
		groups = append(groups, propertyGroupToYAML(g))
	}
	return vertexInfoYAML{
		Type:           v.Type(),
		ChunkSize:      v.ChunkSize(),
		Prefix:         v.Prefix(),
		Labels:         v.Labels(),
		PropertyGroups: groups,
		Version:        v.Version().String(),
	}
}

func edgeInfoToYAML(e *EdgeInfo) edgeInfoYAML {
	adj := e.AdjLists()
	adjLists := make([]adjacentListYAML, 0, len(adj))
	for _, a := range adj {
		adjLists = append(adjLists, adjacentListToYAML(a))
	}
	var groups []propertyGroupYAML
	if pg := e.PropertyGroups(); pg != nil {
		src := pg.groups // in-package read; DTO conversion copies out
		groups = make([]propertyGroupYAML, 0, len(src))
		for _, g := range src {
			groups = append(groups, propertyGroupToYAML(g))
		}
	}
	src, edge, dst := e.Triplet()
	return edgeInfoYAML{
		SrcType:        src,
		EdgeType:       edge,
		DstType:        dst,
		ChunkSize:      e.ChunkSize(),
		SrcChunkSize:   e.SrcChunkSize(),
		DstChunkSize:   e.DstChunkSize(),
		Directed:       e.Directed(),
		Prefix:         e.Prefix(),
		AdjLists:       adjLists,
		PropertyGroups: groups,
		Version:        e.Version().String(),
	}
}
