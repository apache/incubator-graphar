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
	"cmp"
	"fmt"
	"maps"
	"slices"

	"github.com/apache/incubator-graphar/go/graphar/types"
)

// EdgeInfo is the immutable metadata for one edge type (src, edge, dst).
type EdgeInfo struct {
	srcType      string
	edgeType     string
	dstType      string
	chunkSize    int64
	srcChunkSize int64
	dstChunkSize int64
	directed     bool
	prefix       string
	adjLists     []AdjacentList
	adjByType    map[types.AdjListType]AdjacentList
	groups       *PropertyGroups
	version      types.InfoVersion
}

// EdgeInfoOption configures an EdgeInfo at construction time.
type EdgeInfoOption func(*edgeInfoConfig)

type edgeInfoConfig struct {
	prefix  string
	version types.InfoVersion
	groups  []PropertyGroup
}

// WithEdgePrefix sets the on-disk prefix for edge files. Defaults to
// "edge/<src>_<edge>_<dst>/".
func WithEdgePrefix(p string) EdgeInfoOption {
	return func(c *edgeInfoConfig) { c.prefix = p }
}

// WithEdgeVersion overrides the InfoVersion (default: gar/v1).
func WithEdgeVersion(v types.InfoVersion) EdgeInfoOption {
	return func(c *edgeInfoConfig) { c.version = v.Clone() }
}

// WithEdgePropertyGroups attaches property groups to the edge type. Empty
// (or omitted) is allowed: an edge type may carry only its adjacency lists.
//
// The slice is deep-cloned: callers may safely mutate either the outer slice
// or each PropertyGroup's Properties after this call without affecting the
// resulting EdgeInfo.
func WithEdgePropertyGroups(groups []PropertyGroup) EdgeInfoOption {
	return func(c *edgeInfoConfig) {
		c.groups = clonePropertyGroups(groups)
	}
}

// NewEdgeInfo constructs an EdgeInfo and validates it. All three type names
// and all three chunk sizes are mandatory. adjLists must be non-empty and
// every entry must have a known AdjListType.
func NewEdgeInfo(
	src, edge, dst string,
	chunkSize, srcChunkSize, dstChunkSize int64,
	directed bool,
	adjLists []AdjacentList,
	opts ...EdgeInfoOption,
) (*EdgeInfo, error) {
	e, err := newEdgeInfoUnvalidated(src, edge, dst, chunkSize, srcChunkSize, dstChunkSize, directed, adjLists, opts...)
	if err != nil {
		return nil, err
	}
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return e, nil
}

// newEdgeInfoUnvalidated builds an EdgeInfo without validating (the liberal
// load path): adj lists are indexed (duplicate types still error) but not
// validated, and lists may be empty. Call Validate to enforce the full contract.
func newEdgeInfoUnvalidated(
	src, edge, dst string,
	chunkSize, srcChunkSize, dstChunkSize int64,
	directed bool,
	adjLists []AdjacentList,
	opts ...EdgeInfoOption,
) (*EdgeInfo, error) {
	cfg := edgeInfoConfig{version: types.NewInfoVersion(types.DefaultVersion)}
	for _, opt := range opts {
		opt(&cfg)
	}
	adjCp := slices.Clone(adjLists)
	adjByType := make(map[types.AdjListType]AdjacentList, len(adjCp))
	for i, a := range adjCp {
		if _, dup := adjByType[a.Type]; dup {
			return nil, newValidationError(ErrInvalidEdgeInfo,
				fmt.Sprintf("adj_lists[%d].type", i),
				"duplicate adj_list type %q", a.Type.String())
		}
		adjByType[a.Type] = a
	}
	prefix := cfg.prefix
	if prefix == "" && src != "" && edge != "" && dst != "" {
		prefix = "edge/" + src + "_" + edge + "_" + dst + "/"
	}
	var groups *PropertyGroups
	if len(cfg.groups) > 0 {
		pg, err := buildPropertyGroups(cfg.groups)
		if err != nil {
			return nil, fmt.Errorf("edge_info(%s_%s_%s): %w", src, edge, dst, err)
		}
		groups = pg
	}
	return &EdgeInfo{
		srcType:      src,
		edgeType:     edge,
		dstType:      dst,
		chunkSize:    chunkSize,
		srcChunkSize: srcChunkSize,
		dstChunkSize: dstChunkSize,
		directed:     directed,
		prefix:       prefix,
		adjLists:     adjCp,
		adjByType:    adjByType,
		groups:       groups,
		version:      cfg.version,
	}, nil
}

// SrcType returns the source vertex type.
func (e *EdgeInfo) SrcType() string { return e.srcType }

// EdgeType returns the edge type name.
func (e *EdgeInfo) EdgeType() string { return e.edgeType }

// DstType returns the destination vertex type.
func (e *EdgeInfo) DstType() string { return e.dstType }

// Triplet returns the (src, edge, dst) tuple.
func (e *EdgeInfo) Triplet() (string, string, string) { return e.srcType, e.edgeType, e.dstType }

// ChunkSize returns the edge-side chunk size.
func (e *EdgeInfo) ChunkSize() int64 { return e.chunkSize }

// SrcChunkSize returns the source-side chunk size.
func (e *EdgeInfo) SrcChunkSize() int64 { return e.srcChunkSize }

// DstChunkSize returns the destination-side chunk size.
func (e *EdgeInfo) DstChunkSize() int64 { return e.dstChunkSize }

// Directed reports whether the edge is directed.
func (e *EdgeInfo) Directed() bool { return e.directed }

// Prefix returns the on-disk prefix.
func (e *EdgeInfo) Prefix() string { return e.prefix }

// Version returns the InfoVersion.
func (e *EdgeInfo) Version() types.InfoVersion { return e.version.Clone() }

// AdjLists returns the layouts sorted by type for determinism.
func (e *EdgeInfo) AdjLists() []AdjacentList {
	out := slices.Clone(e.adjLists)
	slices.SortStableFunc(out, func(a, b AdjacentList) int {
		return cmp.Compare(a.Type, b.Type)
	})
	return out
}

// AdjListTypes returns the set of available layouts in sorted order.
func (e *EdgeInfo) AdjListTypes() []types.AdjListType {
	return slices.Sorted(maps.Keys(e.adjByType))
}

// AdjList returns the adjacency list for the given layout, or false when
// absent.
func (e *EdgeInfo) AdjList(t types.AdjListType) (AdjacentList, bool) {
	a, ok := e.adjByType[t]
	return a, ok
}

// PropertyGroups returns the property group index, or nil when the edge has
// no properties.
func (e *EdgeInfo) PropertyGroups() *PropertyGroups { return e.groups }

// HasProperty reports whether the named property exists on this edge type.
func (e *EdgeInfo) HasProperty(name string) bool {
	return e.groups != nil && e.groups.Has(name)
}

// Validate enforces cross-field invariants and edge-specific rules:
//   - non-empty src/edge/dst
//   - chunk sizes > 0
//   - at least one adjacency list
//   - every property group's properties are single-valued
func (e *EdgeInfo) Validate() error {
	if e.srcType == "" {
		return newValidationError(ErrInvalidEdgeInfo, "src_type", "must not be empty")
	}
	if e.edgeType == "" {
		return newValidationError(ErrInvalidEdgeInfo, "edge_type", "must not be empty")
	}
	if e.dstType == "" {
		return newValidationError(ErrInvalidEdgeInfo, "dst_type", "must not be empty")
	}
	if e.chunkSize <= 0 {
		return newValidationError(ErrInvalidEdgeInfo, "chunk_size", "must be > 0")
	}
	if err := e.version.Validate(); err != nil {
		return newValidationError(ErrInvalidEdgeInfo, "version", "%s", err)
	}
	if e.srcChunkSize <= 0 {
		return newValidationError(ErrInvalidEdgeInfo, "src_chunk_size", "must be > 0")
	}
	if e.dstChunkSize <= 0 {
		return newValidationError(ErrInvalidEdgeInfo, "dst_chunk_size", "must be > 0")
	}
	if len(e.adjLists) == 0 {
		return newValidationError(ErrInvalidEdgeInfo, "adj_lists",
			"at least one adjacency list is required")
	}
	for i, a := range e.adjLists {
		if err := a.Validate(); err != nil {
			return fmt.Errorf("adj_lists[%d]: %w", i, err)
		}
	}
	if e.groups != nil {
		if err := e.groups.Validate(); err != nil {
			return err
		}
		if err := validateUserDefinedTypes(e.groups, e.version, ErrInvalidEdgeInfo); err != nil {
			return err
		}
		for _, name := range e.groups.Names() {
			p, _, _ := e.groups.Property(name)
			if p.Cardinality != types.CardinalitySingle {
				return newValidationError(ErrInvalidEdgeInfo,
					"property_groups.properties.cardinality",
					"edge property %q must have cardinality single, got %s",
					p.Name, p.Cardinality)
			}
		}
	}
	return nil
}
