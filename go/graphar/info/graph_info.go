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

// EdgeTriplet uniquely identifies an edge type by (src, edge, dst).
type EdgeTriplet struct {
	Src, Edge, Dst string
}

// String returns the triplet spelling "src_edge_dst".
func (t EdgeTriplet) String() string {
	return t.Src + "_" + t.Edge + "_" + t.Dst
}

// compare orders triplets by src, then edge, then dst.
func (t EdgeTriplet) compare(o EdgeTriplet) int {
	if c := cmp.Compare(t.Src, o.Src); c != 0 {
		return c
	}
	if c := cmp.Compare(t.Edge, o.Edge); c != 0 {
		return c
	}
	return cmp.Compare(t.Dst, o.Dst)
}

// GraphInfo is the top-level graph metadata aggregate.
type GraphInfo struct {
	name      string
	prefix    string
	version   types.InfoVersion
	vertices  map[string]*VertexInfo
	edges     map[EdgeTriplet]*EdgeInfo
	labels    []string
	extraInfo map[string]string
	// File references from the loaded graph yaml, keyed by vertex type / edge
	// triplet. Empty for a graph built programmatically; MarshalGraphInfo then
	// derives default names.
	vertexFileRefs map[string]string
	edgeFileRefs   map[EdgeTriplet]string
}

// GraphInfoOption configures a GraphInfo at construction time.
type GraphInfoOption func(*graphInfoConfig)

type graphInfoConfig struct {
	prefix         string
	version        types.InfoVersion
	vertices       []*VertexInfo
	edges          []*EdgeInfo
	labels         []string
	extraInfo      map[string]string
	vertexFileRefs map[string]string
	edgeFileRefs   map[EdgeTriplet]string
}

// WithGraphPrefix sets the on-disk root prefix.
func WithGraphPrefix(p string) GraphInfoOption {
	return func(c *graphInfoConfig) { c.prefix = p }
}

// WithGraphVersion overrides the InfoVersion (default: gar/v1).
func WithGraphVersion(v types.InfoVersion) GraphInfoOption {
	return func(c *graphInfoConfig) { c.version = v.Clone() }
}

// WithVertices attaches vertex infos. Duplicate type names are rejected.
func WithVertices(vs ...*VertexInfo) GraphInfoOption {
	return func(c *graphInfoConfig) { c.vertices = append(c.vertices, vs...) }
}

// WithEdges attaches edge infos. Duplicate (src,edge,dst) triplets are
// rejected.
func WithEdges(es ...*EdgeInfo) GraphInfoOption {
	return func(c *graphInfoConfig) { c.edges = append(c.edges, es...) }
}

// WithGraphLabels attaches graph-level labels (distinct from vertex labels).
func WithGraphLabels(labels ...string) GraphInfoOption {
	return func(c *graphInfoConfig) {
		c.labels = slices.Clone(labels)
	}
}

// WithGraphExtraInfo attaches free-form key/value metadata carried on the
// graph document (the on-disk extra_info field). The map is copied.
func WithGraphExtraInfo(extra map[string]string) GraphInfoOption {
	return func(c *graphInfoConfig) {
		c.extraInfo = maps.Clone(extra)
	}
}

// withVertexFileRefs records the vertex file references from a loaded graph
// yaml so MarshalGraphInfo can reproduce them. Used by the load path only.
func withVertexFileRefs(m map[string]string) GraphInfoOption {
	return func(c *graphInfoConfig) { c.vertexFileRefs = m }
}

// withEdgeFileRefs records the edge file references from a loaded graph yaml.
func withEdgeFileRefs(m map[EdgeTriplet]string) GraphInfoOption {
	return func(c *graphInfoConfig) { c.edgeFileRefs = m }
}

// NewGraphInfo constructs a GraphInfo and validates it.
func NewGraphInfo(name string, opts ...GraphInfoOption) (*GraphInfo, error) {
	g, err := newGraphInfoUnvalidated(name, opts...)
	if err != nil {
		return nil, err
	}
	if err := g.Validate(); err != nil {
		return nil, err
	}
	return g, nil
}

// newGraphInfoUnvalidated builds a GraphInfo without validating (the liberal
// load path): duplicate vertex types / edge triplets still error, but
// cross-references and child validity are not checked. Call Validate to enforce
// the full contract.
func newGraphInfoUnvalidated(name string, opts ...GraphInfoOption) (*GraphInfo, error) {
	cfg := graphInfoConfig{version: types.NewInfoVersion(types.DefaultVersion)}
	for _, opt := range opts {
		opt(&cfg)
	}
	g := &GraphInfo{
		name:           name,
		prefix:         cfg.prefix,
		version:        cfg.version,
		vertices:       make(map[string]*VertexInfo, len(cfg.vertices)),
		edges:          make(map[EdgeTriplet]*EdgeInfo, len(cfg.edges)),
		labels:         cfg.labels,
		extraInfo:      cfg.extraInfo,
		vertexFileRefs: cfg.vertexFileRefs,
		edgeFileRefs:   cfg.edgeFileRefs,
	}
	for _, v := range cfg.vertices {
		if v == nil {
			return nil, newValidationError(ErrInvalidGraphInfo, "vertices", "nil vertex info")
		}
		if _, dup := g.vertices[v.Type()]; dup {
			return nil, newValidationError(ErrInvalidGraphInfo, "vertices",
				"duplicate vertex type %q", v.Type())
		}
		g.vertices[v.Type()] = v
	}
	for _, e := range cfg.edges {
		if e == nil {
			return nil, newValidationError(ErrInvalidGraphInfo, "edges", "nil edge info")
		}
		src, edge, dst := e.Triplet()
		key := EdgeTriplet{Src: src, Edge: edge, Dst: dst}
		if _, dup := g.edges[key]; dup {
			return nil, newValidationError(ErrInvalidGraphInfo, "edges",
				"duplicate edge triplet %q", key.String())
		}
		g.edges[key] = e
	}
	return g, nil
}

// Name returns the graph name.
func (g *GraphInfo) Name() string { return g.name }

// Prefix returns the on-disk root prefix.
func (g *GraphInfo) Prefix() string { return g.prefix }

// Version returns the InfoVersion.
func (g *GraphInfo) Version() types.InfoVersion { return g.version.Clone() }

// Labels returns a copy of the graph-level labels.
func (g *GraphInfo) Labels() []string { return slices.Clone(g.labels) }

// ExtraInfo returns a copy of the free-form key/value metadata carried on the
// graph document.
func (g *GraphInfo) ExtraInfo() map[string]string {
	return maps.Clone(g.extraInfo)
}

// VertexFileName returns the file reference recorded for the vertex type when
// the graph was loaded, and false when none was recorded (e.g. a graph built
// programmatically, which derives its names on marshal).
func (g *GraphInfo) VertexFileName(typ string) (string, bool) {
	n, ok := g.vertexFileRefs[typ]
	return n, ok
}

// EdgeFileName returns the file reference recorded for the edge triplet when
// the graph was loaded, and false when none was recorded.
func (g *GraphInfo) EdgeFileName(src, edge, dst string) (string, bool) {
	n, ok := g.edgeFileRefs[EdgeTriplet{Src: src, Edge: edge, Dst: dst}]
	return n, ok
}

// resolveVertexFile picks the file name for a vertex type: an explicit
// override wins, then the loaded reference, then the derived default.
func (g *GraphInfo) resolveVertexFile(typ string, override map[string]string) string {
	if n, ok := override[typ]; ok {
		return n
	}
	if n, ok := g.vertexFileRefs[typ]; ok {
		return n
	}
	return typ + ".vertex.yml"
}

// resolveEdgeFile picks the file name for an edge triplet, with the same
// precedence as resolveVertexFile.
func (g *GraphInfo) resolveEdgeFile(key EdgeTriplet, override map[EdgeTriplet]string) string {
	if n, ok := override[key]; ok {
		return n
	}
	if n, ok := g.edgeFileRefs[key]; ok {
		return n
	}
	return key.String() + ".edge.yml"
}

// Vertex returns the vertex info for the given type name.
func (g *GraphInfo) Vertex(typ string) (*VertexInfo, bool) {
	v, ok := g.vertices[typ]
	return v, ok
}

// Edge returns the edge info for the given triplet.
func (g *GraphInfo) Edge(src, edge, dst string) (*EdgeInfo, bool) {
	e, ok := g.edges[EdgeTriplet{Src: src, Edge: edge, Dst: dst}]
	return e, ok
}

// Vertices returns vertex infos sorted by type name.
func (g *GraphInfo) Vertices() []*VertexInfo {
	return slices.SortedFunc(maps.Values(g.vertices), func(a, b *VertexInfo) int {
		return cmp.Compare(a.Type(), b.Type())
	})
}

// Edges returns edge infos sorted by triplet.
func (g *GraphInfo) Edges() []*EdgeInfo {
	return slices.SortedFunc(maps.Values(g.edges), func(a, b *EdgeInfo) int {
		as, ae, ad := a.Triplet()
		bs, be, bd := b.Triplet()
		return EdgeTriplet{Src: as, Edge: ae, Dst: ad}.compare(EdgeTriplet{Src: bs, Edge: be, Dst: bd})
	})
}

// Validate enforces cross-field invariants: non-empty name and that every
// edge type references vertex types known to this graph.
func (g *GraphInfo) Validate() error {
	if g.name == "" {
		return newValidationError(ErrInvalidGraphInfo, "name", "must not be empty")
	}
	if err := g.version.Validate(); err != nil {
		return newValidationError(ErrInvalidGraphInfo, "version", "%s", err)
	}
	if err := validateLabels(g.labels, ErrInvalidGraphInfo); err != nil {
		return err
	}
	// Validate contained vertices and edges in deterministic (sorted) order so
	// that multi-failure messages are reproducible (Go map iteration is random).
	for _, typ := range slices.Sorted(maps.Keys(g.vertices)) {
		if err := g.vertices[typ].Validate(); err != nil {
			return fmt.Errorf("vertices[%q]: %w", typ, err)
		}
	}
	triplets := slices.SortedFunc(maps.Keys(g.edges), func(a, b EdgeTriplet) int {
		return a.compare(b)
	})
	for _, triplet := range triplets {
		if err := g.edges[triplet].Validate(); err != nil {
			return fmt.Errorf("edges[%s]: %w", triplet.String(), err)
		}
		if _, ok := g.vertices[triplet.Src]; !ok {
			return newValidationError(ErrInvalidGraphInfo,
				fmt.Sprintf("edges[%s].src_type", triplet.String()),
				"references unknown vertex type %q", triplet.Src)
		}
		if _, ok := g.vertices[triplet.Dst]; !ok {
			return newValidationError(ErrInvalidGraphInfo,
				fmt.Sprintf("edges[%s].dst_type", triplet.String()),
				"references unknown vertex type %q", triplet.Dst)
		}
	}
	return nil
}
