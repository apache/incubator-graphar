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

// Package info contains GraphAr metadata models and utilities for
// parsing and serializing Info YAML files (graph, vertex, edge).
//
// The design is inspired by the Java info module, but follows
// idiomatic Go conventions (errors instead of exceptions, simple
// struct types, and explicit validation).

package info

import (
	"fmt"
	"net/url"
)

// concatEdgeTriplet joins src/edge/dst types into a stable key.
func concatEdgeTriplet(srcType, edgeType, dstType string) string {
	return srcType + regularSeparator + edgeType + regularSeparator + dstType
}

// GraphInfo aggregates metadata for a whole graph: vertices, edges, and layout.
type GraphInfo struct {
	Name    string      `yaml:"name"`
	Prefix  string      `yaml:"prefix"`
	Version VersionInfo `yaml:"version"`

	VertexInfos []*VertexInfo `yaml:"-"`
	EdgeInfos   []*EdgeInfo   `yaml:"-"`

	BaseURI *url.URL `yaml:"-"`

	vertexByType   map[string]*VertexInfo
	edgeByKey      map[string]*EdgeInfo
	types2StoreURI map[string]*url.URL
}

func (g *GraphInfo) GetPrefix() *url.URL {
	return g.BaseURI
}

func (g *GraphInfo) HasVertexInfo(vertexType string) bool {
	_, ok := g.vertexByType[vertexType]
	return ok
}

func (g *GraphInfo) HasEdgeInfo(srcType, edgeType, dstType string) bool {
	_, ok := g.edgeByKey[concatEdgeTriplet(srcType, edgeType, dstType)]
	return ok
}

func (g *GraphInfo) GetVertexInfo(vertexType string) (*VertexInfo, error) {
	v, ok := g.vertexByType[vertexType]
	if !ok {
		return nil, fmt.Errorf("%w: vertex type %q", ErrNotFound, vertexType)
	}
	return v, nil
}

func (g *GraphInfo) GetEdgeInfo(srcType, edgeType, dstType string) (*EdgeInfo, error) {
	k := concatEdgeTriplet(srcType, edgeType, dstType)
	e, ok := g.edgeByKey[k]
	if !ok {
		return nil, fmt.Errorf("%w: edge triplet %q", ErrNotFound, k)
	}
	return e, nil
}

func (g *GraphInfo) SetStoreURIForVertex(vertexType string, storeURI *url.URL) error {
	if storeURI == nil {
		return fmt.Errorf("%w: storeURI is nil", ErrInvalid)
	}
	if !g.HasVertexInfo(vertexType) {
		return fmt.Errorf("%w: vertex type %q", ErrNotFound, vertexType)
	}
	if g.types2StoreURI == nil {
		g.types2StoreURI = make(map[string]*url.URL)
	}
	g.types2StoreURI[vertexType] = storeURI
	return nil
}

func (g *GraphInfo) SetStoreURIForEdge(srcType, edgeType, dstType string, storeURI *url.URL) error {
	if storeURI == nil {
		return fmt.Errorf("%w: storeURI is nil", ErrInvalid)
	}
	k := concatEdgeTriplet(srcType, edgeType, dstType)
	if _, ok := g.edgeByKey[k]; !ok {
		return fmt.Errorf("%w: edge triplet %q", ErrNotFound, k)
	}
	if g.types2StoreURI == nil {
		g.types2StoreURI = make(map[string]*url.URL)
	}
	g.types2StoreURI[k] = storeURI
	return nil
}

func (g *GraphInfo) GetStoreURIForVertex(vertexType string) (*url.URL, error) {
	if g.types2StoreURI == nil {
		return nil, fmt.Errorf("%w: store uri not set", ErrNotFound)
	}
	u, ok := g.types2StoreURI[vertexType]
	if !ok {
		return nil, fmt.Errorf("%w: vertex type %q", ErrNotFound, vertexType)
	}
	return u, nil
}

func (g *GraphInfo) GetStoreURIForEdge(srcType, edgeType, dstType string) (*url.URL, error) {
	k := concatEdgeTriplet(srcType, edgeType, dstType)
	if g.types2StoreURI == nil {
		return nil, fmt.Errorf("%w: store uri not set", ErrNotFound)
	}
	u, ok := g.types2StoreURI[k]
	if !ok {
		return nil, fmt.Errorf("%w: edge triplet %q", ErrNotFound, k)
	}
	return u, nil
}

func (g *GraphInfo) GetTypes2StoreURI() map[string]*url.URL {
	out := make(map[string]*url.URL, len(g.types2StoreURI))
	for k, v := range g.types2StoreURI {
		out[k] = v
	}
	return out
}

// Init builds internal indices after unmarshalling or manual construction.
func (g *GraphInfo) Init() error {
	if g.Prefix != "" && g.BaseURI == nil {
		u, err := url.Parse(g.Prefix)
		if err != nil {
			return fmt.Errorf("%w: invalid graph base uri %q: %v", ErrInvalid, g.Prefix, err)
		}
		g.BaseURI = u
	}

	g.vertexByType = make(map[string]*VertexInfo, len(g.VertexInfos))
	for _, v := range g.VertexInfos {
		if err := v.Init(); err != nil {
			return err
		}
		g.vertexByType[v.Type] = v
	}

	g.edgeByKey = make(map[string]*EdgeInfo, len(g.EdgeInfos))
	for _, e := range g.EdgeInfos {
		if err := e.Init(); err != nil {
			return err
		}
		g.edgeByKey[e.Key()] = e
	}

	if g.types2StoreURI == nil {
		g.types2StoreURI = make(map[string]*url.URL)
	}
	return nil
}

// Validate checks that the GraphInfo is structurally valid.
func (g *GraphInfo) Validate() error {
	if g.Name == "" {
		return fmt.Errorf("%w: graph name is empty", ErrMissingField)
	}
	if g.BaseURI == nil && g.Prefix == "" {
		return fmt.Errorf("%w: graph base uri/prefix is empty", ErrMissingField)
	}
	if len(g.VertexInfos) != len(g.vertexByType) {
		return fmt.Errorf("%w: vertex type map size mismatch (possible duplicate type)", ErrInvalid)
	}
	if len(g.EdgeInfos) != len(g.edgeByKey) {
		return fmt.Errorf("%w: edge concat map size mismatch (possible duplicate edge)", ErrInvalid)
	}
	for _, v := range g.VertexInfos {
		if err := v.Validate(); err != nil {
			return err
		}
	}
	for _, e := range g.EdgeInfos {
		if err := e.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// Compatibility helpers (keep old method names temporarily during refactor).
func (g *GraphInfo) HasVertex(t string) bool            { return g.HasVertexInfo(t) }
func (g *GraphInfo) HasEdge(src, edge, dst string) bool { return g.HasEdgeInfo(src, edge, dst) }
func (g *GraphInfo) VertexInfoByType(t string) (*VertexInfo, bool) {
	v, ok := g.vertexByType[t]
	return v, ok
}
func (g *GraphInfo) EdgeInfoByTypes(src, edge, dst string) (*EdgeInfo, bool) {
	e, ok := g.edgeByKey[concatEdgeTriplet(src, edge, dst)]
	return e, ok
}
