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
	"bytes"
	"fmt"
	"io"
	"maps"
	"slices"

	yaml "gopkg.in/yaml.v3"
)

// MarshalVertexInfo serialises v to a YAML document.
func MarshalVertexInfo(v *VertexInfo) ([]byte, error) {
	if v == nil {
		return nil, fmt.Errorf("%w: nil vertex info", ErrInvalidVertexInfo)
	}
	return encode(vertexInfoToYAML(v))
}

// MarshalEdgeInfo serialises e to a YAML document.
func MarshalEdgeInfo(e *EdgeInfo) ([]byte, error) {
	if e == nil {
		return nil, fmt.Errorf("%w: nil edge info", ErrInvalidEdgeInfo)
	}
	return encode(edgeInfoToYAML(e))
}

// GraphFilesOption overrides the file names MarshalGraphInfo writes for a
// graph's vertices/edges.
type GraphFilesOption func(*graphFilesConfig)

type graphFilesConfig struct {
	vertexFiles map[string]string
	edgeFiles   map[EdgeTriplet]string
}

// WithVertexFileNames overrides vertex file references, keyed by vertex type.
// Entries not listed fall back to the loaded or derived name.
func WithVertexFileNames(m map[string]string) GraphFilesOption {
	return func(c *graphFilesConfig) { c.vertexFiles = m }
}

// WithEdgeFileNames overrides edge file references, keyed by triplet. Entries
// not listed fall back to the loaded or derived name.
func WithEdgeFileNames(m map[EdgeTriplet]string) GraphFilesOption {
	return func(c *graphFilesConfig) { c.edgeFiles = m }
}

// MarshalGraphInfo serialises the top-level graph yaml referencing each
// vertex/edge by file name. By default it reproduces the references recorded
// when the graph was loaded, deriving "<type>.vertex.yml" /
// "<src>_<edge>_<dst>.edge.yml" for anything without one; pass
// WithVertexFileNames / WithEdgeFileNames to override specific names.
//
// Output is emitted in g.Vertices() / g.Edges() sorted order.
func MarshalGraphInfo(g *GraphInfo, opts ...GraphFilesOption) ([]byte, error) {
	if g == nil {
		return nil, fmt.Errorf("%w: nil graph info", ErrInvalidGraphInfo)
	}
	var cfg graphFilesConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	vs := g.Vertices()
	es := g.Edges()
	if err := checkFileNameOverrides(vs, es, cfg); err != nil {
		return nil, err
	}
	// Every reference must resolve to a distinct file; two entries pointing at
	// one file produce a graph yaml that fails to load ("duplicate ... type").
	seen := make(map[string]struct{}, len(vs)+len(es))
	vNames := make([]string, len(vs))
	for i, v := range vs {
		name := g.resolveVertexFile(v.Type(), cfg.vertexFiles)
		if err := validateFileRef(name); err != nil {
			return nil, fmt.Errorf("vertex %q: %w", v.Type(), err)
		}
		if _, dup := seen[name]; dup {
			return nil, fmt.Errorf("%w: duplicate file reference %q", ErrInvalidGraphInfo, name)
		}
		seen[name] = struct{}{}
		vNames[i] = name
	}
	eNames := make([]string, len(es))
	for i, e := range es {
		src, edge, dst := e.Triplet()
		key := EdgeTriplet{Src: src, Edge: edge, Dst: dst}
		name := g.resolveEdgeFile(key, cfg.edgeFiles)
		if err := validateFileRef(name); err != nil {
			return nil, fmt.Errorf("edge %s: %w", key.String(), err)
		}
		if _, dup := seen[name]; dup {
			return nil, fmt.Errorf("%w: duplicate file reference %q", ErrInvalidGraphInfo, name)
		}
		seen[name] = struct{}{}
		eNames[i] = name
	}
	y := graphInfoYAML{
		Name:    g.Name(),
		Prefix:  g.Prefix(),
		Version: g.Version().String(),
	}
	if len(vNames) > 0 {
		y.Vertices = vNames
	}
	if len(eNames) > 0 {
		y.Edges = eNames
	}
	if labels := g.Labels(); len(labels) > 0 {
		y.Labels = labels
	}
	if extra := g.ExtraInfo(); len(extra) > 0 {
		keys := make([]string, 0, len(extra))
		for k := range extra {
			keys = append(keys, k)
		}
		slices.Sort(keys) // deterministic output; on-disk order is not significant
		y.ExtraInfo = make([]extraInfoYAML, len(keys))
		for i, k := range keys {
			y.ExtraInfo[i] = extraInfoYAML{Key: k, Value: extra[k]}
		}
	}
	return encode(y)
}

// checkFileNameOverrides rejects override keys that match no vertex type or edge
// triplet, so a mistyped key surfaces instead of being silently dropped. Keys
// are checked in sorted order for a stable error.
func checkFileNameOverrides(vs []*VertexInfo, es []*EdgeInfo, cfg graphFilesConfig) error {
	if len(cfg.vertexFiles) > 0 {
		known := make(map[string]struct{}, len(vs))
		for _, v := range vs {
			known[v.Type()] = struct{}{}
		}
		for _, k := range slices.Sorted(maps.Keys(cfg.vertexFiles)) {
			if _, ok := known[k]; !ok {
				return fmt.Errorf("%w: WithVertexFileNames key %q matches no vertex type",
					ErrInvalidGraphInfo, k)
			}
		}
	}
	if len(cfg.edgeFiles) > 0 {
		known := make(map[EdgeTriplet]struct{}, len(es))
		for _, e := range es {
			src, edge, dst := e.Triplet()
			known[EdgeTriplet{Src: src, Edge: edge, Dst: dst}] = struct{}{}
		}
		keys := slices.SortedFunc(maps.Keys(cfg.edgeFiles), func(a, b EdgeTriplet) int {
			return a.compare(b)
		})
		for _, k := range keys {
			if _, ok := known[k]; !ok {
				return fmt.Errorf("%w: WithEdgeFileNames key %q matches no edge triplet",
					ErrInvalidGraphInfo, k.String())
			}
		}
	}
	return nil
}

// WriteVertexInfo encodes v to w.
func WriteVertexInfo(w io.Writer, v *VertexInfo) error {
	buf, err := MarshalVertexInfo(v)
	if err != nil {
		return err
	}
	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("write vertex info: %w", err)
	}
	return nil
}

// WriteEdgeInfo encodes e to w.
func WriteEdgeInfo(w io.Writer, e *EdgeInfo) error {
	buf, err := MarshalEdgeInfo(e)
	if err != nil {
		return err
	}
	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("write edge info: %w", err)
	}
	return nil
}

// WriteGraphInfo encodes the top-level graph yaml; see MarshalGraphInfo for
// how file references are resolved and overridden.
func WriteGraphInfo(w io.Writer, g *GraphInfo, opts ...GraphFilesOption) error {
	buf, err := MarshalGraphInfo(g, opts...)
	if err != nil {
		return err
	}
	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("write graph info: %w", err)
	}
	return nil
}

func encode(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(v); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidYAML, err)
	}
	if err := enc.Close(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidYAML, err)
	}
	return buf.Bytes(), nil
}
