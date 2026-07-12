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

func mustVertex(t *testing.T, typ string) *VertexInfo {
	t.Helper()
	v, err := NewVertexInfo(typ, 100, []PropertyGroup{{
		Properties: []Property{{Name: "id", Type: types.Int64(), IsPrimary: true}},
		FileType:   types.FileTypeParquet,
	}})
	if err != nil {
		t.Fatalf("mustVertex(%q): %v", typ, err)
	}
	return v
}

func mustEdge(t *testing.T, src, edge, dst string) *EdgeInfo {
	t.Helper()
	e, err := NewEdgeInfo(src, edge, dst, 100, 100, 100, false, basicAdjLists())
	if err != nil {
		t.Fatalf("mustEdge: %v", err)
	}
	return e
}

func TestNewGraphInfoRejectsZeroVersion(t *testing.T) {
	t.Parallel()
	_, err := NewGraphInfo("g", WithGraphVersion(types.InfoVersion{}))
	if !errors.Is(err, ErrInvalidGraphInfo) {
		t.Errorf("expected ErrInvalidGraphInfo for zero version, got %v", err)
	}
}

func TestNewGraphInfoBasic(t *testing.T) {
	t.Parallel()
	person := mustVertex(t, "person")
	soft := mustVertex(t, "software")
	knows := mustEdge(t, "person", "knows", "person")
	created := mustEdge(t, "person", "created", "software")

	g, err := NewGraphInfo("g",
		WithGraphPrefix("graph/"),
		WithVertices(person, soft),
		WithEdges(knows, created),
	)
	if err != nil {
		t.Fatalf("NewGraphInfo: %v", err)
	}
	if g.Name() != "g" || g.Prefix() != "graph/" {
		t.Errorf("getters mismatch: name=%q prefix=%q", g.Name(), g.Prefix())
	}
	if got, ok := g.Vertex("person"); !ok || got.Type() != "person" {
		t.Errorf("Vertex(person) = (%v,%v)", got, ok)
	}
	if _, ok := g.Vertex("missing"); ok {
		t.Errorf("Vertex(missing) reported ok=true")
	}
	if got, ok := g.Edge("person", "knows", "person"); !ok || got.EdgeType() != "knows" {
		t.Errorf("Edge lookup failed: %v %v", got, ok)
	}
	if _, ok := g.Edge("person", "missing", "person"); ok {
		t.Errorf("Edge(person,missing,person) ok=true")
	}
	if vs := g.Vertices(); len(vs) != 2 || vs[0].Type() > vs[1].Type() {
		t.Errorf("Vertices not sorted: %v", vs)
	}
	if es := g.Edges(); len(es) != 2 {
		t.Errorf("Edges len = %d", len(es))
	}
}

func TestNewGraphInfoSortingEdges(t *testing.T) {
	t.Parallel()
	v1 := mustVertex(t, "a")
	v2 := mustVertex(t, "b")
	v3 := mustVertex(t, "c")
	es := []*EdgeInfo{
		mustEdge(t, "c", "k", "a"),
		mustEdge(t, "a", "k", "b"),
		mustEdge(t, "a", "k", "a"),
		mustEdge(t, "b", "k", "a"),
	}
	g, err := NewGraphInfo("g",
		WithVertices(v1, v2, v3),
		WithEdges(es...),
	)
	if err != nil {
		t.Fatalf("NewGraphInfo: %v", err)
	}
	got := g.Edges()
	// Must be lexicographically by (src,edge,dst).
	for i := 1; i < len(got); i++ {
		prev, cur := got[i-1], got[i]
		key := func(e *EdgeInfo) string {
			s, ed, d := e.Triplet()
			return s + "|" + ed + "|" + d
		}
		if key(prev) > key(cur) {
			t.Errorf("edges not sorted: %v then %v", key(prev), key(cur))
		}
	}
}

func TestNewGraphInfoValidationFailures(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		fn   func() (*GraphInfo, error)
		want error
	}{
		{"empty name", func() (*GraphInfo, error) { return NewGraphInfo("") }, ErrInvalidGraphInfo},
		{"nil vertex", func() (*GraphInfo, error) {
			return NewGraphInfo("g", WithVertices(nil))
		}, ErrInvalidGraphInfo},
		{"duplicate vertex", func() (*GraphInfo, error) {
			return NewGraphInfo("g", WithVertices(mustVertex(t, "a"), mustVertex(t, "a")))
		}, ErrInvalidGraphInfo},
		{"nil edge", func() (*GraphInfo, error) {
			return NewGraphInfo("g", WithVertices(mustVertex(t, "a")), WithEdges(nil))
		}, ErrInvalidGraphInfo},
		{"duplicate edge", func() (*GraphInfo, error) {
			return NewGraphInfo("g",
				WithVertices(mustVertex(t, "a"), mustVertex(t, "b")),
				WithEdges(mustEdge(t, "a", "k", "b"), mustEdge(t, "a", "k", "b")),
			)
		}, ErrInvalidGraphInfo},
		{"unknown src", func() (*GraphInfo, error) {
			return NewGraphInfo("g",
				WithVertices(mustVertex(t, "a")),
				WithEdges(mustEdge(t, "ghost", "k", "a")),
			)
		}, ErrInvalidGraphInfo},
		{"unknown dst", func() (*GraphInfo, error) {
			return NewGraphInfo("g",
				WithVertices(mustVertex(t, "a")),
				WithEdges(mustEdge(t, "a", "k", "ghost")),
			)
		}, ErrInvalidGraphInfo},
		{"empty label", func() (*GraphInfo, error) {
			return NewGraphInfo("g", WithGraphLabels(""))
		}, ErrInvalidGraphInfo},
		{"duplicate labels", func() (*GraphInfo, error) {
			return NewGraphInfo("g", WithGraphLabels("a", "a"))
		}, ErrInvalidGraphInfo},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := c.fn()
			if !errors.Is(err, c.want) {
				t.Errorf("got %v, want wrap of %v", err, c.want)
			}
		})
	}
}

func TestEdgeTripletString(t *testing.T) {
	t.Parallel()
	tr := EdgeTriplet{Src: "a", Edge: "k", Dst: "b"}
	if tr.String() != "a_k_b" {
		t.Errorf("EdgeTriplet.String = %q", tr.String())
	}
}

func TestGraphInfoValidateDeterministicErrorOrder(t *testing.T) {
	t.Parallel()
	// Two edges both reference unknown vertex types. With the pre-fix map
	// iteration, the failing field/triplet flipped run-to-run; the fix sorts
	// edges first, so repeated calls must produce identical errors.
	mk := func() error {
		// Construct a GraphInfo directly to bypass Validate-on-construct
		// (which would reject these edges immediately on the first random
		// iteration order). The Validate test must observe Validate alone.
		// The edges are individually valid but reference the unknown vertex
		// "z", so the deterministic failure is the endpoint check.
		v := mustVertex(t, "a")
		g := &GraphInfo{
			name:     "g",
			vertices: map[string]*VertexInfo{"a": v},
			edges: map[EdgeTriplet]*EdgeInfo{
				{Src: "z", Edge: "k", Dst: "a"}: mustEdge(t, "z", "k", "a"),
				{Src: "a", Edge: "k", Dst: "z"}: mustEdge(t, "a", "k", "z"),
			},
		}
		return g.Validate()
	}
	first := mk().Error()
	for range 20 {
		if got := mk().Error(); got != first {
			t.Fatalf("Validate error not deterministic across runs:\nfirst: %s\nthen:  %s", first, got)
		}
	}
}
