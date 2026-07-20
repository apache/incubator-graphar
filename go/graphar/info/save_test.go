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
	"errors"
	"strings"
	"testing"
)

func TestMarshalNilInputs(t *testing.T) {
	t.Parallel()
	if _, err := MarshalVertexInfo(nil); !errors.Is(err, ErrInvalidVertexInfo) {
		t.Errorf("expected ErrInvalidVertexInfo, got %v", err)
	}
	if _, err := MarshalEdgeInfo(nil); !errors.Is(err, ErrInvalidEdgeInfo) {
		t.Errorf("expected ErrInvalidEdgeInfo, got %v", err)
	}
	if _, err := MarshalGraphInfo(nil); !errors.Is(err, ErrInvalidGraphInfo) {
		t.Errorf("expected ErrInvalidGraphInfo, got %v", err)
	}
}

func TestVertexInfoRoundTripBytes(t *testing.T) {
	t.Parallel()
	v, err := NewVertexInfo("person", 100, vertexGroupsOK())
	if err != nil {
		t.Fatalf("NewVertexInfo: %v", err)
	}
	data, err := MarshalVertexInfo(v)
	if err != nil {
		t.Fatalf("MarshalVertexInfo: %v", err)
	}
	v2, err := LoadVertexInfo(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("LoadVertexInfo: %v", err)
	}
	if v.Type() != v2.Type() || v.ChunkSize() != v2.ChunkSize() {
		t.Errorf("round-trip lost fields")
	}
	if v.PropertyGroups().Len() != v2.PropertyGroups().Len() {
		t.Errorf("round-trip group count")
	}
}

func TestEdgeInfoRoundTripBytes(t *testing.T) {
	t.Parallel()
	e, err := NewEdgeInfo("person", "knows", "person", 1024, 100, 100, false, basicAdjLists())
	if err != nil {
		t.Fatalf("NewEdgeInfo: %v", err)
	}
	data, err := MarshalEdgeInfo(e)
	if err != nil {
		t.Fatalf("MarshalEdgeInfo: %v", err)
	}
	e2, err := LoadEdgeInfo(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("LoadEdgeInfo: %v", err)
	}
	if e.SrcType() != e2.SrcType() || e.DstType() != e2.DstType() {
		t.Errorf("round-trip lost triplet")
	}
}

func TestMarshalGraphInfoDefaultNames(t *testing.T) {
	t.Parallel()
	// A programmatic graph has no recorded references, so names are derived.
	v := mustVertex(t, "a")
	e := mustEdge(t, "a", "k", "a")
	g, err := NewGraphInfo("g", WithVertices(v), WithEdges(e))
	if err != nil {
		t.Fatalf("NewGraphInfo: %v", err)
	}
	data, err := MarshalGraphInfo(g)
	if err != nil {
		t.Fatalf("MarshalGraphInfo: %v", err)
	}
	s := string(data)
	for _, want := range []string{"a.vertex.yml", "a_k_a.edge.yml"} {
		if !strings.Contains(s, want) {
			t.Errorf("output missing derived name %q\n%s", want, s)
		}
	}
}

func TestMarshalGraphInfoOverride(t *testing.T) {
	t.Parallel()
	v := mustVertex(t, "a")
	g, err := NewGraphInfo("g", WithVertices(v))
	if err != nil {
		t.Fatalf("NewGraphInfo: %v", err)
	}
	data, err := MarshalGraphInfo(g, WithVertexFileNames(map[string]string{"a": "custom.vertex.yml"}))
	if err != nil {
		t.Fatalf("MarshalGraphInfo: %v", err)
	}
	if s := string(data); !strings.Contains(s, "custom.vertex.yml") {
		t.Errorf("override name not used\n%s", s)
	}
}

func TestMarshalGraphInfoRejectsBadFileRef(t *testing.T) {
	t.Parallel()
	v := mustVertex(t, "a")
	g, err := NewGraphInfo("g", WithVertices(v))
	if err != nil {
		t.Fatalf("NewGraphInfo: %v", err)
	}
	bad := []string{"", "/abs/a.vertex.yml", "../a.vertex.yml", "sub/../../a.vertex.yml"}
	for _, name := range bad {
		_, err := MarshalGraphInfo(g, WithVertexFileNames(map[string]string{"a": name}))
		if !errors.Is(err, ErrInvalidGraphInfo) {
			t.Errorf("MarshalGraphInfo(vertexFiles=%q) = %v, want ErrInvalidGraphInfo", name, err)
		}
	}
}

func TestMarshalGraphInfoOK(t *testing.T) {
	t.Parallel()
	v := mustVertex(t, "a")
	w := mustVertex(t, "b")
	e := mustEdge(t, "a", "k", "b")
	g, err := NewGraphInfo("g", WithVertices(v, w), WithEdges(e), WithGraphPrefix("p/"))
	if err != nil {
		t.Fatalf("NewGraphInfo: %v", err)
	}
	data, err := MarshalGraphInfo(g,
		WithVertexFileNames(map[string]string{
			"a": "a.vertex.yml",
			"b": "b.vertex.yml",
		}),
		WithEdgeFileNames(map[EdgeTriplet]string{
			{Src: "a", Edge: "k", Dst: "b"}: "a_k_b.edge.yml",
		}),
	)
	if err != nil {
		t.Fatalf("MarshalGraphInfo: %v", err)
	}
	s := string(data)
	for _, want := range []string{"name: g", "p/", "a.vertex.yml", "b.vertex.yml", "a_k_b.edge.yml", "gar/v1"} {
		if !strings.Contains(s, want) {
			t.Errorf("MarshalGraphInfo output missing %q\n%s", want, s)
		}
	}
}

func TestWriteHelpers(t *testing.T) {
	t.Parallel()
	v, err := NewVertexInfo("person", 100, vertexGroupsOK())
	if err != nil {
		t.Fatalf("NewVertexInfo: %v", err)
	}
	var buf bytes.Buffer
	if err := WriteVertexInfo(&buf, v); err != nil {
		t.Fatalf("WriteVertexInfo: %v", err)
	}
	if buf.Len() == 0 {
		t.Errorf("WriteVertexInfo produced empty output")
	}

	e, err := NewEdgeInfo("person", "knows", "person", 1024, 100, 100, false, basicAdjLists())
	if err != nil {
		t.Fatalf("NewEdgeInfo: %v", err)
	}
	buf.Reset()
	if err := WriteEdgeInfo(&buf, e); err != nil {
		t.Fatalf("WriteEdgeInfo: %v", err)
	}
	if buf.Len() == 0 {
		t.Errorf("WriteEdgeInfo produced empty output")
	}

	g, err := NewGraphInfo("g",
		WithVertices(mustVertex(t, "a")),
	)
	if err != nil {
		t.Fatalf("NewGraphInfo: %v", err)
	}
	buf.Reset()
	if err := WriteGraphInfo(&buf, g); err != nil {
		t.Fatalf("WriteGraphInfo: %v", err)
	}
	if buf.Len() == 0 {
		t.Errorf("WriteGraphInfo produced empty output")
	}
}

func TestWriteHelpersPropagateError(t *testing.T) {
	t.Parallel()
	if err := WriteVertexInfo(failingWriter{}, mustVertex(t, "a")); err == nil {
		t.Errorf("WriteVertexInfo did not propagate writer error")
	}
	if err := WriteEdgeInfo(failingWriter{}, mustEdge(t, "a", "k", "b")); err == nil {
		t.Errorf("WriteEdgeInfo did not propagate writer error")
	}
	g, err := NewGraphInfo("g", WithVertices(mustVertex(t, "a")))
	if err != nil {
		t.Fatalf("NewGraphInfo: %v", err)
	}
	if err := WriteGraphInfo(failingWriter{}, g); err == nil {
		t.Errorf("WriteGraphInfo did not propagate writer error")
	}
}

type failingWriter struct{}

func (failingWriter) Write(_ []byte) (int, error) { return 0, errBrokenWriter }

var errBrokenWriter = errors.New("broken writer")

func TestMarshalGraphInfoRejectsDuplicateFileRef(t *testing.T) {
	t.Parallel()
	g, err := NewGraphInfo("g", WithVertices(mustVertex(t, "a"), mustVertex(t, "b")))
	if err != nil {
		t.Fatalf("NewGraphInfo: %v", err)
	}
	_, err = MarshalGraphInfo(g, WithVertexFileNames(map[string]string{
		"a": "same.vertex.yml",
		"b": "same.vertex.yml",
	}))
	if !errors.Is(err, ErrInvalidGraphInfo) {
		t.Errorf("expected ErrInvalidGraphInfo for duplicate file ref, got %v", err)
	}
}

func TestMarshalGraphInfoRejectsUnmatchedOverrideKey(t *testing.T) {
	t.Parallel()
	g, err := NewGraphInfo("g", WithVertices(mustVertex(t, "a")), WithEdges(mustEdge(t, "a", "k", "a")))
	if err != nil {
		t.Fatalf("NewGraphInfo: %v", err)
	}
	// Vertex key typo.
	if _, err := MarshalGraphInfo(g, WithVertexFileNames(map[string]string{"A": "x.vertex.yml"})); !errors.Is(err, ErrInvalidGraphInfo) {
		t.Errorf("expected ErrInvalidGraphInfo for unmatched vertex key, got %v", err)
	}
	// Edge key typo.
	bad := map[EdgeTriplet]string{{Src: "a", Edge: "nope", Dst: "a"}: "x.edge.yml"}
	if _, err := MarshalGraphInfo(g, WithEdgeFileNames(bad)); !errors.Is(err, ErrInvalidGraphInfo) {
		t.Errorf("expected ErrInvalidGraphInfo for unmatched edge key, got %v", err)
	}
}
