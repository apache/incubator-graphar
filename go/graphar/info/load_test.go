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
	"io/fs"
	"maps"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
)

func tinyFixtureFS() fstest.MapFS {
	return fstest.MapFS{
		"sample.graph.yml": &fstest.MapFile{Data: []byte(
			"name: sample\n" +
				"vertices:\n" +
				"  - person.vertex.yml\n" +
				"edges:\n" +
				"  - person_knows_person.edge.yml\n" +
				"version: gar/v1\n",
		)},
		"person.vertex.yml": &fstest.MapFile{Data: []byte(
			"type: person\n" +
				"chunk_size: 100\n" +
				"prefix: vertex/person/\n" +
				"property_groups:\n" +
				"  - properties:\n" +
				"      - name: id\n" +
				"        data_type: int64\n" +
				"        is_primary: true\n" +
				"    file_type: parquet\n" +
				"version: gar/v1\n",
		)},
		"person_knows_person.edge.yml": &fstest.MapFile{Data: []byte(
			"src_type: person\n" +
				"edge_type: knows\n" +
				"dst_type: person\n" +
				"chunk_size: 1\n" +
				"src_chunk_size: 1\n" +
				"dst_chunk_size: 1\n" +
				"directed: false\n" +
				"adj_lists:\n" +
				"  - ordered: true\n" +
				"    aligned_by: src\n" +
				"    file_type: parquet\n" +
				"version: gar/v1\n",
		)},
	}
}

func TestLoadGraphInfoFlat(t *testing.T) {
	t.Parallel()
	g, err := LoadGraphInfo(tinyFixtureFS(), "sample.graph.yml")
	if err != nil {
		t.Fatalf("LoadGraphInfo: %v", err)
	}
	if g.Name() != "sample" {
		t.Errorf("name = %q", g.Name())
	}
	if _, ok := g.Vertex("person"); !ok {
		t.Errorf("missing person vertex")
	}
	if _, ok := g.Edge("person", "knows", "person"); !ok {
		t.Errorf("missing person->knows->person edge")
	}
}

func TestLoadGraphInfoNestedDir(t *testing.T) {
	t.Parallel()
	// Same fixture, placed under a subdirectory to exercise path resolution.
	files := tinyFixtureFS()
	nested := fstest.MapFS{}
	for k, v := range files {
		nested["graphs/"+k] = v
	}
	g, err := LoadGraphInfo(nested, "graphs/sample.graph.yml")
	if err != nil {
		t.Fatalf("LoadGraphInfo (nested): %v", err)
	}
	if g.Name() != "sample" {
		t.Errorf("name = %q", g.Name())
	}
}

func TestLoadGraphInfoMissingGraphFile(t *testing.T) {
	t.Parallel()
	_, err := LoadGraphInfo(tinyFixtureFS(), "missing.graph.yml")
	if err == nil {
		t.Fatal("expected error opening missing graph yaml")
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("expected fs.ErrNotExist, got %v", err)
	}
}

func TestLoadGraphInfoMissingReferencedVertex(t *testing.T) {
	t.Parallel()
	fsys := fstest.MapFS{
		"g.graph.yml": &fstest.MapFile{Data: []byte(
			"name: g\nvertices:\n  - ghost.vertex.yml\nversion: gar/v1\n",
		)},
	}
	_, err := LoadGraphInfo(fsys, "g.graph.yml")
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("expected fs.ErrNotExist for missing vertex file, got %v", err)
	}
}

func TestLoadGraphInfoMissingReferencedEdge(t *testing.T) {
	t.Parallel()
	fsys := fstest.MapFS{
		"g.graph.yml": &fstest.MapFile{Data: []byte(
			"name: g\nedges:\n  - ghost.edge.yml\nversion: gar/v1\n",
		)},
	}
	_, err := LoadGraphInfo(fsys, "g.graph.yml")
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("expected fs.ErrNotExist for missing edge file, got %v", err)
	}
}

func TestLoadGraphInfoBadGraphYAML(t *testing.T) {
	t.Parallel()
	fsys := fstest.MapFS{
		"g.graph.yml": &fstest.MapFile{Data: []byte("name: [")},
	}
	_, err := LoadGraphInfo(fsys, "g.graph.yml")
	if !errors.Is(err, ErrInvalidYAML) {
		t.Errorf("expected ErrInvalidYAML, got %v", err)
	}
}

func TestLoadGraphInfoBadVersion(t *testing.T) {
	t.Parallel()
	fsys := fstest.MapFS{
		"g.graph.yml": &fstest.MapFile{Data: []byte("name: g\nversion: bogus\n")},
	}
	_, err := LoadGraphInfo(fsys, "g.graph.yml")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestLoadGraphInfoRejectsPathTraversal(t *testing.T) {
	t.Parallel()
	// References that escape the graph yaml's directory via ".." must be
	// rejected, even when fsys.Open would otherwise serve them.
	fsys := fstest.MapFS{
		"graphs/g.graph.yml": &fstest.MapFile{Data: []byte(
			"name: g\nvertices:\n  - ../secrets.vertex.yml\nversion: gar/v1\n",
		)},
		"secrets.vertex.yml": &fstest.MapFile{Data: []byte(
			"type: secret\nchunk_size: 1\n" +
				"property_groups:\n  - properties:\n      - name: id\n        data_type: int64\n        is_primary: true\n    file_type: parquet\n" +
				"version: gar/v1\n",
		)},
	}
	_, err := LoadGraphInfo(fsys, "graphs/g.graph.yml")
	if !errors.Is(err, ErrInvalidGraphInfo) {
		t.Errorf("expected ErrInvalidGraphInfo for path escape, got %v", err)
	}
}

func TestLoadGraphInfoRejectsAbsolutePath(t *testing.T) {
	t.Parallel()
	fsys := fstest.MapFS{
		"g.graph.yml": &fstest.MapFile{Data: []byte(
			"name: g\nvertices:\n  - /etc/passwd.vertex.yml\nversion: gar/v1\n",
		)},
	}
	_, err := LoadGraphInfo(fsys, "g.graph.yml")
	if !errors.Is(err, ErrInvalidGraphInfo) {
		t.Errorf("expected ErrInvalidGraphInfo for absolute path, got %v", err)
	}
}

func TestLoadGraphInfoRejectsDotFileRef(t *testing.T) {
	t.Parallel()
	fsys := fstest.MapFS{
		"g.graph.yml": &fstest.MapFile{Data: []byte(
			"name: g\nvertices:\n  - .\nversion: gar/v1\n",
		)},
	}
	_, err := LoadGraphInfo(fsys, "g.graph.yml")
	if !errors.Is(err, ErrInvalidGraphInfo) {
		t.Errorf("expected ErrInvalidGraphInfo for \".\" file ref, got %v", err)
	}
}

func TestLoadGraphInfoMissingNameRejected(t *testing.T) {
	t.Parallel()
	// A non-graph document mis-filed as the graph yaml decodes to a zero name.
	fsys := fstest.MapFS{
		"g.graph.yml": &fstest.MapFile{Data: []byte("prefix: p/\nversion: gar/v1\n")},
	}
	_, err := LoadGraphInfo(fsys, "g.graph.yml")
	if !errors.Is(err, ErrInvalidYAML) {
		t.Errorf("expected ErrInvalidYAML for graph with no name, got %v", err)
	}
}

func TestLoadGraphInfoBadCrossRef(t *testing.T) {
	t.Parallel()
	// Graph references vertices/edges that exist as files but the edge
	// triplet refers to a vertex type not declared in the graph itself.
	fsys := fstest.MapFS{
		"g.graph.yml": &fstest.MapFile{Data: []byte(
			"name: g\n" +
				"vertices:\n  - a.vertex.yml\n" +
				"edges:\n  - a_k_ghost.edge.yml\n" +
				"version: gar/v1\n",
		)},
		"a.vertex.yml": &fstest.MapFile{Data: []byte(
			"type: a\nchunk_size: 1\n" +
				"property_groups:\n  - properties:\n      - name: id\n        data_type: int64\n        is_primary: true\n    file_type: parquet\n" +
				"version: gar/v1\n",
		)},
		"a_k_ghost.edge.yml": &fstest.MapFile{Data: []byte(
			"src_type: a\nedge_type: k\ndst_type: ghost\n" +
				"chunk_size: 1\nsrc_chunk_size: 1\ndst_chunk_size: 1\ndirected: false\n" +
				"adj_lists:\n  - type: ordered_by_source\n    file_type: parquet\n" +
				"version: gar/v1\n",
		)},
	}
	// Liberal read: the graph loads even though an edge references an
	// undeclared vertex; Validate is what surfaces the cross-reference error.
	g, err := LoadGraphInfo(fsys, "g.graph.yml")
	if err != nil {
		t.Fatalf("LoadGraphInfo should be liberal, got load error: %v", err)
	}
	if err := g.Validate(); !errors.Is(err, ErrInvalidGraphInfo) {
		t.Errorf("expected ErrInvalidGraphInfo from Validate (unknown vertex referenced), got %v", err)
	}
}

// TestGraphInfoPreservesFileRefs checks that a file reference not matching the
// derived default ("People.vertex.yml" for type "person") survives a load and
// re-marshal instead of silently becoming "person.vertex.yml".
func TestGraphInfoPreservesFileRefs(t *testing.T) {
	t.Parallel()
	src := fstest.MapFS{
		"g.graph.yml": &fstest.MapFile{Data: []byte(
			"name: g\nvertices:\n  - People.vertex.yml\nversion: gar/v1\n")},
		"People.vertex.yml": &fstest.MapFile{Data: []byte(
			"type: person\nchunk_size: 1\n" +
				"property_groups:\n  - file_type: parquet\n    properties:\n      - name: id\n        data_type: int64\n        is_primary: true\n" +
				"version: gar/v1\n")},
	}
	g, err := LoadGraphInfo(src, "g.graph.yml")
	if err != nil {
		t.Fatalf("LoadGraphInfo: %v", err)
	}
	if name, ok := g.VertexFileName("person"); !ok || name != "People.vertex.yml" {
		t.Errorf("VertexFileName(person) = %q,%v, want People.vertex.yml,true", name, ok)
	}
	out, err := MarshalGraphInfo(g)
	if err != nil {
		t.Fatalf("MarshalGraphInfo: %v", err)
	}
	if s := string(out); !strings.Contains(s, "People.vertex.yml") || strings.Contains(s, "person.vertex.yml") {
		t.Errorf("marshaled refs not preserved\n%s", s)
	}
}

// TestGraphInfoLabelsAndExtraInfoRoundTrip covers modelling of graph-level
// labels and the extra_info key/value list: they used to trip KnownFields and
// be rejected; now they load, are queryable, and survive a marshal round-trip.
func TestGraphInfoLabelsAndExtraInfoRoundTrip(t *testing.T) {
	t.Parallel()
	vfile := &fstest.MapFile{Data: []byte(
		"type: a\nchunk_size: 1\n" +
			"property_groups:\n  - file_type: parquet\n    properties:\n      - name: id\n        data_type: int64\n        is_primary: true\n" +
			"version: gar/v1\n")}
	src := fstest.MapFS{
		"g.graph.yml": &fstest.MapFile{Data: []byte(
			"name: g\n" +
				"vertices:\n  - a.vertex.yml\n" +
				"labels:\n  - lbl1\n  - lbl2\n" +
				"extra_info:\n  - key: author\n    value: zeki\n  - key: created\n    value: \"2026\"\n" +
				"version: gar/v1\n")},
		"a.vertex.yml": vfile,
	}
	g, err := LoadGraphInfo(src, "g.graph.yml")
	if err != nil {
		t.Fatalf("LoadGraphInfo: %v", err)
	}
	if got := g.Labels(); !slices.Equal(got, []string{"lbl1", "lbl2"}) {
		t.Errorf("Labels() = %v, want [lbl1 lbl2]", got)
	}
	if ei := g.ExtraInfo(); ei["author"] != "zeki" || ei["created"] != "2026" {
		t.Errorf("ExtraInfo() = %v", ei)
	}

	out, err := MarshalGraphInfo(g)
	if err != nil {
		t.Fatalf("MarshalGraphInfo: %v", err)
	}
	rt := fstest.MapFS{
		"g.graph.yml":  &fstest.MapFile{Data: out},
		"a.vertex.yml": vfile,
	}
	g2, err := LoadGraphInfo(rt, "g.graph.yml")
	if err != nil {
		t.Fatalf("re-LoadGraphInfo: %v", err)
	}
	if !slices.Equal(g2.Labels(), g.Labels()) {
		t.Errorf("labels lost on round-trip: %v", g2.Labels())
	}
	if !maps.Equal(g2.ExtraInfo(), g.ExtraInfo()) {
		t.Errorf("extra_info lost on round-trip: %v", g2.ExtraInfo())
	}
}

// TestLoadGraphInfoLiberalValidateRecursion covers that a graph with a
// semantically invalid child (a CSV property group carrying a list type) LOADS
// but is rejected by Validate - exercising the graph -> vertex ->
// property-group validation recursion introduced with the liberal-read change.
func TestLoadGraphInfoLiberalValidateRecursion(t *testing.T) {
	t.Parallel()
	fsys := fstest.MapFS{
		"g.graph.yml": &fstest.MapFile{Data: []byte(
			"name: g\nvertices:\n  - a.vertex.yml\nversion: gar/v1\n")},
		"a.vertex.yml": &fstest.MapFile{Data: []byte(
			"type: a\nchunk_size: 1\n" +
				"property_groups:\n  - file_type: csv\n    properties:\n      - name: tags\n        data_type: list<int32>\n        is_primary: false\n" +
				"version: gar/v1\n")},
	}
	g, err := LoadGraphInfo(fsys, "g.graph.yml")
	if err != nil {
		t.Fatalf("liberal load should accept csv+list, got %v", err)
	}
	if err := g.Validate(); !errors.Is(err, ErrInvalidPropertyGroup) {
		t.Errorf("Validate should reject csv+list via recursion, got %v", err)
	}
}
