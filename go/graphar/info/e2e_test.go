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

package info_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/incubator-graphar/go/graphar/info"
	"github.com/apache/incubator-graphar/go/graphar/types"
)

// assertPropType fails the test unless pg has a property `name` of type want.
func assertPropType(t *testing.T, pg *info.PropertyGroups, name string, want types.DataType) {
	t.Helper()
	got, ok := pg.PropertyType(name)
	if !ok {
		t.Errorf("PropertyType(%s) not found, want %v", name, want)
		return
	}
	if !got.Equal(want) {
		t.Errorf("PropertyType(%s) = %v, want %v", name, got, want)
	}
}

// loadFixture loads a graph yaml from testing/<relDir>/<graphFile>. It skips
// (not fails) when the testing/ submodule or the specific fixture is absent,
// so a vanilla clone without submodules stays green.
func loadFixture(t *testing.T, relDir, graphFile string) *info.GraphInfo {
	t.Helper()
	root := testingRoot(t)
	if root == "" {
		t.Skip("testing/ submodule not initialised; run `git submodule update --init testing`")
	}
	dir := filepath.Join(root, relDir)
	if _, err := os.Stat(filepath.Join(dir, graphFile)); err != nil {
		t.Skipf("fixture %s/%s not present: %v", relDir, graphFile, err)
	}
	g, err := info.LoadGraphInfo(os.DirFS(dir), graphFile)
	if err != nil {
		t.Fatalf("LoadGraphInfo(%s/%s): %v", relDir, graphFile, err)
	}
	return g
}

// loadLdbc loads a graph yaml from testing/ldbc_sample/<format>/<graphFile>.
func loadLdbc(t *testing.T, format, graphFile string) *info.GraphInfo {
	t.Helper()
	return loadFixture(t, filepath.Join("ldbc_sample", format), graphFile)
}

// TestE2EPersonVertex verifies the parsed content of the ldbc_sample
// person vertex against the actual on-disk fixture, not just that it loads.
func TestE2EPersonVertex(t *testing.T) {
	t.Parallel()
	g := loadLdbc(t, "parquet", "ldbc_sample.graph.yml")

	if g.Name() != "ldbc_sample" {
		t.Errorf("Name() = %q, want ldbc_sample", g.Name())
	}
	if v := g.Version().String(); v != "gar/v1" {
		t.Errorf("Version() = %q, want gar/v1", v)
	}
	if n := len(g.Vertices()); n != 1 {
		t.Fatalf("len(Vertices()) = %d, want 1", n)
	}

	v, ok := g.Vertex("person")
	if !ok {
		t.Fatal("Vertex(person) not found")
	}
	if v.ChunkSize() != 100 {
		t.Errorf("ChunkSize() = %d, want 100", v.ChunkSize())
	}
	if v.Prefix() != "vertex/person/" {
		t.Errorf("Prefix() = %q, want vertex/person/", v.Prefix())
	}

	pg := v.PropertyGroups()
	if pg.Len() != 2 {
		t.Errorf("PropertyGroups.Len() = %d, want 2", pg.Len())
	}

	// id: int64, primary, alone in its own group.
	assertPropType(t, pg, "id", types.Int64())
	if !pg.IsPrimary("id") {
		t.Error("IsPrimary(id) = false, want true")
	}
	if idGroup, ok := pg.GroupOf("id"); !ok || len(idGroup.Properties) != 1 {
		t.Errorf("GroupOf(id) has %d properties, want 1", len(idGroup.Properties))
	}

	// firstName/lastName/gender: string, non-primary, sharing one group.
	for _, name := range []string{"firstName", "lastName", "gender"} {
		assertPropType(t, pg, name, types.String())
		if pg.IsPrimary(name) {
			t.Errorf("IsPrimary(%s) = true, want false", name)
		}
	}
	if nameGroup, ok := pg.GroupOf("firstName"); !ok || len(nameGroup.Properties) != 3 {
		t.Errorf("GroupOf(firstName) has %d properties, want 3", len(nameGroup.Properties))
	}
}

// TestE2EKnowsEdge verifies the parsed content of the person-knows-person edge,
// including the three adjacency-list layouts derived from (ordered, aligned_by).
func TestE2EKnowsEdge(t *testing.T) {
	t.Parallel()
	g := loadLdbc(t, "parquet", "ldbc_sample.graph.yml")

	e, ok := g.Edge("person", "knows", "person")
	if !ok {
		t.Fatal("Edge(person, knows, person) not found")
	}
	if e.ChunkSize() != 1024 {
		t.Errorf("ChunkSize() = %d, want 1024", e.ChunkSize())
	}
	if e.SrcChunkSize() != 100 || e.DstChunkSize() != 100 {
		t.Errorf("Src/DstChunkSize() = %d/%d, want 100/100", e.SrcChunkSize(), e.DstChunkSize())
	}
	if e.Directed() {
		t.Error("Directed() = true, want false")
	}
	if e.Prefix() != "edge/person_knows_person/" {
		t.Errorf("Prefix() = %q, want edge/person_knows_person/", e.Prefix())
	}

	// adj_lists: (false,src)->UnorderedBySource, (true,src)->OrderedBySource,
	// (true,dst)->OrderedByDest.
	if n := len(e.AdjListTypes()); n != 3 {
		t.Errorf("len(AdjListTypes()) = %d, want 3", n)
	}
	for _, want := range []types.AdjListType{
		types.UnorderedBySource, types.OrderedBySource, types.OrderedByDest,
	} {
		al, ok := e.AdjList(want)
		if !ok {
			t.Errorf("AdjList(%v) not found", want)
			continue
		}
		if al.FileType != types.FileTypeParquet {
			t.Errorf("AdjList(%v).FileType = %v, want parquet", want, al.FileType)
		}
	}

	// creationDate property: string.
	assertPropType(t, e.PropertyGroups(), "creationDate", types.String())
}

// TestE2EFeatureListType exercises the list<float> element-type path against the
// real with_feature fixture - the one list spelling shipped in testing/.
func TestE2EFeatureListType(t *testing.T) {
	t.Parallel()
	g := loadLdbc(t, "parquet", "ldbc_sample_with_feature.graph.yml")

	v, ok := g.Vertex("person")
	if !ok {
		t.Fatal("Vertex(person) not found")
	}
	if pg := v.PropertyGroups(); pg.Len() != 3 {
		t.Errorf("PropertyGroups.Len() = %d, want 3 (id, names, feature)", pg.Len())
	}

	got, ok := v.PropertyType("feature")
	if !ok {
		t.Fatal("PropertyType(feature) not found")
	}
	if !got.Equal(types.List(types.Float())) {
		t.Errorf("PropertyType(feature) = %v, want list<float>", got)
	}
	if got.String() != "list<float>" {
		t.Errorf("feature.String() = %q, want list<float>", got.String())
	}
}

// TestE2EScalarDateTimestamp exercises the date and timestamp scalar types via
// the dedicated ldbc_sample fixtures.
func TestE2EScalarDateTimestamp(t *testing.T) {
	t.Parallel()
	cases := []struct {
		graph    string
		edge     string
		property string
		want     types.DataType
	}{
		{"ldbc_sample_date.graph.yml", "knows-date", "creationDate-date", types.Date()},
		{"ldbc_sample_timestamp.graph.yml", "knows-timestamp", "creationDate-timestamp", types.Timestamp()},
	}
	for _, tc := range cases {
		t.Run(tc.edge, func(t *testing.T) {
			g := loadLdbc(t, "parquet", tc.graph)
			e, ok := g.Edge("person", tc.edge, "person")
			if !ok {
				t.Fatalf("Edge(person, %s, person) not found", tc.edge)
			}
			assertPropType(t, e.PropertyGroups(), tc.property, tc.want)
		})
	}
}

// TestE2EMultiFormat loads the same ldbc_sample graph from its csv, orc and
// parquet variants and asserts the file_type is parsed correctly per format.
func TestE2EMultiFormat(t *testing.T) {
	t.Parallel()
	cases := []struct {
		format string
		want   types.FileType
	}{
		{"csv", types.FileTypeCSV},
		{"orc", types.FileTypeORC},
		{"parquet", types.FileTypeParquet},
	}
	for _, tc := range cases {
		t.Run(tc.format, func(t *testing.T) {
			g := loadLdbc(t, tc.format, "ldbc_sample.graph.yml")
			v, ok := g.Vertex("person")
			if !ok {
				t.Fatal("Vertex(person) not found")
			}
			grp, ok := v.PropertyGroups().GroupOf("id")
			if !ok {
				t.Fatal("GroupOf(id) not found")
			}
			if grp.FileType != tc.want {
				t.Errorf("person id group FileType = %v, want %v", grp.FileType, tc.want)
			}
		})
	}
}

// TestE2EModernGraph covers the TinkerPop modern graph: multiple vertex/edge
// types, per-property-group prefixes, a directed edge, and the double type.
func TestE2EModernGraph(t *testing.T) {
	t.Parallel()
	g := loadFixture(t, "modern_graph", "modern_graph.graph.yml")

	if g.Name() != "modern_graph" {
		t.Errorf("Name() = %q, want modern_graph", g.Name())
	}
	if len(g.Vertices()) != 2 || len(g.Edges()) != 2 {
		t.Fatalf("got %d vertices / %d edges, want 2 / 2", len(g.Vertices()), len(g.Edges()))
	}

	person, ok := g.Vertex("person")
	if !ok {
		t.Fatal("Vertex(person) not found")
	}
	if person.ChunkSize() != 2 {
		t.Errorf("person.ChunkSize() = %d, want 2", person.ChunkSize())
	}
	assertPropType(t, person.PropertyGroups(), "age", types.Int64())
	// Per-group prefix is parsed.
	if grp, ok := person.PropertyGroups().GroupOf("id"); !ok || grp.Prefix != "id/" {
		t.Errorf("person id group Prefix = %q, want id/", grp.Prefix)
	}

	if sw, ok := g.Vertex("software"); !ok {
		t.Error("Vertex(software) not found")
	} else if sw.ChunkSize() != 10 {
		t.Errorf("software.ChunkSize() = %d, want 10", sw.ChunkSize())
	}

	// created: person -> software, directed, with a double weight property.
	created, ok := g.Edge("person", "created", "software")
	if !ok {
		t.Fatal("Edge(person, created, software) not found")
	}
	if !created.Directed() {
		t.Error("created.Directed() = false, want true")
	}
	if created.DstChunkSize() != 10 {
		t.Errorf("created.DstChunkSize() = %d, want 10", created.DstChunkSize())
	}
	assertPropType(t, created.PropertyGroups(), "weight", types.Double())
	// Single adjacency list with an explicit prefix.
	if al, ok := created.AdjList(types.OrderedBySource); !ok {
		t.Error("created AdjList(OrderedBySource) not found")
	} else if al.Prefix != "ordered_by_source/" {
		t.Errorf("created adj prefix = %q, want ordered_by_source/", al.Prefix)
	}
}

// TestE2ENebula covers the nebula fixture: a graph-level prefix, an edge that
// omits the `directed` field (must default to false), and non-primary props.
func TestE2ENebula(t *testing.T) {
	t.Parallel()
	g := loadFixture(t, "nebula", "basketballplayergraph.graph.yml")

	if g.Name() != "basketballplayergraph" {
		t.Errorf("Name() = %q, want basketballplayergraph", g.Name())
	}
	if g.Prefix() != "/tmp/graphar/nebula2graphar/" {
		t.Errorf("Prefix() = %q, want /tmp/graphar/nebula2graphar/", g.Prefix())
	}

	player, ok := g.Vertex("player")
	if !ok {
		t.Fatal("Vertex(player) not found")
	}
	assertPropType(t, player.PropertyGroups(), "_vertexId", types.String())
	if !player.PropertyGroups().IsPrimary("_vertexId") {
		t.Error("player._vertexId IsPrimary = false, want true")
	}

	follow, ok := g.Edge("player", "follow", "player")
	if !ok {
		t.Fatal("Edge(player, follow, player) not found")
	}
	// `directed` is absent in the fixture -> defaults to false.
	if follow.Directed() {
		t.Error("follow.Directed() = true, want false (absent -> default)")
	}
	assertPropType(t, follow.PropertyGroups(), "degree", types.Int64())
}

// TestE2ENeo4jMovie covers the neo4j MovieGraph: many edge types and an edge
// (ACTED_IN) that carries NO property groups at all.
func TestE2ENeo4jMovie(t *testing.T) {
	t.Parallel()
	g := loadFixture(t, "neo4j", "MovieGraph.graph.yml")

	if g.Name() != "MovieGraph" {
		t.Errorf("Name() = %q, want MovieGraph", g.Name())
	}
	if len(g.Vertices()) != 2 || len(g.Edges()) != 6 {
		t.Fatalf("got %d vertices / %d edges, want 2 / 6", len(g.Vertices()), len(g.Edges()))
	}

	person, ok := g.Vertex("Person")
	if !ok {
		t.Fatal("Vertex(Person) not found")
	}
	if !person.PropertyGroups().IsPrimary("name") {
		t.Error("Person.name IsPrimary = false, want true")
	}
	assertPropType(t, person.PropertyGroups(), "born", types.Int64())

	// ACTED_IN has adjacency lists but no property groups.
	acted, ok := g.Edge("Person", "ACTED_IN", "Movie")
	if !ok {
		t.Fatal("Edge(Person, ACTED_IN, Movie) not found")
	}
	if acted.PropertyGroups() != nil {
		t.Errorf("ACTED_IN.PropertyGroups() = %v, want nil (no properties)", acted.PropertyGroups())
	}
	if acted.HasProperty("weight") {
		t.Error("ACTED_IN.HasProperty(weight) = true, want false")
	}
	if len(acted.AdjListTypes()) != 2 {
		t.Errorf("ACTED_IN has %d adj lists, want 2", len(acted.AdjListTypes()))
	}
}

// TestE2EJavaLabelDialect verifies the Java `label:` alias (in place of `type:`)
// is accepted on read and mapped to the vertex Type.
func TestE2EJavaLabelDialect(t *testing.T) {
	t.Parallel()
	g := loadFixture(t, filepath.Join("java", "ldbc_sample", "parquet"), "ldbc_sample.graph.yml")

	// The person vertex file uses `label: person` rather than `type: person`.
	v, ok := g.Vertex("person")
	if !ok {
		t.Fatal("Vertex(person) not found - label: alias not mapped to Type")
	}
	if v.Type() != "person" {
		t.Errorf("Type() = %q, want person", v.Type())
	}
	assertPropType(t, v.PropertyGroups(), "id", types.Int64())
}

// TestE2ELdbcFull covers the full LDBC schema: many vertex and edge types,
// exercising the multi-entity index at scale.
func TestE2ELdbcFull(t *testing.T) {
	t.Parallel()
	g := loadFixture(t, "ldbc", "ldbc.graph.yml")

	if g.Name() != "ldbc" {
		t.Errorf("Name() = %q, want ldbc", g.Name())
	}
	if len(g.Vertices()) != 8 {
		t.Errorf("len(Vertices()) = %d, want 8", len(g.Vertices()))
	}
	if len(g.Edges()) != 23 {
		t.Errorf("len(Edges()) = %d, want 23", len(g.Edges()))
	}
	person, ok := g.Vertex("person")
	if !ok {
		t.Fatal("Vertex(person) not found")
	}
	if person.ChunkSize() != 4096 {
		t.Errorf("person.ChunkSize() = %d, want 4096", person.ChunkSize())
	}
	if !person.HasProperty("firstName") {
		t.Error("person.HasProperty(firstName) = false, want true")
	}
	// Spot-check a representative edge triplet resolves.
	if _, ok := g.Edge("person", "knows", "person"); !ok {
		t.Error("Edge(person, knows, person) not found")
	}
}

// TestE2EJSONRejected asserts the json fixture is rejected on Validate:
// file_type json is parseable (so liberal Load accepts it) but is not a valid
// PropertyGroup storage format, so Validate must reject it - for the right
// reason (property-group validation, not e.g. a missing file).
func TestE2EJSONRejected(t *testing.T) {
	t.Parallel()
	g := loadFixture(t, filepath.Join("ldbc_sample", "json"), "LdbcSample.graph.yml")
	err := g.Validate()
	if err == nil {
		t.Fatal("Validate accepted a json-file_type graph; want rejection")
	}
	if !errors.Is(err, info.ErrInvalidPropertyGroup) {
		t.Errorf("want ErrInvalidPropertyGroup, got %v", err)
	}
}
