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

	yaml "gopkg.in/yaml.v3"

	"github.com/apache/incubator-graphar/go/graphar/types"
)

func TestLoadVertexInfoRoundTrip(t *testing.T) {
	t.Parallel()
	in := `type: person
chunk_size: 100
prefix: vertex/person/
property_groups:
  - properties:
      - name: id
        data_type: int64
        is_primary: true
    file_type: parquet
  - properties:
      - name: firstName
        data_type: string
        is_primary: false
      - name: lastName
        data_type: string
        is_primary: false
    file_type: parquet
version: gar/v1
`
	v, err := LoadVertexInfo(strings.NewReader(in))
	if err != nil {
		t.Fatalf("LoadVertexInfo: %v", err)
	}
	if v.Type() != "person" || v.ChunkSize() != 100 {
		t.Errorf("getters mismatch: %+v", v)
	}
	if !v.HasProperty("id") || !v.HasProperty("firstName") {
		t.Errorf("missing expected properties")
	}
	// Re-marshal and re-load: must produce equivalent in-memory state.
	out, err := MarshalVertexInfo(v)
	if err != nil {
		t.Fatalf("MarshalVertexInfo: %v", err)
	}
	v2, err := LoadVertexInfo(strings.NewReader(string(out)))
	if err != nil {
		t.Fatalf("second LoadVertexInfo: %v", err)
	}
	if v2.Type() != v.Type() || v2.ChunkSize() != v.ChunkSize() {
		t.Errorf("round-trip mismatch")
	}
	if v2.PropertyGroups().Len() != v.PropertyGroups().Len() {
		t.Errorf("group count mismatch after round-trip")
	}
}

func TestLoadVertexInfoCardinalityAndNullable(t *testing.T) {
	t.Parallel()
	in := `type: person
chunk_size: 100
property_groups:
  - file_type: parquet
    properties:
      - name: id
        data_type: int64
        is_primary: true
  - file_type: parquet
    properties:
      - name: emails
        data_type: string
        is_primary: false
        cardinality: list
      - name: nickname
        data_type: string
        is_primary: false
        is_nullable: true
version: gar/v1
`
	v, err := LoadVertexInfo(strings.NewReader(in))
	if err != nil {
		t.Fatalf("LoadVertexInfo: %v", err)
	}
	p, _, ok := v.PropertyGroups().Property("emails")
	if !ok || p.Cardinality != types.CardinalityList {
		t.Errorf("emails cardinality not parsed: %+v", p)
	}
	if !v.PropertyGroups().IsNullable("nickname") {
		t.Errorf("nickname should be nullable")
	}
}

func TestLoadEdgeInfoLegacyAdjList(t *testing.T) {
	t.Parallel()
	// adj_lists without explicit type, only ordered + aligned_by.
	in := `src_type: person
edge_type: knows
dst_type: person
chunk_size: 1024
src_chunk_size: 100
dst_chunk_size: 100
directed: false
prefix: edge/person_knows_person/
adj_lists:
  - ordered: true
    aligned_by: src
    file_type: parquet
  - ordered: true
    aligned_by: dst
    file_type: parquet
version: gar/v1
`
	e, err := LoadEdgeInfo(strings.NewReader(in))
	if err != nil {
		t.Fatalf("LoadEdgeInfo: %v", err)
	}
	if _, ok := e.AdjList(types.OrderedBySource); !ok {
		t.Errorf("OrderedBySource missing")
	}
	if _, ok := e.AdjList(types.OrderedByDest); !ok {
		t.Errorf("OrderedByDest missing")
	}
}

func TestLoadEdgeInfoExplicitAdjList(t *testing.T) {
	t.Parallel()
	in := `src_type: person
edge_type: knows
dst_type: person
chunk_size: 1
src_chunk_size: 1
dst_chunk_size: 1
directed: false
adj_lists:
  - type: ordered_by_source
    file_type: parquet
version: gar/v1
`
	e, err := LoadEdgeInfo(strings.NewReader(in))
	if err != nil {
		t.Fatalf("LoadEdgeInfo: %v", err)
	}
	if _, ok := e.AdjList(types.OrderedBySource); !ok {
		t.Errorf("explicit-type adj_list not parsed")
	}
}

func TestLoadEdgeInfoMissingAdjListTypeRejected(t *testing.T) {
	t.Parallel()
	in := `src_type: s
edge_type: k
dst_type: d
chunk_size: 1
src_chunk_size: 1
dst_chunk_size: 1
directed: false
adj_lists:
  - file_type: parquet
version: gar/v1
`
	_, err := LoadEdgeInfo(strings.NewReader(in))
	if !errors.Is(err, ErrInvalidEdgeInfo) {
		t.Errorf("expected ErrInvalidEdgeInfo, got %v", err)
	}
}

func TestLoadVertexInfoUnknownDataTypeAsUserDefined(t *testing.T) {
	t.Parallel()
	// Liberal read: an unrecognised type name loads as a user-defined type.
	in := `type: p
chunk_size: 1
property_groups:
  - properties:
      - name: id
        data_type: myType
        is_primary: true
    file_type: parquet
version: gar/v1
`
	v, err := LoadVertexInfo(strings.NewReader(in))
	if err != nil {
		t.Fatalf("LoadVertexInfo: %v", err)
	}
	got, ok := v.PropertyType("id")
	if !ok || !got.Equal(types.UserDefined("myType")) {
		t.Errorf("PropertyType(id) = %v ok=%v, want UserDefined(myType)", got, ok)
	}
}

func TestUserDefinedTypeRoundTrip(t *testing.T) {
	t.Parallel()
	v, err := NewVertexInfo("person", 100, []PropertyGroup{{
		Properties: []Property{
			{Name: "id", Type: types.Int64(), IsPrimary: true},
			{Name: "geo", Type: types.UserDefined("myType")},
		},
		FileType: types.FileTypeParquet,
	}}, WithVertexVersion(types.NewInfoVersion(1, "myType")))
	if err != nil {
		t.Fatalf("NewVertexInfo with declared user-defined type: %v", err)
	}
	data, err := MarshalVertexInfo(v)
	if err != nil {
		t.Fatalf("MarshalVertexInfo: %v", err)
	}
	v2, err := LoadVertexInfo(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("LoadVertexInfo: %v", err)
	}
	if err := v2.Validate(); err != nil {
		t.Fatalf("reloaded Validate: %v", err)
	}
	got, ok := v2.PropertyType("geo")
	if !ok || !got.Equal(types.UserDefined("myType")) {
		t.Errorf("PropertyType(geo) = %v ok=%v, want UserDefined(myType)", got, ok)
	}
	if !v2.Version().Equal(v.Version()) {
		t.Errorf("version lost on round-trip: %v vs %v", v2.Version(), v.Version())
	}
}

func TestUserDefinedTypeUndeclaredRejected(t *testing.T) {
	t.Parallel()
	// A user-defined type not listed in the version must fail Validate.
	_, err := NewVertexInfo("person", 100, []PropertyGroup{{
		Properties: []Property{{Name: "geo", Type: types.UserDefined("myType"), IsPrimary: true}},
		FileType:   types.FileTypeParquet,
	}})
	if !errors.Is(err, ErrInvalidVertexInfo) {
		t.Errorf("expected ErrInvalidVertexInfo for undeclared user-defined type, got %v", err)
	}
}

func TestLoadVertexInfoEmptyDataTypeRejected(t *testing.T) {
	t.Parallel()
	in := `type: p
chunk_size: 1
property_groups:
  - properties:
      - name: id
        data_type: ""
        is_primary: true
    file_type: parquet
version: gar/v1
`
	_, err := LoadVertexInfo(strings.NewReader(in))
	if !errors.Is(err, types.ErrInvalidDataType) {
		t.Errorf("expected types.ErrInvalidDataType for empty data_type, got %v", err)
	}
}

func TestLoadVertexInfoMalformedListRejected(t *testing.T) {
	t.Parallel()
	// A missing closing bracket is a typo, not a custom type name.
	in := `type: p
chunk_size: 1
property_groups:
  - properties:
      - name: id
        data_type: list<int32
        is_primary: true
    file_type: parquet
version: gar/v1
`
	_, err := LoadVertexInfo(strings.NewReader(in))
	if !errors.Is(err, types.ErrInvalidDataType) {
		t.Errorf("expected types.ErrInvalidDataType for malformed list, got %v", err)
	}
}

func TestLoadVertexInfoUserDefinedTrimmed(t *testing.T) {
	t.Parallel()
	// A padded unknown name is trimmed before becoming a user-defined type, so
	// it validates against a declared name without surrounding whitespace.
	in := `type: p
chunk_size: 1
property_groups:
  - properties:
      - name: geo
        data_type: "  Vector  "
    file_type: parquet
version: gar/v1 (Vector)
`
	v, err := LoadVertexInfo(strings.NewReader(in))
	if err != nil {
		t.Fatalf("LoadVertexInfo: %v", err)
	}
	got, ok := v.PropertyType("geo")
	if !ok || !got.Equal(types.UserDefined("Vector")) {
		t.Errorf("PropertyType(geo) = %v ok=%v, want UserDefined(Vector)", got, ok)
	}
	if err := v.Validate(); err != nil {
		t.Errorf("Validate after trim = %v, want nil", err)
	}
}

func TestLoadVertexInfoMissingTypeRejected(t *testing.T) {
	t.Parallel()
	// An edge document mis-filed under a vertex reference decodes to a zero type.
	in := `src_type: a
edge_type: k
dst_type: b
chunk_size: 1
`
	_, err := LoadVertexInfo(strings.NewReader(in))
	if !errors.Is(err, ErrInvalidYAML) {
		t.Errorf("expected ErrInvalidYAML for vertex with no type, got %v", err)
	}
}

func TestLoadEdgeInfoMissingTripletRejected(t *testing.T) {
	t.Parallel()
	// A vertex document mis-filed under an edge reference lacks the triplet.
	in := `type: p
chunk_size: 1
`
	_, err := LoadEdgeInfo(strings.NewReader(in))
	if !errors.Is(err, ErrInvalidYAML) {
		t.Errorf("expected ErrInvalidYAML for edge with no triplet, got %v", err)
	}
}

func TestLoadVertexInfoIgnoresUnknownField(t *testing.T) {
	t.Parallel()
	// Unknown fields are ignored so documents written by newer SDKs still load.
	in := `type: p
chunk_size: 1
new_field: surprise
property_groups:
  - properties:
      - name: id
        data_type: int64
        is_primary: true
    file_type: parquet
version: gar/v1
`
	v, err := LoadVertexInfo(strings.NewReader(in))
	if err != nil {
		t.Fatalf("LoadVertexInfo should ignore unknown field, got %v", err)
	}
	if v.Type() != "p" {
		t.Errorf("Type() = %q, want p", v.Type())
	}
}

func TestLoadVertexInfoBadFileType(t *testing.T) {
	t.Parallel()
	in := `type: p
chunk_size: 1
property_groups:
  - properties:
      - name: id
        data_type: int64
        is_primary: true
    file_type: tsv
version: gar/v1
`
	_, err := LoadVertexInfo(strings.NewReader(in))
	if !errors.Is(err, types.ErrInvalidFileType) {
		t.Errorf("expected ErrInvalidFileType, got %v", err)
	}
}

func TestLoadVertexInfoBadCardinality(t *testing.T) {
	t.Parallel()
	in := `type: p
chunk_size: 1
property_groups:
  - properties:
      - name: id
        data_type: int64
        is_primary: true
      - name: tags
        data_type: string
        is_primary: false
        cardinality: many
    file_type: parquet
version: gar/v1
`
	_, err := LoadVertexInfo(strings.NewReader(in))
	if !errors.Is(err, types.ErrInvalidCardinality) {
		t.Errorf("expected ErrInvalidCardinality, got %v", err)
	}
}

func TestLoadVertexInfoBadVersion(t *testing.T) {
	t.Parallel()
	in := `type: p
chunk_size: 1
property_groups:
  - properties:
      - name: id
        data_type: int64
        is_primary: true
    file_type: parquet
version: not-a-version
`
	_, err := LoadVertexInfo(strings.NewReader(in))
	if !errors.Is(err, types.ErrInvalidVersion) {
		t.Errorf("expected ErrInvalidVersion, got %v", err)
	}
}

func TestLoadVertexInfoMalformedYAML(t *testing.T) {
	t.Parallel()
	_, err := LoadVertexInfo(strings.NewReader("type: ["))
	if !errors.Is(err, ErrInvalidYAML) {
		t.Errorf("expected ErrInvalidYAML, got %v", err)
	}
}

func TestLoadVertexInfoPreservesYAMLErrorChain(t *testing.T) {
	t.Parallel()
	// Decoder errors must remain in the Unwrap chain so callers can pin
	// line numbers via errors.As(yaml.TypeError) - the original "%w + %s"
	// wrap broke this.
	in := `type: p
chunk_size: not_an_int
property_groups:
  - properties:
      - name: id
        data_type: int64
        is_primary: true
    file_type: parquet
version: gar/v1
`
	_, err := LoadVertexInfo(strings.NewReader(in))
	if err == nil {
		t.Fatal("expected error for non-int chunk_size")
	}
	if !errors.Is(err, ErrInvalidYAML) {
		t.Errorf("missing ErrInvalidYAML in chain: %v", err)
	}
	var typeErr *yaml.TypeError
	if !errors.As(err, &typeErr) {
		t.Errorf("expected yaml.TypeError reachable via errors.As, got %v", err)
	}
}

func TestLoadEdgeInfoBadAlignedBy(t *testing.T) {
	t.Parallel()
	in := `src_type: s
edge_type: k
dst_type: d
chunk_size: 1
src_chunk_size: 1
dst_chunk_size: 1
directed: false
adj_lists:
  - ordered: true
    aligned_by: nowhere
    file_type: parquet
version: gar/v1
`
	_, err := LoadEdgeInfo(strings.NewReader(in))
	if !errors.Is(err, types.ErrInvalidAdjListType) {
		t.Errorf("expected ErrInvalidAdjListType, got %v", err)
	}
}

func TestLoadEdgeInfoBadAdjListFileType(t *testing.T) {
	t.Parallel()
	in := `src_type: s
edge_type: k
dst_type: d
chunk_size: 1
src_chunk_size: 1
dst_chunk_size: 1
directed: false
adj_lists:
  - type: ordered_by_source
    file_type: tsv
version: gar/v1
`
	_, err := LoadEdgeInfo(strings.NewReader(in))
	if !errors.Is(err, types.ErrInvalidFileType) {
		t.Errorf("expected ErrInvalidFileType, got %v", err)
	}
}

func TestLoadEdgeInfoBadAdjListType(t *testing.T) {
	t.Parallel()
	in := `src_type: s
edge_type: k
dst_type: d
chunk_size: 1
src_chunk_size: 1
dst_chunk_size: 1
directed: false
adj_lists:
  - type: csr
    file_type: parquet
version: gar/v1
`
	_, err := LoadEdgeInfo(strings.NewReader(in))
	if !errors.Is(err, types.ErrInvalidAdjListType) {
		t.Errorf("expected ErrInvalidAdjListType, got %v", err)
	}
}

func TestLoadEdgeInfoUnknownDataTypeAsUserDefined(t *testing.T) {
	t.Parallel()
	in := `src_type: s
edge_type: k
dst_type: d
chunk_size: 1
src_chunk_size: 1
dst_chunk_size: 1
directed: false
adj_lists:
  - type: ordered_by_source
    file_type: parquet
property_groups:
  - file_type: parquet
    properties:
      - name: weight
        data_type: bogus
        is_primary: false
version: gar/v1
`
	// Liberal read: "bogus" loads as a user-defined type.
	e, err := LoadEdgeInfo(strings.NewReader(in))
	if err != nil {
		t.Fatalf("LoadEdgeInfo: %v", err)
	}
	got, ok := e.PropertyGroups().PropertyType("weight")
	if !ok || !got.Equal(types.UserDefined("bogus")) {
		t.Errorf("PropertyType(weight) = %v ok=%v, want UserDefined(bogus)", got, ok)
	}
}

func TestLoadEdgeInfoBadVersion(t *testing.T) {
	t.Parallel()
	in := `src_type: s
edge_type: k
dst_type: d
chunk_size: 1
src_chunk_size: 1
dst_chunk_size: 1
directed: false
adj_lists:
  - type: ordered_by_source
    file_type: parquet
version: not-a-version
`
	_, err := LoadEdgeInfo(strings.NewReader(in))
	if !errors.Is(err, types.ErrInvalidVersion) {
		t.Errorf("expected ErrInvalidVersion, got %v", err)
	}
}

func TestLoadEdgeInfoMalformedYAML(t *testing.T) {
	t.Parallel()
	_, err := LoadEdgeInfo(strings.NewReader(":\n  -\n}"))
	if !errors.Is(err, ErrInvalidYAML) {
		t.Errorf("expected ErrInvalidYAML, got %v", err)
	}
}

func TestPropertyToYAMLEmitsNonDefaultCardinality(t *testing.T) {
	t.Parallel()
	in := `type: p
chunk_size: 1
property_groups:
  - file_type: parquet
    properties:
      - name: id
        data_type: int64
        is_primary: true
      - name: tags
        data_type: string
        is_primary: false
        cardinality: list
version: gar/v1
`
	v, err := LoadVertexInfo(strings.NewReader(in))
	if err != nil {
		t.Fatalf("LoadVertexInfo: %v", err)
	}
	out, err := MarshalVertexInfo(v)
	if err != nil {
		t.Fatalf("MarshalVertexInfo: %v", err)
	}
	s := string(out)
	if !strings.Contains(s, "cardinality: list") {
		t.Errorf("non-default cardinality was not emitted:\n%s", s)
	}
}

func TestLoadVertexInfoLabelFallback(t *testing.T) {
	t.Parallel()
	// Older fixtures use "label" instead of "type"; load.go must
	// accept the alias to preserve cross-language interop.
	in := `label: person
chunk_size: 100
property_groups:
  - properties:
      - name: id
        data_type: int64
        is_primary: true
    file_type: parquet
version: gar/v1
`
	v, err := LoadVertexInfo(strings.NewReader(in))
	if err != nil {
		t.Fatalf("LoadVertexInfo with label alias: %v", err)
	}
	if v.Type() != "person" {
		t.Errorf("label alias not honoured: got %q", v.Type())
	}
}

func TestLoadEdgeInfoTripletLabelFallback(t *testing.T) {
	t.Parallel()
	in := `src_label: person
edge_label: knows
dst_label: person
chunk_size: 1
src_chunk_size: 1
dst_chunk_size: 1
directed: false
adj_lists:
  - type: ordered_by_source
    file_type: parquet
version: gar/v1
`
	e, err := LoadEdgeInfo(strings.NewReader(in))
	if err != nil {
		t.Fatalf("LoadEdgeInfo with *_label alias: %v", err)
	}
	src, edge, dst := e.Triplet()
	if src != "person" || edge != "knows" || dst != "person" {
		t.Errorf("triplet from labels = %s/%s/%s", src, edge, dst)
	}
}

func TestLoadEdgeInfoIgnoresNestedPropertyGroups(t *testing.T) {
	t.Parallel()
	// Some older fixtures (ldbc, nebula) attach a property_groups subtree
	// to each adj_list entry. It is ignored on read: the per-layout subtree
	// must not surface in the domain model.
	in := `src_type: s
edge_type: k
dst_type: d
chunk_size: 1
src_chunk_size: 1
dst_chunk_size: 1
directed: false
adj_lists:
  - type: ordered_by_source
    file_type: parquet
    property_groups:
      - file_type: parquet
        properties:
          - name: w
            data_type: int64
            is_primary: false
version: gar/v1
`
	e, err := LoadEdgeInfo(strings.NewReader(in))
	if err != nil {
		t.Fatalf("LoadEdgeInfo with nested property_groups: %v", err)
	}
	if e.PropertyGroups() != nil {
		t.Errorf("nested property_groups leaked into domain model")
	}
}

func TestEdgeInfoToYAMLEmitsPropertyGroups(t *testing.T) {
	t.Parallel()
	in := `src_type: s
edge_type: k
dst_type: d
chunk_size: 1
src_chunk_size: 1
dst_chunk_size: 1
directed: false
adj_lists:
  - type: ordered_by_source
    file_type: parquet
property_groups:
  - file_type: parquet
    properties:
      - name: weight
        data_type: double
        is_primary: false
version: gar/v1
`
	e, err := LoadEdgeInfo(strings.NewReader(in))
	if err != nil {
		t.Fatalf("LoadEdgeInfo: %v", err)
	}
	out, err := MarshalEdgeInfo(e)
	if err != nil {
		t.Fatalf("MarshalEdgeInfo: %v", err)
	}
	s := string(out)
	for _, want := range []string{"property_groups:", "name: weight", "data_type: double"} {
		if !strings.Contains(s, want) {
			t.Errorf("MarshalEdgeInfo missing %q\n%s", want, s)
		}
	}
}

// TestIsNullableTriState covers the liberal-read contract: absent is_nullable
// on a non-primary defaults to true; an explicit is_nullable:true on a primary
// is preserved verbatim on load (the readers do not coerce); and Validate -
// not Load - is what flags the contradiction.
func TestIsNullableTriState(t *testing.T) {
	t.Parallel()
	in := `type: person
chunk_size: 100
property_groups:
  - file_type: parquet
    properties:
      - name: id
        data_type: int64
        is_primary: true
        is_nullable: true
      - name: firstName
        data_type: string
        is_primary: false
version: gar/v1
`
	v, err := LoadVertexInfo(strings.NewReader(in))
	if err != nil {
		t.Fatalf("LoadVertexInfo: %v", err)
	}
	pg := v.PropertyGroups()
	if !pg.IsNullable("firstName") {
		t.Error("absent is_nullable on non-primary must default to true")
	}
	if !pg.IsNullable("id") {
		t.Error("explicit is_nullable:true on a primary must be preserved on load")
	}
	// Load is liberal; Validate is where the primary+nullable contradiction
	// surfaces.
	if err := v.Validate(); !errors.Is(err, ErrInvalidProperty) {
		t.Errorf("Validate should reject primary+nullable, got %v", err)
	}
}

// TestLoadVertexInfoLiberal covers the liberal-read contract at the vertex
// level: a chunk_size<=0 document LOADS but fails Validate, while a
// zero-property-group vertex both loads and validates (readers accept it;
// Go used to reject it at load).
func TestLoadVertexInfoLiberal(t *testing.T) {
	t.Parallel()
	bad := `type: person
chunk_size: 0
property_groups:
  - file_type: parquet
    properties:
      - name: id
        data_type: int64
        is_primary: true
version: gar/v1
`
	v, err := LoadVertexInfo(strings.NewReader(bad))
	if err != nil {
		t.Fatalf("liberal load should accept chunk_size:0, got %v", err)
	}
	if err := v.Validate(); !errors.Is(err, ErrInvalidVertexInfo) {
		t.Errorf("Validate should reject chunk_size:0, got %v", err)
	}

	empty := `type: person
chunk_size: 100
version: gar/v1
`
	v2, err := LoadVertexInfo(strings.NewReader(empty))
	if err != nil {
		t.Fatalf("liberal load should accept zero property groups, got %v", err)
	}
	if err := v2.Validate(); err != nil {
		t.Errorf("zero property groups should validate, got %v", err)
	}
}

// TestIsNullableDefaultsNotEmitted asserts byte-stable, divergence-only write:
// a file where every property sits at its default nullability round-trips
// without re-introducing an is_nullable field.
func TestIsNullableDefaultsNotEmitted(t *testing.T) {
	t.Parallel()
	in := `type: person
chunk_size: 100
property_groups:
  - file_type: parquet
    properties:
      - name: id
        data_type: int64
        is_primary: true
      - name: firstName
        data_type: string
        is_primary: false
version: gar/v1
`
	v, err := LoadVertexInfo(strings.NewReader(in))
	if err != nil {
		t.Fatalf("LoadVertexInfo: %v", err)
	}
	out, err := MarshalVertexInfo(v)
	if err != nil {
		t.Fatalf("MarshalVertexInfo: %v", err)
	}
	if strings.Contains(string(out), "is_nullable") {
		t.Errorf("default nullability should not be emitted:\n%s", out)
	}
}

// TestIsNullableFalseNonPrimaryEmitted asserts an explicit non-default
// is_nullable:false on a non-primary property survives and is re-emitted.
func TestIsNullableFalseNonPrimaryEmitted(t *testing.T) {
	t.Parallel()
	in := `type: person
chunk_size: 100
property_groups:
  - file_type: parquet
    properties:
      - name: id
        data_type: int64
        is_primary: true
      - name: score
        data_type: int64
        is_primary: false
        is_nullable: false
version: gar/v1
`
	v, err := LoadVertexInfo(strings.NewReader(in))
	if err != nil {
		t.Fatalf("LoadVertexInfo: %v", err)
	}
	if v.PropertyGroups().IsNullable("score") {
		t.Error("explicit is_nullable:false must be preserved")
	}
	out, err := MarshalVertexInfo(v)
	if err != nil {
		t.Fatalf("MarshalVertexInfo: %v", err)
	}
	if !strings.Contains(string(out), "is_nullable: false") {
		t.Errorf("non-default is_nullable:false must be emitted:\n%s", out)
	}
}

// TestAdjacentListMarshalUsesOrderedAlignedBy guards the adjacency-list write
// format: adj_list must be written in the (ordered, aligned_by) form
// that the readers accept - never the `type:` spelling, which those
// readers ignore (silently mis-decoding every adj_list to unordered_by_dest).
// Also serves as a full edge round-trip.
func TestAdjacentListMarshalUsesOrderedAlignedBy(t *testing.T) {
	t.Parallel()
	in := `src_type: person
edge_type: knows
dst_type: person
chunk_size: 1024
src_chunk_size: 100
dst_chunk_size: 100
directed: false
prefix: edge/person_knows_person/
adj_lists:
  - ordered: true
    aligned_by: src
    file_type: parquet
  - ordered: true
    aligned_by: dst
    file_type: parquet
property_groups:
  - file_type: parquet
    properties:
      - name: since
        data_type: int64
        is_primary: false
version: gar/v1
`
	e, err := LoadEdgeInfo(strings.NewReader(in))
	if err != nil {
		t.Fatalf("LoadEdgeInfo: %v", err)
	}
	out, err := MarshalEdgeInfo(e)
	if err != nil {
		t.Fatalf("MarshalEdgeInfo: %v", err)
	}
	s := string(out)
	if n := strings.Count(s, "aligned_by:"); n != 2 {
		t.Errorf("expected 2 aligned_by entries (expected form), got %d:\n%s", n, s)
	}
	if !strings.Contains(s, "ordered:") {
		t.Errorf("marshalled adj_list missing `ordered:`:\n%s", s)
	}
	if strings.Contains(s, "type: ordered_by") {
		t.Errorf("adj_list emitted the `type:` form readers ignore:\n%s", s)
	}
	// Full round-trip: re-load must preserve adjacency types and properties.
	e2, err := LoadEdgeInfo(strings.NewReader(s))
	if err != nil {
		t.Fatalf("re-LoadEdgeInfo: %v", err)
	}
	got, want := e2.AdjListTypes(), e.AdjListTypes()
	if len(got) != len(want) {
		t.Fatalf("adj list count changed after round-trip: %v vs %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("adj list type[%d] = %v, want %v", i, got[i], want[i])
		}
	}
	if pt, ok := e2.PropertyGroups().PropertyType("since"); !ok || !pt.Equal(types.Int64()) {
		t.Errorf("property `since` not preserved: (%v, %v)", pt, ok)
	}
	if e2.ChunkSize() != e.ChunkSize() || e2.Directed() != e.Directed() {
		t.Errorf("edge scalar fields changed after round-trip")
	}
}
