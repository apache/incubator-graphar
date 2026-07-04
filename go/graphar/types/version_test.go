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

package types

import (
	"errors"
	"testing"
)

func TestNewInfoVersion(t *testing.T) {
	t.Parallel()
	v := NewInfoVersion(1)
	if v.Version != 1 || len(v.UserDefinedTypes) != 0 {
		t.Errorf("NewInfoVersion(1) = %+v", v)
	}
	v2 := NewInfoVersion(2, "geometry", "point")
	if v2.Version != 2 || len(v2.UserDefinedTypes) != 2 ||
		v2.UserDefinedTypes[0] != "geometry" || v2.UserDefinedTypes[1] != "point" {
		t.Errorf("NewInfoVersion(2, ...) = %+v", v2)
	}
	// Mutating the input slice must not affect the stored value.
	orig := []string{"geometry"}
	v3 := NewInfoVersion(3, orig...)
	orig[0] = "mutated"
	if v3.UserDefinedTypes[0] != "geometry" {
		t.Errorf("UserDefinedTypes was not defensively copied")
	}
}

func TestInfoVersionString(t *testing.T) {
	t.Parallel()
	cases := []struct {
		v    InfoVersion
		want string
	}{
		{NewInfoVersion(1), "gar/v1"},
		{NewInfoVersion(2, "geometry"), "gar/v2 (geometry)"},
		{NewInfoVersion(3, "geometry", "point"), "gar/v3 (geometry,point)"},
	}
	for _, c := range cases {
		if got := c.v.String(); got != c.want {
			t.Errorf("String() = %q, want %q", got, c.want)
		}
	}
}

func TestInfoVersionEqualAndZero(t *testing.T) {
	t.Parallel()
	if !(InfoVersion{}).IsZero() {
		t.Error("zero InfoVersion should report IsZero")
	}
	if NewInfoVersion(1).IsZero() {
		t.Error("v1 should not report IsZero")
	}
	a := NewInfoVersion(1, "g")
	b := NewInfoVersion(1, "g")
	if !a.Equal(b) {
		t.Error("identical InfoVersions not equal")
	}
	if a.Equal(NewInfoVersion(2, "g")) {
		t.Error("different version reported equal")
	}
	if a.Equal(NewInfoVersion(1)) {
		t.Error("different type lists reported equal")
	}
	if a.Equal(NewInfoVersion(1, "h")) {
		t.Error("different type names reported equal")
	}
}

func TestParseInfoVersionOK(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want InfoVersion
	}{
		{"gar/v1", NewInfoVersion(1)},
		{"  gar/v2  ", NewInfoVersion(2)},
		{"gar/v3 (geometry)", NewInfoVersion(3, "geometry")},
		{"gar/v4 (geometry, point)", NewInfoVersion(4, "geometry", "point")},
		{"gar/v5(a,b,c)", NewInfoVersion(5, "a", "b", "c")},
	}
	for _, c := range cases {
		got, err := ParseInfoVersion(c.in)
		if err != nil {
			t.Errorf("ParseInfoVersion(%q): %v", c.in, err)
			continue
		}
		if !got.Equal(c.want) {
			t.Errorf("ParseInfoVersion(%q) = %+v, want %+v", c.in, got, c.want)
		}
	}
}

func TestParseInfoVersionErrors(t *testing.T) {
	t.Parallel()
	cases := []string{
		"",
		"   ",
		"gar/v",
		"gar/v0",        // version must be > 0
		"gar/v-1",       // negative
		"gar/vabc",      // not numeric
		"gar/v1 (",      // unterminated
		"gar/v1 (a,,b)", // empty element
		"foo/v1",        // wrong prefix
		"v1",
	}
	for _, in := range cases {
		_, err := ParseInfoVersion(in)
		if err == nil {
			t.Errorf("ParseInfoVersion(%q) succeeded, want error", in)
			continue
		}
		if !errors.Is(err, ErrInvalidVersion) {
			t.Errorf("ParseInfoVersion(%q) error %v not wrapped", in, err)
		}
	}
}

func TestParseInfoVersionRoundTrip(t *testing.T) {
	t.Parallel()
	for _, in := range []string{"gar/v1", "gar/v2 (geometry,point)"} {
		v, err := ParseInfoVersion(in)
		if err != nil {
			t.Fatalf("ParseInfoVersion(%q): %v", in, err)
		}
		if v.String() != in {
			t.Errorf("round-trip failed: ParseInfoVersion(%q).String() = %q", in, v.String())
		}
	}
}

func TestDefaultVersionConstant(t *testing.T) {
	t.Parallel()
	if DefaultVersion != 1 {
		t.Errorf("DefaultVersion changed to %d; cross-language fixtures still ship gar/v1", DefaultVersion)
	}
}

func TestNewInfoVersionNonPositiveClamps(t *testing.T) {
	t.Parallel()
	// NewInfoVersion must keep the result round-trippable: a 0 or negative
	// argument is clamped to DefaultVersion so MarshalInfoVersion/ParseInfoVersion
	// are inverses on every produced value.
	for _, v := range []int{0, -1, -1000} {
		got := NewInfoVersion(v)
		if got.Version != DefaultVersion {
			t.Errorf("NewInfoVersion(%d) = %d, want clamped to %d", v, got.Version, DefaultVersion)
		}
		// Verify round-trip.
		parsed, err := ParseInfoVersion(got.String())
		if err != nil {
			t.Errorf("clamped NewInfoVersion(%d) fails round-trip: %v", v, err)
		}
		if !parsed.Equal(got) {
			t.Errorf("round-trip mismatch for NewInfoVersion(%d): %v vs %v", v, parsed, got)
		}
	}
}

func TestTryNewInfoVersion(t *testing.T) {
	t.Parallel()
	v, err := TryNewInfoVersion(2, "geometry")
	if err != nil {
		t.Fatalf("TryNewInfoVersion(2,...): %v", err)
	}
	if v.Version != 2 || len(v.UserDefinedTypes) != 1 {
		t.Errorf("got %+v", v)
	}
	for _, bad := range []int{0, -1} {
		if _, err := TryNewInfoVersion(bad); !errors.Is(err, ErrInvalidVersion) {
			t.Errorf("TryNewInfoVersion(%d): expected ErrInvalidVersion, got %v", bad, err)
		}
	}
}

// TestInfoVersionCloneIndependent guards the immutability contract: Clone must
// return a version whose UserDefinedTypes slice is independent of the source,
// so a value handed out by an Info's Version() getter cannot mutate stored
// state.
func TestInfoVersionCloneIndependent(t *testing.T) {
	t.Parallel()
	orig := NewInfoVersion(1, "geometry", "point")
	clone := orig.Clone()
	if !clone.Equal(orig) {
		t.Fatalf("Clone not equal to source: %+v vs %+v", clone, orig)
	}
	clone.UserDefinedTypes[0] = "hacked"
	if orig.UserDefinedTypes[0] != "geometry" {
		t.Errorf("mutating clone leaked into source: %v", orig.UserDefinedTypes)
	}
	// Empty case must not panic and stays independent.
	if got := NewInfoVersion(2).Clone(); len(got.UserDefinedTypes) != 0 || got.Version != 2 {
		t.Errorf("empty clone wrong: %+v", got)
	}
}

func TestInfoVersionValidate(t *testing.T) {
	t.Parallel()
	valid := []InfoVersion{
		NewInfoVersion(1),
		NewInfoVersion(2, "geometry", "point"),
	}
	for _, v := range valid {
		if err := v.Validate(); err != nil {
			t.Errorf("Validate(%q) = %v, want nil", v, err)
		}
	}

	invalid := []struct {
		name string
		v    InfoVersion
	}{
		{"zero version", InfoVersion{Version: 0}},
		{"negative version", InfoVersion{Version: -1}},
		{"name with comma", InfoVersion{Version: 1, UserDefinedTypes: []string{"my,type"}}},
		{"name with surrounding space", InfoVersion{Version: 1, UserDefinedTypes: []string{" geometry "}}},
		{"empty name", InfoVersion{Version: 1, UserDefinedTypes: []string{""}}},
	}
	for _, c := range invalid {
		if err := c.v.Validate(); !errors.Is(err, ErrInvalidVersion) {
			t.Errorf("Validate(%s) = %v, want ErrInvalidVersion", c.name, err)
		}
	}
}

// TestInfoVersionCommaNameRejected pins the round-trip hazard: a name with a
// comma would split into two on ParseInfoVersion, so Validate must reject it
// before it reaches String.
func TestInfoVersionCommaNameRejected(t *testing.T) {
	t.Parallel()
	v := InfoVersion{Version: 1, UserDefinedTypes: []string{"a,b"}}
	if err := v.Validate(); !errors.Is(err, ErrInvalidVersion) {
		t.Fatalf("Validate = %v, want ErrInvalidVersion", err)
	}
	// A validated (single, clean) name must survive String -> ParseInfoVersion.
	ok := NewInfoVersion(1, "geometry")
	if err := ok.Validate(); err != nil {
		t.Fatalf("Validate = %v, want nil", err)
	}
	back, err := ParseInfoVersion(ok.String())
	if err != nil || !back.Equal(ok) {
		t.Errorf("round-trip %q -> %+v (err %v)", ok.String(), back, err)
	}
}
