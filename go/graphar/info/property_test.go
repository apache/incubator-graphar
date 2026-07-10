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

func TestNewPropertyDefaults(t *testing.T) {
	t.Parallel()
	// Primary: not nullable.
	p, err := NewProperty("id", types.Int64(), true)
	if err != nil {
		t.Fatalf("NewProperty: %v", err)
	}
	if p.Cardinality != types.CardinalitySingle {
		t.Errorf("default cardinality = %v, want single", p.Cardinality)
	}
	if p.IsNullable {
		t.Errorf("primary property default IsNullable should be false")
	}
	// Non-primary: nullable by default, matching the loader default.
	np, err := NewProperty("name", types.String(), false)
	if err != nil {
		t.Fatalf("NewProperty: %v", err)
	}
	if !np.IsNullable {
		t.Errorf("non-primary property default IsNullable should be true")
	}
}

func TestPropertyValidateRejectsEmptyName(t *testing.T) {
	t.Parallel()
	p := Property{Type: types.Int64()}
	err := p.Validate()
	if !errors.Is(err, ErrInvalidProperty) {
		t.Fatalf("expected ErrInvalidProperty, got %v", err)
	}
	var ve *ValidationError
	if !errors.As(err, &ve) || ve.Field != "name" {
		t.Errorf("expected field=name, got %+v", ve)
	}
}

func TestPropertyValidatePrimaryNotNullable(t *testing.T) {
	t.Parallel()
	p := Property{Name: "id", Type: types.Int64(), IsPrimary: true, IsNullable: true}
	err := p.Validate()
	if !errors.Is(err, ErrInvalidProperty) {
		t.Fatalf("expected ErrInvalidProperty, got %v", err)
	}
}

func TestPropertyEqual(t *testing.T) {
	t.Parallel()
	a := Property{Name: "x", Type: types.Int32(), Cardinality: types.CardinalitySingle}
	if !a.Equal(a) {
		t.Error("reflexive equality failed")
	}
	cases := []struct {
		mutate func(p *Property)
		label  string
	}{
		{func(p *Property) { p.Name = "y" }, "name"},
		{func(p *Property) { p.Type = types.Int64() }, "type"},
		{func(p *Property) { p.IsPrimary = true }, "primary"},
		{func(p *Property) { p.IsNullable = true }, "nullable"},
		{func(p *Property) { p.Cardinality = types.CardinalityList }, "cardinality"},
	}
	for _, c := range cases {
		b := a
		c.mutate(&b)
		if a.Equal(b) {
			t.Errorf("equal returned true after mutating %s", c.label)
		}
	}
}

func TestNewPropertyValidatesEmptyName(t *testing.T) {
	t.Parallel()
	_, err := NewProperty("", types.Int64(), false)
	if !errors.Is(err, ErrInvalidProperty) {
		t.Fatalf("expected ErrInvalidProperty, got %v", err)
	}
}

func TestPropertyValidateRejectsZeroType(t *testing.T) {
	t.Parallel()
	// A hand-constructed Property whose Type was never set must be rejected
	// rather than silently serialising as "bool".
	p := Property{Name: "id", IsPrimary: true}
	err := p.Validate()
	if !errors.Is(err, ErrInvalidProperty) {
		t.Fatalf("expected ErrInvalidProperty for unset type, got %v", err)
	}
}

func TestNewPropertyRejectsMalformedType(t *testing.T) {
	t.Parallel()
	bad := []struct {
		name string
		dt   types.DataType
	}{
		{"list of unset", types.List(types.DataType{})},
		{"list of bool", types.List(types.Bool())},
		{"list of date", types.List(types.Date())},
		{"nested list", types.List(types.List(types.Int32()))},
		{"empty user-defined", types.UserDefined("")},
	}
	for _, c := range bad {
		if _, err := NewProperty("x", c.dt, false); !errors.Is(err, ErrInvalidProperty) {
			t.Errorf("NewProperty(%s) = %v, want ErrInvalidProperty", c.name, err)
		}
	}
}

func TestNewPropertyAcceptsSupportedTypes(t *testing.T) {
	t.Parallel()
	ok := []types.DataType{
		types.List(types.Int32()), types.List(types.String()),
		types.UserDefined("geometry"),
	}
	for _, dt := range ok {
		if _, err := NewProperty("x", dt, false); err != nil {
			t.Errorf("NewProperty(%s) = %v, want nil", dt, err)
		}
	}
}

// TestPropertyValidateErrorChain guards that a bad data type surfaces both the
// property sentinel and the underlying data-type sentinel via errors.Is.
func TestPropertyValidateErrorChain(t *testing.T) {
	t.Parallel()
	p := Property{Name: "geo", Type: types.UserDefined("string")} // collides with builtin
	err := p.Validate()
	if err == nil {
		t.Fatal("expected error for colliding user-defined type")
	}
	if !errors.Is(err, ErrInvalidProperty) {
		t.Errorf("missing ErrInvalidProperty in chain: %v", err)
	}
	if !errors.Is(err, types.ErrInvalidDataType) {
		t.Errorf("missing types.ErrInvalidDataType in chain: %v", err)
	}
}
