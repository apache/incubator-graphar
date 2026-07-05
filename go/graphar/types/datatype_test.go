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

func TestDataTypeStringPrimitives(t *testing.T) {
	t.Parallel()
	cases := []struct {
		dt   DataType
		want string
		id   TypeID
	}{
		{Bool(), "bool", BoolType},
		{Int32(), "int32", Int32Type},
		{Int64(), "int64", Int64Type},
		{Float(), "float", FloatType},
		{Double(), "double", DoubleType},
		{String(), "string", StringType},
		{Date(), "date", DateType},
		{Timestamp(), "timestamp", TimestampType},
	}
	for _, c := range cases {
		if got := c.dt.String(); got != c.want {
			t.Errorf("String() = %q, want %q", got, c.want)
		}
		if c.dt.ID() != c.id {
			t.Errorf("ID() = %d, want %d (for %s)", c.dt.ID(), c.id, c.want)
		}
		if _, ok := c.dt.ValueType(); ok {
			t.Errorf("ValueType ok=true for primitive %s", c.want)
		}
		if c.dt.UserDefinedName() != "" {
			t.Errorf("UserDefinedName non-empty for primitive %s", c.want)
		}
	}
}

func TestDataTypeListNested(t *testing.T) {
	t.Parallel()
	li := List(Int32())
	if got := li.String(); got != "list<int32>" {
		t.Fatalf("String() = %q, want list<int32>", got)
	}
	child, ok := li.ValueType()
	if !ok || !child.Equal(Int32()) {
		t.Fatalf("ValueType() = %v ok=%v, want int32", child, ok)
	}

	nested := List(List(String()))
	if got := nested.String(); got != "list<list<string>>" {
		t.Fatalf("nested = %q", got)
	}
	if !nested.Equal(List(List(String()))) {
		t.Fatalf("nested not equal to itself")
	}
}

func TestDataTypeListNilChild(t *testing.T) {
	t.Parallel()
	// A list constructed in-package without going through List() (valid=true
	// but child==nil) - defensive String form still degrades gracefully.
	d := DataType{id: ListType, valid: true}
	if got := d.String(); got != "list<unknown>" {
		t.Errorf("nil-child list String() = %q", got)
	}
	if _, ok := d.ValueType(); ok {
		t.Errorf("ValueType on nil-child list returned ok=true")
	}
}

func TestDataTypeUnknownID(t *testing.T) {
	t.Parallel()
	// A constructed-but-out-of-range ID (e.g. forward-compat decoding) must
	// not crash; it stringifies to "unknown".
	d := DataType{id: TypeID(0xff), valid: true}
	if got := d.String(); got != "unknown" {
		t.Errorf("unknown ID String() = %q", got)
	}
}

func TestDataTypeZero(t *testing.T) {
	t.Parallel()
	// The zero value is the "unset" sentinel: IsZero is true and String
	// returns "unset", which is not parseable by ParseDataType so an
	// accidentally-emitted zero value fails downstream.
	var d DataType
	if !d.IsZero() {
		t.Error("zero DataType IsZero should be true")
	}
	if got := d.String(); got != "unset" {
		t.Errorf("zero DataType String() = %q, want unset", got)
	}
	if _, err := ParseDataType(d.String()); err == nil {
		t.Error("ParseDataType(\"unset\") should return error")
	}
	// Any constructed DataType - including Bool() which shares the BoolType
	// underlying id - is not equal to the zero value.
	if d.Equal(Bool()) {
		t.Error("zero DataType should not equal Bool()")
	}
	if Bool().IsZero() {
		t.Error("Bool() must not report IsZero")
	}
}

func TestDataTypeUserDefined(t *testing.T) {
	t.Parallel()
	d := UserDefined("geometry")
	if d.String() != "geometry" {
		t.Errorf("UserDefined String = %q", d.String())
	}
	if d.UserDefinedName() != "geometry" {
		t.Errorf("UserDefinedName = %q", d.UserDefinedName())
	}
	if d.ID() != UserDefinedType {
		t.Errorf("ID = %d, want UserDefinedType", d.ID())
	}
	// Equality across user-defined depends on the name.
	if !d.Equal(UserDefined("geometry")) {
		t.Errorf("Equal failed for same user-defined name")
	}
	if d.Equal(UserDefined("polygon")) {
		t.Errorf("Equal succeeded for different user-defined names")
	}
}

func TestDataTypeValidate(t *testing.T) {
	t.Parallel()
	valid := []DataType{
		Bool(), Int32(), Int64(), Float(), Double(), String(), Date(), Timestamp(),
		List(Int32()), List(Int64()), List(Float()), List(Double()), List(String()),
		UserDefined("geometry"),
	}
	for _, d := range valid {
		if err := d.Validate(); err != nil {
			t.Errorf("Validate(%s) = %v, want nil", d, err)
		}
	}

	invalid := []struct {
		name string
		dt   DataType
	}{
		{"unset", DataType{}},
		{"list of bool", List(Bool())},
		{"list of date", List(Date())},
		{"list of timestamp", List(Timestamp())},
		{"nested list", List(List(Int32()))},
		{"list of unset", List(DataType{})},
		{"empty user-defined", UserDefined("")},
		{"user-defined collides with builtin", UserDefined("string")},
		{"user-defined collides with list", UserDefined("list<int32>")},
		{"user-defined with comma", UserDefined("my,type")},
		{"user-defined with angle bracket", UserDefined("vec<3>")},
		{"user-defined with surrounding space", UserDefined(" Vector ")},
	}
	for _, c := range invalid {
		if err := c.dt.Validate(); !errors.Is(err, ErrInvalidDataType) {
			t.Errorf("Validate(%s) = %v, want ErrInvalidDataType", c.name, err)
		}
	}
}

func TestDataTypeEqualMixedCategories(t *testing.T) {
	t.Parallel()
	if Int32().Equal(Int64()) {
		t.Error("Int32 should not equal Int64")
	}
	if List(Int32()).Equal(List(Int64())) {
		t.Error("list<int32> should not equal list<int64>")
	}
	if List(Int32()).Equal(Int32()) {
		t.Error("list<int32> should not equal int32")
	}
	// One list with nil child, the other with a real child: must not be equal.
	a := DataType{id: ListType} // nil child
	b := List(Int32())
	if a.Equal(b) || b.Equal(a) {
		t.Error("nil-child list equal to populated list")
	}
	// Both nil children: equal.
	c := DataType{id: ListType}
	if !a.Equal(c) {
		t.Error("two nil-child lists should be equal")
	}
}

func TestParseDataTypeOK(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want DataType
	}{
		{"bool", Bool()},
		{"int32", Int32()},
		{"int64", Int64()},
		{"float", Float()},
		{"double", Double()},
		{"string", String()},
		{"date", Date()},
		{"timestamp", Timestamp()},
		{"list<int32>", List(Int32())},
		{"list<int64>", List(Int64())},
		{"list<float>", List(Float())},
		{"list<double>", List(Double())},
		{"list<string>", List(String())},
		{"list<list<int32>>", List(List(Int32()))},
		// Surrounding whitespace tolerated.
		{"  int32  ", Int32()},
	}
	for _, c := range cases {
		got, err := ParseDataType(c.in)
		if err != nil {
			t.Errorf("ParseDataType(%q) error: %v", c.in, err)
			continue
		}
		if !got.Equal(c.want) {
			t.Errorf("ParseDataType(%q) = %v, want %v", c.in, got, c.want)
		}
		// Round-trip: simple primitives + list of primitives must String-round-trip.
		if s := got.String(); s != "list<list<int32>>" && c.in != "  int32  " {
			// Avoid testing whitespace round-trip on padded input.
			if _, err := ParseDataType(s); err != nil {
				t.Errorf("round-trip ParseDataType(String()) failed for %q: %v", c.in, err)
			}
		}
	}
}

func TestParseDataTypeErrors(t *testing.T) {
	t.Parallel()
	bad := []string{
		"",
		"  ",
		"int128",
		"List<int32>", // case-sensitive
		"list<>",
		"list<unknown>",
		"list<int32",
		"foo",
		"list<list<>>",
	}
	for _, in := range bad {
		_, err := ParseDataType(in)
		if err == nil {
			t.Errorf("ParseDataType(%q) succeeded, want error", in)
			continue
		}
		if !errors.Is(err, ErrInvalidDataType) {
			t.Errorf("ParseDataType(%q) error %v not wrapped by ErrInvalidDataType", in, err)
		}
	}
}
