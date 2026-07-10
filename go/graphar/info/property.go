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
	"github.com/apache/incubator-graphar/go/graphar/types"
)

// Property is a single named field within a PropertyGroup.
//
// The zero value of Property is not legal (Name and Type are required); use
// NewProperty or populate the fields explicitly before passing to
// PropertyGroup.
type Property struct {
	// Name is the on-disk column name. Required, non-empty.
	Name string
	// Type is the logical data type.
	Type types.DataType
	// IsPrimary marks the property as part of the primary key. Primary
	// properties may not be nullable.
	IsPrimary bool
	// IsNullable indicates whether the property may be absent in a record.
	// The on-disk default (applied by the loader and by NewProperty) is
	// !IsPrimary - non-primary properties are nullable unless stated otherwise;
	// the zero value of a bare Property literal is false.
	IsNullable bool
	// Cardinality is the multiplicity of values. Defaults to CardinalitySingle.
	// Edge properties must remain CardinalitySingle.
	Cardinality types.Cardinality
}

// NewProperty constructs a Property with the on-disk defaults
// (IsNullable = !isPrimary, same as the loader; Cardinality = single) and
// validates it.
func NewProperty(name string, dt types.DataType, isPrimary bool) (Property, error) {
	p := Property{
		Name:        name,
		Type:        dt,
		IsPrimary:   isPrimary,
		IsNullable:  !isPrimary,
		Cardinality: types.CardinalitySingle,
	}
	if err := p.Validate(); err != nil {
		return Property{}, err
	}
	return p, nil
}

// Validate returns a ValidationError if p is malformed.
func (p Property) Validate() error {
	if p.Name == "" {
		return newValidationError(ErrInvalidProperty, "name", "must not be empty")
	}
	if p.Type.IsZero() {
		return newValidationError(ErrInvalidProperty, "data_type",
			"property %q has no data type set; use one of the types factory functions", p.Name)
	}
	if err := p.Type.Validate(); err != nil {
		ve := newValidationError(ErrInvalidProperty, "data_type",
			"property %q: %s", p.Name, err)
		ve.Cause = err
		return ve
	}
	if p.IsPrimary && p.IsNullable {
		return newValidationError(ErrInvalidProperty, "is_nullable",
			"primary property %q must not be nullable", p.Name)
	}
	// We do not constrain Cardinality here: edge-specific
	// restrictions live with EdgeInfo.Validate to keep this type reusable.
	return nil
}

// Equal reports whether two properties describe the same field.
func (p Property) Equal(other Property) bool {
	return p.Name == other.Name &&
		p.Type.Equal(other.Type) &&
		p.IsPrimary == other.IsPrimary &&
		p.IsNullable == other.IsNullable &&
		p.Cardinality == other.Cardinality
}
