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
	"fmt"
)

// Sentinel errors. Callers should test with errors.Is, not string match.
var (
	// ErrInvalidProperty is returned when a Property fails validation.
	ErrInvalidProperty = errors.New("graphar/info: invalid property")
	// ErrInvalidPropertyGroup is returned when a PropertyGroup fails validation.
	ErrInvalidPropertyGroup = errors.New("graphar/info: invalid property group")
	// ErrInvalidVertexInfo is returned when a VertexInfo fails validation.
	ErrInvalidVertexInfo = errors.New("graphar/info: invalid vertex info")
	// ErrInvalidEdgeInfo is returned when an EdgeInfo fails validation.
	ErrInvalidEdgeInfo = errors.New("graphar/info: invalid edge info")
	// ErrInvalidGraphInfo is returned when a GraphInfo fails validation.
	ErrInvalidGraphInfo = errors.New("graphar/info: invalid graph info")
	// ErrInvalidYAML is returned for malformed YAML input.
	ErrInvalidYAML = errors.New("graphar/info: invalid YAML")
)

// ValidationError adds a structured field path + reason on top of one of the
// sentinel errors above. errors.Is on a ValidationError reports true for the
// wrapped sentinel.
type ValidationError struct {
	// Field is a dotted path locating the offending field, e.g.
	// "vertex_info.property_groups[0].properties[2].name".
	Field string
	// Reason is the human-readable cause.
	Reason string
	// Sentinel is the underlying ErrInvalid... category.
	Sentinel error
	// Cause is an optional wrapped error kept in the chain so errors.Is matches
	// it too (e.g. a types.ErrInvalidDataType behind an invalid property).
	Cause error
}

func (e *ValidationError) Error() string {
	if e.Field == "" {
		return fmt.Sprintf("%s: %s", e.Sentinel.Error(), e.Reason)
	}
	return fmt.Sprintf("%s: %s: %s", e.Sentinel.Error(), e.Field, e.Reason)
}

// Unwrap exposes the sentinel (and any wrapped cause) so errors.Is matches
// both categories.
func (e *ValidationError) Unwrap() []error {
	if e.Cause == nil {
		return []error{e.Sentinel}
	}
	return []error{e.Sentinel, e.Cause}
}

func newValidationError(sentinel error, field, reasonFmt string, args ...any) *ValidationError {
	return &ValidationError{
		Field:    field,
		Reason:   fmt.Sprintf(reasonFmt, args...),
		Sentinel: sentinel,
	}
}
