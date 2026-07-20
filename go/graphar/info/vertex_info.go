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
	"fmt"
	"slices"

	"github.com/apache/incubator-graphar/go/graphar/types"
)

// VertexInfo is the immutable metadata for one vertex type.
type VertexInfo struct {
	typ       string
	chunkSize int64
	prefix    string
	labels    []string
	groups    *PropertyGroups
	version   types.InfoVersion
}

// VertexInfoOption configures a VertexInfo at construction time.
type VertexInfoOption func(*vertexInfoConfig)

type vertexInfoConfig struct {
	prefix  string
	labels  []string
	version types.InfoVersion
}

// WithVertexPrefix sets the on-disk prefix for the vertex type's files. When
// unset, the prefix defaults to "vertex/<type>/".
func WithVertexPrefix(p string) VertexInfoOption {
	return func(c *vertexInfoConfig) { c.prefix = p }
}

// WithVertexLabels attaches labels to the vertex type.
func WithVertexLabels(labels ...string) VertexInfoOption {
	return func(c *vertexInfoConfig) {
		c.labels = slices.Clone(labels)
	}
}

// WithVertexVersion overrides the InfoVersion (default: gar/v1).
func WithVertexVersion(v types.InfoVersion) VertexInfoOption {
	return func(c *vertexInfoConfig) { c.version = v.Clone() }
}

// NewVertexInfo constructs a VertexInfo and validates it. The type name and
// chunk size are mandatory; chunk size must be positive.
func NewVertexInfo(typ string, chunkSize int64, groups []PropertyGroup, opts ...VertexInfoOption) (*VertexInfo, error) {
	if len(groups) == 0 {
		return nil, fmt.Errorf("vertex_info(%q): %w", typ,
			newValidationError(ErrInvalidPropertyGroup, "property_groups",
				"at least one property group is required"))
	}
	v, err := newVertexInfoUnvalidated(typ, chunkSize, groups, opts...)
	if err != nil {
		return nil, err
	}
	if err := v.Validate(); err != nil {
		return nil, err
	}
	return v, nil
}

// newVertexInfoUnvalidated builds a VertexInfo without validating (the liberal
// load path): groups are indexed but not validated and may be empty. Call
// Validate to enforce the full contract.
func newVertexInfoUnvalidated(typ string, chunkSize int64, groups []PropertyGroup, opts ...VertexInfoOption) (*VertexInfo, error) {
	cfg := vertexInfoConfig{version: types.NewInfoVersion(types.DefaultVersion)}
	for _, opt := range opts {
		opt(&cfg)
	}
	pg, err := buildPropertyGroups(groups)
	if err != nil {
		return nil, fmt.Errorf("vertex_info(%q): %w", typ, err)
	}
	prefix := cfg.prefix
	if prefix == "" && typ != "" {
		prefix = "vertex/" + typ + "/"
	}
	return &VertexInfo{
		typ:       typ,
		chunkSize: chunkSize,
		prefix:    prefix,
		labels:    cfg.labels,
		groups:    pg,
		version:   cfg.version,
	}, nil
}

// Type returns the vertex type name.
func (v *VertexInfo) Type() string { return v.typ }

// ChunkSize returns the number of vertices per chunk.
func (v *VertexInfo) ChunkSize() int64 { return v.chunkSize }

// Prefix returns the on-disk prefix of vertex files.
func (v *VertexInfo) Prefix() string { return v.prefix }

// Labels returns a copy of the label list.
func (v *VertexInfo) Labels() []string {
	return slices.Clone(v.labels)
}

// PropertyGroups returns the property group index.
func (v *VertexInfo) PropertyGroups() *PropertyGroups { return v.groups }

// Version returns the InfoVersion.
func (v *VertexInfo) Version() types.InfoVersion { return v.version.Clone() }

// HasProperty is shorthand for v.PropertyGroups().Has(name).
func (v *VertexInfo) HasProperty(name string) bool {
	return v.groups != nil && v.groups.Has(name)
}

// PropertyType returns the data type of the named property.
func (v *VertexInfo) PropertyType(name string) (types.DataType, bool) {
	if v.groups == nil {
		return types.DataType{}, false
	}
	return v.groups.PropertyType(name)
}

// Validate enforces the cross-field invariants:
//   - non-empty type name
//   - chunk size > 0
//   - non-nil, valid property group index
//   - no duplicate labels
//
// The number of primary properties is not constrained, to stay compatible with
// fixtures emitted by the other SDKs; only unique property names are required,
// which NewPropertyGroups already enforces.
func (v *VertexInfo) Validate() error {
	if v.typ == "" {
		return newValidationError(ErrInvalidVertexInfo, "type", "must not be empty")
	}
	if v.chunkSize <= 0 {
		return newValidationError(ErrInvalidVertexInfo, "chunk_size",
			"must be > 0, got %d", v.chunkSize)
	}
	if err := v.version.Validate(); err != nil {
		return newValidationError(ErrInvalidVertexInfo, "version", "%s", err)
	}
	if v.groups == nil {
		return newValidationError(ErrInvalidVertexInfo, "property_groups",
			"must not be nil")
	}
	if err := v.groups.Validate(); err != nil {
		return err
	}
	if err := validateUserDefinedTypes(v.groups, v.version, ErrInvalidVertexInfo); err != nil {
		return err
	}
	return validateLabels(v.labels, ErrInvalidVertexInfo)
}

// validateLabels rejects empty and duplicate labels, tagging failures with the
// given sentinel. Shared by VertexInfo and GraphInfo.
func validateLabels(labels []string, sentinel error) error {
	seen := make(map[string]struct{}, len(labels))
	for _, l := range labels {
		if l == "" {
			return newValidationError(sentinel, "labels", "label must not be empty")
		}
		if _, dup := seen[l]; dup {
			return newValidationError(sentinel, "labels", "duplicate label %q", l)
		}
		seen[l] = struct{}{}
	}
	return nil
}

// AddPropertyGroup returns a new VertexInfo with the additional group
// appended. The receiver is unchanged.
func (v *VertexInfo) AddPropertyGroup(g PropertyGroup) (*VertexInfo, error) {
	current := v.groups.Groups()
	return NewVertexInfo(v.typ, v.chunkSize, append(current, g),
		WithVertexPrefix(v.prefix),
		WithVertexLabels(v.labels...),
		WithVertexVersion(v.version),
	)
}
