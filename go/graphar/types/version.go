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
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// DefaultVersion is the version assumed for Info objects that do not specify
// one explicitly. It is the on-disk default for the GraphAr format.
const DefaultVersion = 1

// versionPrefix is the on-disk marker every version string starts with. Shared
// by String (write) and ParseInfoVersion (read) so the two never drift.
const versionPrefix = "gar/v"

// InfoVersion is the on-disk schema version of an Info document.
//
// The string form is "gar/vN" (e.g. "gar/v1"). An optional user-defined type
// list may follow in parentheses: "gar/v1 (foo,bar)". UserDefinedTypes are
// recorded in declaration order and duplicates are kept; surrounding
// whitespace on each entry is trimmed on parse (so "gar/v1 (a, b)" round-trips
// as "gar/v1 (a,b)").
type InfoVersion struct {
	Version          int
	UserDefinedTypes []string
}

// NewInfoVersion returns an InfoVersion with the given version and user-defined
// type names (copied defensively). A non-positive version is clamped to
// DefaultVersion; use TryNewInfoVersion to get an error instead.
func NewInfoVersion(version int, userDefinedTypes ...string) InfoVersion {
	if version <= 0 {
		version = DefaultVersion
	}
	if len(userDefinedTypes) == 0 {
		return InfoVersion{Version: version}
	}
	return InfoVersion{Version: version, UserDefinedTypes: slices.Clone(userDefinedTypes)}
}

// TryNewInfoVersion is like NewInfoVersion but returns an error wrapping
// ErrInvalidVersion for non-positive versions instead of clamping.
func TryNewInfoVersion(version int, userDefinedTypes ...string) (InfoVersion, error) {
	if version <= 0 {
		return InfoVersion{}, fmt.Errorf("%w: version must be positive, got %d", ErrInvalidVersion, version)
	}
	return NewInfoVersion(version, userDefinedTypes...), nil
}

// String returns the on-disk spelling.
func (v InfoVersion) String() string {
	if len(v.UserDefinedTypes) == 0 {
		return fmt.Sprintf("%s%d", versionPrefix, v.Version)
	}
	return fmt.Sprintf("%s%d (%s)", versionPrefix, v.Version, strings.Join(v.UserDefinedTypes, ","))
}

// Clone returns a deep copy of v, independent of the source's UserDefinedTypes
// slice; getters that hand out a stored InfoVersion should return v.Clone().
func (v InfoVersion) Clone() InfoVersion {
	if len(v.UserDefinedTypes) == 0 {
		return InfoVersion{Version: v.Version}
	}
	return InfoVersion{Version: v.Version, UserDefinedTypes: slices.Clone(v.UserDefinedTypes)}
}

// Validate reports whether v is well-formed: a positive version number and
// user-defined type names that round-trip through String / ParseInfoVersion.
func (v InfoVersion) Validate() error {
	if v.Version <= 0 {
		return fmt.Errorf("%w: version must be positive, got %d", ErrInvalidVersion, v.Version)
	}
	for _, name := range v.UserDefinedTypes {
		if err := validateTypeName(name); err != nil {
			return fmt.Errorf("%w: %s", ErrInvalidVersion, err)
		}
	}
	return nil
}

// validateTypeName reports why name cannot survive a String / ParseInfoVersion
// round trip: it must be non-empty, carry no surrounding whitespace and avoid
// the delimiters the version string and list syntax rely on (, ( ) < >).
func validateTypeName(name string) error {
	switch {
	case name == "":
		return errors.New("user-defined type name must not be empty")
	case strings.TrimSpace(name) != name:
		return fmt.Errorf("user-defined type name %q has leading or trailing whitespace", name)
	case strings.ContainsAny(name, ",()<>"):
		return fmt.Errorf("user-defined type name %q must not contain any of , ( ) < >", name)
	}
	return nil
}

// IsZero reports whether v is the zero InfoVersion.
func (v InfoVersion) IsZero() bool {
	return v.Version == 0 && len(v.UserDefinedTypes) == 0
}

// Equal reports whether two InfoVersions are equivalent.
func (v InfoVersion) Equal(other InfoVersion) bool {
	return v.Version == other.Version &&
		slices.Equal(v.UserDefinedTypes, other.UserDefinedTypes)
}

// ParseInfoVersion parses a version string in either the bare form "gar/vN" or
// the user-defined-types form "gar/vN (t1,t2,...)". An empty input is rejected.
func ParseInfoVersion(s string) (InfoVersion, error) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return InfoVersion{}, fmt.Errorf("%w: empty version string", ErrInvalidVersion)
	}
	if !strings.HasPrefix(trimmed, versionPrefix) {
		return InfoVersion{}, fmt.Errorf("%w: must start with %q, got %q", ErrInvalidVersion, versionPrefix, s)
	}
	rest := trimmed[len(versionPrefix):]

	// Split numeric prefix from optional "(types)" suffix.
	var (
		numStr   string
		typesStr string
	)
	if i := strings.IndexByte(rest, '('); i >= 0 {
		numStr = strings.TrimSpace(rest[:i])
		// Must end with ')'.
		if !strings.HasSuffix(rest, ")") {
			return InfoVersion{}, fmt.Errorf("%w: unterminated %q in %q", ErrInvalidVersion, "(", s)
		}
		typesStr = rest[i+1 : len(rest)-1]
	} else {
		numStr = rest
	}

	n, err := strconv.Atoi(numStr)
	if err != nil || n <= 0 {
		return InfoVersion{}, fmt.Errorf("%w: bad version number in %q", ErrInvalidVersion, s)
	}

	out := InfoVersion{Version: n}
	if typesStr != "" {
		parts := strings.Split(typesStr, ",")
		out.UserDefinedTypes = make([]string, 0, len(parts))
		for _, p := range parts {
			t := strings.TrimSpace(p)
			if t == "" {
				return InfoVersion{}, fmt.Errorf("%w: empty user-defined type entry in %q", ErrInvalidVersion, s)
			}
			out.UserDefinedTypes = append(out.UserDefinedTypes, t)
		}
	}
	return out, nil
}
