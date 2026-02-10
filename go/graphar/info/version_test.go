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
	"testing"

	"gopkg.in/yaml.v3"
)

func TestParseVersion(t *testing.T) {
	tests := []struct {
		input    string
		expected VersionInfo
		wantErr  bool
	}{
		{"0.1.0", VersionInfo{0, 1, 0}, false},
		{"1.2.3", VersionInfo{1, 2, 3}, false},
		{"gar/v1", VersionInfo{1, 0, 0}, false},
		{"1.0", VersionInfo{}, true},
		{"1.2.3.4", VersionInfo{}, true},
		{"a.b.c", VersionInfo{}, true},
		{"1.a.0", VersionInfo{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseVersion(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseVersion() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.expected {
				t.Errorf("ParseVersion() got = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestVersionUnmarshalYAML(t *testing.T) {
	t.Run("string version", func(t *testing.T) {
		var v VersionInfo
		err := yaml.Unmarshal([]byte("0.1.0"), &v)
		if err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if v.Major != 0 || v.Minor != 1 || v.Patch != 0 {
			t.Errorf("expected 0.1.0, got %v", v)
		}
	})

	t.Run("struct version", func(t *testing.T) {
		var v VersionInfo
		err := yaml.Unmarshal([]byte("major: 1\nminor: 2\npatch: 3"), &v)
		if err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if v.Major != 1 || v.Minor != 2 || v.Patch != 3 {
			t.Errorf("expected 1.2.3, got %v", v)
		}
	})

	t.Run("gar/v1 version", func(t *testing.T) {
		var v VersionInfo
		err := yaml.Unmarshal([]byte("gar/v1"), &v)
		if err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if v.Major != 1 || v.Minor != 0 || v.Patch != 0 {
			t.Errorf("expected 1.0.0 for gar/v1, got %v", v)
		}
	})

	t.Run("invalid version", func(t *testing.T) {
		var v VersionInfo
		err := yaml.Unmarshal([]byte("invalid.version"), &v)
		if err == nil {
			t.Errorf("expected error for invalid version string")
		}
	})
}

func TestVersionString(t *testing.T) {
	v := VersionInfo{Major: 1, Minor: 2, Patch: 3}
	expected := "1.2.3"
	if got := v.String(); got != expected {
		t.Errorf("VersionInfo.String() = %v, want %v", got, expected)
	}
}
