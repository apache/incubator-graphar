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
	"net/url"
	"strings"
	"testing"
)

func TestParseGraphInfoYAML(t *testing.T) {
	yamlStr := `
name: test_graph
prefix: /tmp/graphar/
vertices: []
edges: []
version: 0.1.0
`
	u, _ := url.Parse("/tmp/")
	_, err := parseGraphInfoYAML(strings.NewReader(yamlStr), u, "/tmp/", nil)
	if err != nil && !errors.Is(err, ErrUnsupported) {
		t.Errorf("expected ErrUnsupported for version 2.0.0, got %v", err)
	}
}

func TestSentinelErrors(t *testing.T) {
	t.Run("MissingField", func(t *testing.T) {
		yamlStr := `name: test` // missing version and prefix
		u, _ := url.Parse("/tmp/")
		_, err := parseGraphInfoYAML(strings.NewReader(yamlStr), u, "/tmp/", nil)
		if !errors.Is(err, ErrMissingField) {
			t.Errorf("expected ErrMissingField, got %v", err)
		}
	})

	t.Run("NotFound", func(t *testing.T) {
		yamlStr := `
name: test
prefix: /tmp/
version: 0.1.0
vertices: [non-existent.vertex.yaml]
`
		loader := func(path string) ([]byte, error) {
			return nil, ErrNotFound
		}
		u, _ := url.Parse("/tmp/")
		_, err := parseGraphInfoYAML(strings.NewReader(yamlStr), u, "/tmp/", loader)
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("expected ErrNotFound for missing vertex info, got %v", err)
		}
	})

	t.Run("Invalid", func(t *testing.T) {
		vi := &VertexInfo{Type: "person", ChunkSize: -1}
		err := vi.Validate()
		if !errors.Is(err, ErrInvalid) {
			t.Errorf("expected ErrInvalid for negative chunk size, got %v", err)
		}
	})
}

func TestParseInvalidYAML(t *testing.T) {
	invalidYaml := `
name: : invalid
`
	u, _ := url.Parse("/tmp/")
	_, err := parseGraphInfoYAML(strings.NewReader(invalidYaml), u, "/tmp/", nil)
	if err == nil {
		t.Errorf("expected error for invalid YAML")
	}
}
