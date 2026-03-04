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

package graphar

import (
	"context"
	"errors"
	"testing"
)

func TestNew(t *testing.T) {
	t.Parallel()

	c := New()
	if c == nil {
		t.Fatalf("New() returned nil")
	}
}

type mockFS struct {
	files map[string][]byte
}

func (m *mockFS) ReadFile(ctx context.Context, name string) ([]byte, error) {
	if b, ok := m.files[name]; ok {
		return b, nil
	}
	return nil, errors.New("file not found")
}

func TestClientWithFileSystem(t *testing.T) {
	fs := &mockFS{
		files: map[string][]byte{
			"graph.yaml": []byte(`
name: test_graph
prefix: /tmp/
vertices: [v.yaml]
edges: []
version: 0.1.0
`),
			"v.yaml": []byte(`
type: person
chunk_size: 100
prefix: person/
property_groups: []
version: 0.1.0
`),
		},
	}

	c := New(WithFileSystem(fs))
	g, err := c.LoadGraphInfo(context.Background(), "graph.yaml")
	if err != nil {
		t.Fatalf("LoadGraphInfo failed: %v", err)
	}

	if g.Name != "test_graph" {
		t.Errorf("expected name test_graph, got %s", g.Name)
	}
	if len(g.VertexInfos) != 1 {
		t.Errorf("expected 1 vertex info, got %d", len(g.VertexInfos))
	}
}
