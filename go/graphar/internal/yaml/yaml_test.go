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

package yaml

import (
	"strings"
	"testing"
)

func TestDecodeGraph(t *testing.T) {
	yamlStr := `
name: test_graph
prefix: /tmp/
vertices:
  - v1.yml
edges:
  - e1.yml
version: 0.1.0
`
	g, err := DecodeGraph(strings.NewReader(yamlStr))
	if err != nil {
		t.Fatalf("DecodeGraph failed: %v", err)
	}
	if g.Name != "test_graph" || len(g.Vertices) != 1 {
		t.Errorf("unexpected graph data: %+v", g)
	}
}

func TestDecodeVertex(t *testing.T) {
	yamlStr := "type: person\nchunk_size: 100"
	var v struct {
		Type      string `yaml:"type"`
		ChunkSize int    `yaml:"chunk_size"`
	}
	err := DecodeVertex(strings.NewReader(yamlStr), &v)
	if err != nil {
		t.Fatalf("DecodeVertex failed: %v", err)
	}
	if v.Type != "person" || v.ChunkSize != 100 {
		t.Errorf("unexpected vertex data: %+v", v)
	}
}

func TestDecodeEdge(t *testing.T) {
	yamlStr := "edge_type: knows\ndirected: true"
	var e struct {
		EdgeType string `yaml:"edge_type"`
		Directed bool   `yaml:"directed"`
	}
	err := DecodeEdge(strings.NewReader(yamlStr), &e)
	if err != nil {
		t.Fatalf("DecodeEdge failed: %v", err)
	}
	if e.EdgeType != "knows" || !e.Directed {
		t.Errorf("unexpected edge data: %+v", e)
	}
}
