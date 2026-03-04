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
	"path/filepath"
	"runtime"
	"testing"
)

func getRepoRoot() string {
	_, filename, _, _ := runtime.Caller(0)
	// current file: go/graphar/info/integration_test.go
	// go/graphar/info -> go/graphar -> go -> root
	return filepath.Join(filepath.Dir(filename), "..", "..", "..")
}

func TestLoadIntegrationData(t *testing.T) {
	root := getRepoRoot()

	t.Run("LoadLdbcSampleCSV", func(t *testing.T) {
		path := filepath.Join(root, "testing", "ldbc_sample", "csv", "ldbc_sample.graph.yml")
		g, err := LoadGraphInfo(path)
		if err != nil {
			t.Fatalf("failed to load ldbc_sample graph: %v", err)
		}
		if g.Name != "ldbc_sample" {
			t.Errorf("expected graph name ldbc_sample, got %s", g.Name)
		}
		if !g.HasVertexInfo("person") {
			t.Errorf("expected person vertex")
		}
		if !g.HasEdgeInfo("person", "knows", "person") {
			t.Errorf("expected person-knows-person edge")
		}
	})

	t.Run("LoadModernGraph", func(t *testing.T) {
		path := filepath.Join(root, "testing", "modern_graph", "modern_graph.graph.yml")
		g, err := LoadGraphInfo(path)
		if err != nil {
			t.Fatalf("failed to load modern_graph: %v", err)
		}
		if g.Name != "modern_graph" {
			t.Errorf("expected graph name modern_graph, got %s", g.Name)
		}
	})

	t.Run("LoadVertexInfoDirectly", func(t *testing.T) {
		path := filepath.Join(root, "testing", "ldbc_sample", "csv", "person.vertex.yml")
		v, err := LoadVertexInfo(path)
		if err != nil {
			t.Fatalf("failed to load vertex info: %v", err)
		}
		if v.Type != "person" {
			t.Errorf("expected person vertex type, got %s", v.Type)
		}
	})

	t.Run("LoadEdgeInfoDirectly", func(t *testing.T) {
		path := filepath.Join(root, "testing", "ldbc_sample", "csv", "person_knows_person.edge.yml")
		e, err := LoadEdgeInfo(path)
		if err != nil {
			t.Fatalf("failed to load edge info: %v", err)
		}
		if e.EdgeType != "knows" {
			t.Errorf("expected knows edge type, got %s", e.EdgeType)
		}
	})
}
