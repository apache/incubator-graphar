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
	"path/filepath"
	"runtime"
	"testing"
)

func getRepoRoot() string {
	_, filename, _, _ := runtime.Caller(0)
	// current file: go/graphar/graphar_client_test.go
	return filepath.Join(filepath.Dir(filename), "..", "..")
}

func TestNewClient(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		c := New()
		if c == nil {
			t.Fatalf("expected non-nil client")
		}
	})

	t.Run("with options", func(t *testing.T) {
		called := false
		opt := func(c *client) {
			called = true
		}
		c := New(opt, nil)
		if c == nil {
			t.Fatalf("expected non-nil client")
		}
		if !called {
			t.Errorf("expected option to be called")
		}
	})
}

func TestClientLoadGraphInfoErrors(t *testing.T) {
	c := New()
	_, err := c.LoadGraphInfo(context.Background(), "non-existent-path.yml")
	if err == nil {
		t.Errorf("expected error for non-existent path")
	}
}

func TestClientLoadGraphInfo(t *testing.T) {
	root := getRepoRoot()
	graphPath := filepath.Join(root, "testing", "modern_graph", "modern_graph.graph.yml")

	c := New()
	g, err := c.LoadGraphInfo(context.Background(), graphPath)
	if err != nil {
		t.Fatalf("LoadGraphInfo failed: %v", err)
	}
	if g.Name != "modern_graph" {
		t.Fatalf("unexpected graph name: %s", g.Name)
	}
	if !g.HasVertex("person") {
		t.Fatalf("expected graph to have vertex person")
	}
	if !g.HasEdge("person", "knows", "person") {
		t.Fatalf("expected graph to have edge person-knows-person")
	}
}
