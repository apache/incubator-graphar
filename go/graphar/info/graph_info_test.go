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
)

func TestGraphInfo(t *testing.T) {
	vi := &VertexInfo{
		Type:      "person",
		ChunkSize: 100,
		Prefix:    "person/",
		Version:   VersionInfo{Major: 0, Minor: 1, Patch: 0},
	}
	ei := &EdgeInfo{
		SrcType:      "person",
		EdgeType:     "knows",
		DstType:      "person",
		ChunkSize:    100,
		SrcChunkSize: 100,
		DstChunkSize: 100,
		Prefix:       "person_knows_person/",
		Adjacent: []*AdjacentList{
			{Type: AdjListOrderedBySource, FileType: FileTypeParquet},
		},
		Version: VersionInfo{Major: 0, Minor: 1, Patch: 0},
	}

	g := &GraphInfo{
		Name:        "test_graph",
		Prefix:      "/tmp/graphar/",
		VertexInfos: []*VertexInfo{vi},
		EdgeInfos:   []*EdgeInfo{ei},
		Version:     VersionInfo{Major: 0, Minor: 1, Patch: 0},
	}

	t.Run("init", func(t *testing.T) {
		if err := g.Init(); err != nil {
			t.Fatalf("init failed: %v", err)
		}
	})

	t.Run("validate", func(t *testing.T) {
		if err := g.Validate(); err != nil {
			t.Errorf("validate failed: %v", err)
		}

		invalidG := &GraphInfo{Name: ""}
		if err := invalidG.Validate(); err == nil {
			t.Errorf("expected error for empty Name")
		}
	})

	t.Run("Queries", func(t *testing.T) {
		if !g.HasVertexInfo("person") {
			t.Errorf("expected to have vertex person")
		}
		if g.HasVertexInfo("robot") {
			t.Errorf("did not expect to have vertex robot")
		}

		if !g.HasEdgeInfo("person", "knows", "person") {
			t.Errorf("expected to have edge person-knows-person")
		}

		vInfo, err := g.GetVertexInfo("person")
		if err != nil || vInfo.Type != "person" {
			t.Errorf("GetVertexInfo failed")
		}

		eInfo, err := g.GetEdgeInfo("person", "knows", "person")
		if err != nil || eInfo.EdgeType != "knows" {
			t.Errorf("GetEdgeInfo failed")
		}
	})
}
