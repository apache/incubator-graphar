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

import "testing"

func TestVertexInfo(t *testing.T) {
	v := &VertexInfo{
		Type:      "person",
		ChunkSize: 100,
		Prefix:    "person/",
		PropertyGroups: []*PropertyGroup{
			{
				FileType: FileTypeParquet,
				Properties: []*Property{
					{Name: "id", DataType: DataTypeInt64, Primary: true},
				},
			},
		},
		Version: VersionInfo{Major: 0, Minor: 1, Patch: 0},
	}

	t.Run("init", func(t *testing.T) {
		if err := v.Init(); err != nil {
			t.Fatalf("init failed: %v", err)
		}
		if !v.HasProperty("id") {
			t.Errorf("expected to have property id after init")
		}
	})

	t.Run("validate", func(t *testing.T) {
		if err := v.Validate(); err != nil {
			t.Errorf("validate failed: %v", err)
		}

		// Type empty
		vErr := *v
		vErr.Type = ""
		if err := vErr.Validate(); err == nil {
			t.Error("expected error for empty type")
		}

		// ChunkSize <= 0
		vErr = *v
		vErr.ChunkSize = 0
		if err := vErr.Validate(); err == nil {
			t.Error("expected error for zero chunk_size")
		}

		// Prefix empty
		vErr = *v
		vErr.Prefix = ""
		vErr.BaseURI = nil
		if err := vErr.Validate(); err == nil {
			t.Error("expected error for empty prefix/base_uri")
		}

		// Duplicated property name
		vErr = *v
		vErr.PropertyGroups = []*PropertyGroup{
			{
				FileType: FileTypeParquet,
				Properties: []*Property{
					{Name: "id", DataType: DataTypeInt64},
					{Name: "id", DataType: DataTypeInt64},
				},
			},
		}
		if err := vErr.Validate(); err == nil {
			t.Error("expected error for duplicated property name")
		}

		// Empty property name
		vErr = *v
		vErr.PropertyGroups = []*PropertyGroup{
			{
				FileType: FileTypeParquet,
				Properties: []*Property{
					{Name: "", DataType: DataTypeInt64},
				},
			},
		}
		if err := vErr.Validate(); err == nil {
			t.Error("expected error for empty property name")
		}
	})

	t.Run("GetPropertyGroup", func(t *testing.T) {
		pg, err := v.GetPropertyGroup("id")
		if err != nil {
			t.Fatalf("GetPropertyGroup failed: %v", err)
		}
		if pg.FileType != FileTypeParquet {
			t.Errorf("expected parquet group")
		}
	})
}
