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
	"fmt"
	"io"

	"gopkg.in/yaml.v3"
)

// GraphSchema mirrors the on-disk YAML representation of a graph info file.
type GraphSchema struct {
	Name     string   `yaml:"name"`
	Prefix   string   `yaml:"prefix"`
	Vertices []string `yaml:"vertices"`
	Edges    []string `yaml:"edges"`
	Version  string   `yaml:"version"`
}

// DecodeGraph decodes a graph info YAML from r.
func DecodeGraph(r io.Reader) (*GraphSchema, error) {
	var g GraphSchema
	if err := yaml.NewDecoder(r).Decode(&g); err != nil {
		return nil, fmt.Errorf("decode graph yaml: %w", err)
	}
	return &g, nil
}

// DecodeVertex decodes a vertex info YAML from r into v.
func DecodeVertex(r io.Reader, v interface{}) error {
	if err := yaml.NewDecoder(r).Decode(v); err != nil {
		return fmt.Errorf("decode vertex info: %w", err)
	}
	return nil
}

// DecodeEdge decodes an edge info YAML from r into e.
func DecodeEdge(r io.Reader, e interface{}) error {
	if err := yaml.NewDecoder(r).Decode(e); err != nil {
		return fmt.Errorf("decode edge info: %w", err)
	}
	return nil
}
