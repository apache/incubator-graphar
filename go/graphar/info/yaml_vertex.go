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
	"bytes"
	"net/url"
	"os"

	internalyaml "github.com/apache/incubator-graphar/go/graphar/internal/yaml"
)

// LoadVertexInfo loads a single vertex info YAML file from local filesystem.
func LoadVertexInfo(path string) (*VertexInfo, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return LoadVertexInfoFromBytes(b)
}

// LoadVertexInfoFromBytes loads a single vertex info from bytes.
func LoadVertexInfoFromBytes(b []byte) (*VertexInfo, error) {
	var v VertexInfo
	if err := internalyaml.DecodeVertex(bytes.NewReader(b), &v); err != nil {
		return nil, err
	}

	// Prefix is serialized string form of BaseURI.
	if v.Prefix != "" {
		u, err := url.Parse(v.Prefix)
		if err != nil {
			return nil, err
		}
		v.BaseURI = u
	}
	if err := v.Init(); err != nil {
		return nil, err
	}
	if err := v.Validate(); err != nil {
		return nil, err
	}
	return &v, nil
}
