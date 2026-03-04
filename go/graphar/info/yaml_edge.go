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

// LoadEdgeInfo loads a single edge info YAML file from local filesystem.
func LoadEdgeInfo(path string) (*EdgeInfo, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return LoadEdgeInfoFromBytes(b)
}

// LoadEdgeInfoFromBytes loads a single edge info from bytes.
func LoadEdgeInfoFromBytes(b []byte) (*EdgeInfo, error) {
	var e EdgeInfo
	if err := internalyaml.DecodeEdge(bytes.NewReader(b), &e); err != nil {
		return nil, err
	}

	if e.Prefix != "" {
		u, err := url.Parse(e.Prefix)
		if err != nil {
			return nil, err
		}
		e.BaseURI = u
	}
	if err := e.Init(); err != nil {
		return nil, err
	}
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return &e, nil
}
