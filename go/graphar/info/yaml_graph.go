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
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"

	internalyaml "github.com/apache/incubator-graphar/go/graphar/internal/yaml"
)

// Loader defines a function type to load file content from a given path.
type Loader func(path string) ([]byte, error)

// LoadGraphInfo reads a graph info YAML file and all referenced vertex/edge info files from local filesystem.
func LoadGraphInfo(path string) (*GraphInfo, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	l := func(p string) ([]byte, error) {
		return os.ReadFile(p)
	}

	return LoadGraphInfoFromBytes(b, path, l)
}

// LoadGraphInfoFromBytes loads GraphInfo from bytes and uses the loader to load referenced info files.
func LoadGraphInfoFromBytes(b []byte, path string, l Loader) (*GraphInfo, error) {
	baseDir := filepath.Dir(path)
	// Ensure baseURI has a trailing slash for proper relative resolution
	baseURIStr := baseDir
	if baseURIStr != "" && !os.IsPathSeparator(baseURIStr[len(baseURIStr)-1]) {
		baseURIStr += string(os.PathSeparator)
	}
	baseURI, _ := url.Parse(baseURIStr)

	return parseGraphInfoYAML(bytes.NewReader(b), baseURI, baseDir, l)
}

// parseGraphInfoYAML parses a graph-level YAML from r, resolving referenced
// vertex/edge info files relative to baseDir/baseURI using the provided loader.
func parseGraphInfoYAML(r io.Reader, baseURI *url.URL, baseDir string, l Loader) (*GraphInfo, error) {
	gy, err := internalyaml.DecodeGraph(r)
	if err != nil {
		return nil, fmt.Errorf("%w: decode graph yaml: %v", ErrParse, err)
	}

	if gy.Version == "" {
		return nil, fmt.Errorf("%w: graph yaml missing version", ErrMissingField)
	}
	v, err := parseVersion(gy.Version)
	if err != nil {
		return nil, fmt.Errorf("%w: parse graph version: %v", ErrParse, err)
	}
	if !v.isSupported() {
		return nil, fmt.Errorf("%w: graph info version %s is not supported", ErrUnsupported, v.String())
	}

	g := &GraphInfo{
		Name:    gy.Name,
		Prefix:  gy.Prefix,
		Version: v,
		BaseURI: baseURI,
	}

	for _, vpath := range gy.Vertices {
		abs := filepath.Join(baseDir, vpath)
		var vi *VertexInfo
		if l != nil {
			vb, err := l(abs)
			if err != nil {
				return nil, fmt.Errorf("%w: load vertex info %q: %v", ErrNotFound, vpath, err)
			}
			vi, err = LoadVertexInfoFromBytes(vb)
		} else {
			vi, err = LoadVertexInfo(abs)
		}
		if err != nil {
			return nil, fmt.Errorf("%w: parse vertex info %q: %v", ErrParse, vpath, err)
		}
		g.VertexInfos = append(g.VertexInfos, vi)
	}

	for _, epath := range gy.Edges {
		abs := filepath.Join(baseDir, epath)
		var ei *EdgeInfo
		if l != nil {
			eb, err := l(abs)
			if err != nil {
				return nil, fmt.Errorf("%w: load edge info %q: %v", ErrNotFound, epath, err)
			}
			ei, err = LoadEdgeInfoFromBytes(eb)
		} else {
			ei, err = LoadEdgeInfo(abs)
		}
		if err != nil {
			return nil, fmt.Errorf("%w: parse edge info %q: %v", ErrParse, epath, err)
		}
		g.EdgeInfos = append(g.EdgeInfos, ei)
	}

	if err := g.Init(); err != nil {
		return nil, err
	}
	if err := g.Validate(); err != nil {
		return nil, err
	}
	return g, nil
}
