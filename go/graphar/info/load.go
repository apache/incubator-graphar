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
	"fmt"
	"io"
	iofs "io/fs"
	"path"
	"strings"

	yaml "gopkg.in/yaml.v3"

	"github.com/apache/incubator-graphar/go/graphar/types"
)

// LoadVertexInfo decodes a vertex info YAML document from r.
//
// Loading is liberal: any document that parses succeeds, even one that violates
// GraphAr's semantic rules - call Validate to enforce them. Only unparseable
// input (bad YAML, an unknown enum, a duplicate property name) or a document
// that does not name its type is rejected here.
func LoadVertexInfo(r io.Reader) (*VertexInfo, error) {
	var y vertexInfoYAML
	if err := decode(r, &y); err != nil {
		return nil, err
	}
	return vertexInfoFromYAML(y)
}

// LoadEdgeInfo decodes an edge info YAML document from r. Loading is liberal;
// see LoadVertexInfo for the read-vs-validate contract. Call Validate on the
// result to enforce GraphAr's semantic rules.
func LoadEdgeInfo(r io.Reader) (*EdgeInfo, error) {
	var y edgeInfoYAML
	if err := decode(r, &y); err != nil {
		return nil, err
	}
	return edgeInfoFromYAML(y)
}

// LoadGraphInfo decodes a graph info YAML at graphPath inside fsys and then
// resolves every referenced vertex/edge file relative to graphPath's directory.
//
// References inside the graph yaml are filenames (e.g. "person.vertex.yml")
// taken as paths relative to the graph yaml's directory. Use path/filepath-style
// separators acceptable to io/fs.
//
// Loading is liberal (see LoadVertexInfo); Validate recurses into every vertex
// and edge to enforce the full contract, including cross-references.
func LoadGraphInfo(fsys iofs.FS, graphPath string) (*GraphInfo, error) {
	gy, err := readGraphYAML(fsys, graphPath)
	if err != nil {
		return nil, err
	}
	// Minimal completeness: a graph document must name itself, otherwise the
	// input was some other (mis-filed) document decoded into a zero DTO.
	if gy.Name == "" {
		return nil, fmt.Errorf("%q: %w: graph info document has no name", graphPath, ErrInvalidYAML)
	}
	dir := path.Dir(graphPath)

	vertices := make([]*VertexInfo, 0, len(gy.Vertices))
	vertexRefs := make(map[string]string, len(gy.Vertices))
	for _, vname := range gy.Vertices {
		vpath, err := joinFSPath(dir, vname)
		if err != nil {
			return nil, fmt.Errorf("graph_info(%q).vertices[%q]: %w", graphPath, vname, err)
		}
		v, err := loadVertexInfoFile(fsys, vpath)
		if err != nil {
			return nil, fmt.Errorf("graph_info(%q).vertices[%q]: %w", graphPath, vname, err)
		}
		vertices = append(vertices, v)
		vertexRefs[v.Type()] = vname
	}
	edges := make([]*EdgeInfo, 0, len(gy.Edges))
	edgeRefs := make(map[EdgeTriplet]string, len(gy.Edges))
	for _, ename := range gy.Edges {
		epath, err := joinFSPath(dir, ename)
		if err != nil {
			return nil, fmt.Errorf("graph_info(%q).edges[%q]: %w", graphPath, ename, err)
		}
		e, err := loadEdgeInfoFile(fsys, epath)
		if err != nil {
			return nil, fmt.Errorf("graph_info(%q).edges[%q]: %w", graphPath, ename, err)
		}
		edges = append(edges, e)
		src, edge, dst := e.Triplet()
		edgeRefs[EdgeTriplet{Src: src, Edge: edge, Dst: dst}] = ename
	}

	opts := []GraphInfoOption{}
	if gy.Prefix != "" {
		opts = append(opts, WithGraphPrefix(gy.Prefix))
	}
	if len(vertices) > 0 {
		opts = append(opts, WithVertices(vertices...), withVertexFileRefs(vertexRefs))
	}
	if len(edges) > 0 {
		opts = append(opts, WithEdges(edges...), withEdgeFileRefs(edgeRefs))
	}
	if gy.Version != "" {
		ver, err := types.ParseInfoVersion(gy.Version)
		if err != nil {
			return nil, fmt.Errorf("graph_info.version: %w", err)
		}
		opts = append(opts, WithGraphVersion(ver))
	}
	if len(gy.Labels) > 0 {
		opts = append(opts, WithGraphLabels(gy.Labels...))
	}
	if len(gy.ExtraInfo) > 0 {
		extra := make(map[string]string, len(gy.ExtraInfo))
		for _, kv := range gy.ExtraInfo {
			extra[kv.Key] = kv.Value
		}
		opts = append(opts, WithGraphExtraInfo(extra))
	}
	return newGraphInfoUnvalidated(gy.Name, opts...)
}

func readGraphYAML(fsys iofs.FS, p string) (*graphInfoYAML, error) {
	f, err := fsys.Open(p)
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", p, err)
	}
	defer f.Close() //nolint:errcheck // best-effort close on a read-only handle.
	var y graphInfoYAML
	if err := decode(f, &y); err != nil {
		return nil, fmt.Errorf("%q: %w", p, err)
	}
	return &y, nil
}

func loadVertexInfoFile(fsys iofs.FS, p string) (*VertexInfo, error) {
	f, err := fsys.Open(p)
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", p, err)
	}
	defer f.Close() //nolint:errcheck // best-effort close.
	return LoadVertexInfo(f)
}

func loadEdgeInfoFile(fsys iofs.FS, p string) (*EdgeInfo, error) {
	f, err := fsys.Open(p)
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", p, err)
	}
	defer f.Close() //nolint:errcheck // best-effort close.
	return LoadEdgeInfo(f)
}

func decode(r io.Reader, out any) error {
	// Unknown fields are ignored: other SDKs add fields over time (labels,
	// extra_info, per-adj-list property_groups all arrived late) and a reader
	// that rejected them would break forward compatibility.
	dec := yaml.NewDecoder(r)
	if err := dec.Decode(out); err != nil {
		// Wrap both sentinels with %w (Go 1.20+ multi-%w) so callers can
		// errors.Is the package sentinel AND errors.As the underlying
		// yaml.TypeError / io.ErrUnexpectedEOF / etc. for line numbers.
		return fmt.Errorf("%w: %w", ErrInvalidYAML, err)
	}
	return nil
}

// validateFileRef checks a vertex/edge file reference: non-empty, not absolute,
// and a valid relative fs path with no "." or ".." element. Shared by the load
// path (joinFSPath) and the write path so a graph yaml never references a file
// the loader would refuse to open.
func validateFileRef(name string) error {
	if name == "" {
		return fmt.Errorf("%w: empty file reference", ErrInvalidGraphInfo)
	}
	if strings.HasPrefix(name, "/") {
		return fmt.Errorf("%w: absolute path %q not allowed", ErrInvalidGraphInfo, name)
	}
	// fs.ValidPath(".") is true but "." is a directory, not a file reference.
	if name == "." {
		return fmt.Errorf("%w: file reference %q is not a file", ErrInvalidGraphInfo, name)
	}
	if !iofs.ValidPath(name) {
		return fmt.Errorf("%w: invalid file reference %q", ErrInvalidGraphInfo, name)
	}
	return nil
}

// joinFSPath resolves name as a path relative to dir within an io/fs.FS root.
// It rejects names that escape dir via "..", that start with "/" (absolute),
// or that are otherwise not fs.ValidPath. The resulting path is always
// contained within dir; this is the security boundary that backs the
// "references are relative to the graph yaml's directory" contract.
func joinFSPath(dir, name string) (string, error) {
	if err := validateFileRef(name); err != nil {
		return "", err
	}
	var joined string
	if dir == "" || dir == "." {
		joined = path.Clean(name)
	} else {
		joined = path.Join(dir, name)
	}
	// path.Clean / path.Join collapse ".." - an escape attempt either yields
	// a path outside dir, or (when fs.FS root is the destination) a sibling
	// directory the graph yaml had no right to reach.
	if !iofs.ValidPath(joined) {
		return "", fmt.Errorf("%w: invalid path %q", ErrInvalidGraphInfo, name)
	}
	if dir != "" && dir != "." {
		cleanDir := path.Clean(dir)
		if joined != cleanDir && !strings.HasPrefix(joined, cleanDir+"/") {
			return "", fmt.Errorf("%w: file reference %q escapes graph directory %q",
				ErrInvalidGraphInfo, name, dir)
		}
	}
	return joined, nil
}
