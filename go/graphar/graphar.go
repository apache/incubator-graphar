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
	"fmt"

	"github.com/apache/incubator-graphar/go/graphar/info"
)

// FileSystem defines a minimal interface for reading files.
// It matches the requirements for loading GraphAr info files.
type FileSystem interface {
	// ReadFile reads the named file and returns the contents.
	ReadFile(ctx context.Context, name string) ([]byte, error)
}

// GraphAr is the main entry point for using the GraphAr Go SDK.
type GraphAr interface {
	// LoadGraphInfo loads a graph-level Info YAML (plus referenced vertex/edge info files)
	// using the configured FileSystem.
	LoadGraphInfo(ctx context.Context, path string) (*info.GraphInfo, error)
}

type client struct {
	fs FileSystem
}

// option configures the GraphAr instance.
type option func(*client)

// WithFileSystem sets a custom FileSystem for the GraphAr instance.
func WithFileSystem(fs FileSystem) option {
	return func(c *client) {
		c.fs = fs
	}
}

// New creates a new GraphAr instance.
func New(opts ...option) GraphAr {
	c := &client{}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	return c
}

// LoadGraphInfo loads a graph-level Info YAML (plus referenced vertex/edge
// info files) using the configured FileSystem.
func (c *client) LoadGraphInfo(ctx context.Context, path string) (*info.GraphInfo, error) {
	var g *info.GraphInfo
	var err error

	if c.fs != nil {
		// If custom FS is provided, we need to load via reader
		b, err := c.fs.ReadFile(ctx, path)
		if err != nil {
			return nil, fmt.Errorf("graphar: read graph info %q from fs: %w", path, err)
		}
		// We'll need to update info.LoadGraphInfo to accept a loader or use a simplified version
		g, err = info.LoadGraphInfoFromBytes(b, path, func(p string) ([]byte, error) {
			return c.fs.ReadFile(ctx, p)
		})
	} else {
		g, err = info.LoadGraphInfo(path)
	}

	if err != nil {
		return nil, fmt.Errorf("graphar: load graph info %q: %w", path, err)
	}
	return g, nil
}
