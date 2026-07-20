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
	"errors"
	"testing"

	"github.com/apache/incubator-graphar/go/graphar/types"
)

func TestAdjacentListEffectivePrefix(t *testing.T) {
	t.Parallel()
	a := AdjacentList{Type: types.OrderedBySource, FileType: types.FileTypeParquet}
	if got := a.EffectivePrefix(); got != "ordered_by_source/" {
		t.Errorf("EffectivePrefix() = %q", got)
	}
	a.Prefix = "ordered_by_src/"
	if got := a.EffectivePrefix(); got != "ordered_by_src/" {
		t.Errorf("explicit prefix not honoured: %q", got)
	}
	// An explicit prefix without a trailing slash is normalised to one.
	a.Prefix = "custom"
	if got := a.EffectivePrefix(); got != "custom/" {
		t.Errorf("prefix trailing slash not normalised: %q", got)
	}
}

func TestAdjacentListValidate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		a    AdjacentList
		want error
	}{
		{"zero", AdjacentList{}, ErrInvalidEdgeInfo},
		{"unknown type", AdjacentList{Type: 0xff, FileType: types.FileTypeCSV}, ErrInvalidEdgeInfo},
		{"missing file type", AdjacentList{Type: types.OrderedBySource}, ErrInvalidEdgeInfo},
		{"json file type rejected", AdjacentList{Type: types.OrderedBySource, FileType: types.FileTypeJSON}, ErrInvalidEdgeInfo},
		{"ok", AdjacentList{Type: types.OrderedBySource, FileType: types.FileTypeCSV}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.a.Validate()
			if c.want == nil {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				return
			}
			if !errors.Is(err, c.want) {
				t.Errorf("got %v, want wrap of %v", err, c.want)
			}
		})
	}
}

func TestAdjacentListEqual(t *testing.T) {
	t.Parallel()
	a := AdjacentList{Type: types.OrderedBySource, FileType: types.FileTypeParquet, Prefix: "p/"}
	if !a.Equal(a) {
		t.Error("reflexive failed")
	}
	other := a
	other.Type = types.OrderedByDest
	if a.Equal(other) {
		t.Error("different type reported equal")
	}
	other = a
	other.FileType = types.FileTypeCSV
	if a.Equal(other) {
		t.Error("different filetype reported equal")
	}
	other = a
	other.Prefix = "q/"
	if a.Equal(other) {
		t.Error("different prefix reported equal")
	}
}
