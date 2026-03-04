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

import "errors"

var (
	// ErrNotFound indicates a requested info object (e.g. vertex type, edge triplet) doesn't exist.
	ErrNotFound = errors.New("graphar info: not found")

	// ErrInvalid indicates an info object is structurally invalid.
	ErrInvalid = errors.New("graphar info: invalid")

	// ErrParse indicates parsing failed (e.g. YAML decoding error).
	ErrParse = errors.New("graphar info: parse error")

	// ErrUnsupported indicates an unsupported feature or version.
	ErrUnsupported = errors.New("graphar info: unsupported")

	// ErrMissingField indicates a required field is missing in YAML.
	ErrMissingField = errors.New("graphar info: missing field")

	// ErrDuplicate indicates a duplicated entity (e.g. property name, label).
	ErrDuplicate = errors.New("graphar info: duplicate")
)
