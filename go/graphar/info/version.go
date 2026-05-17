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
	"strconv"
	"strings"
)

const (
	// maxSupportedMajor is the maximum major version of the GraphAr metadata supported by this SDK.
	maxSupportedMajor = 1
	// maxSupportedMinor is the maximum minor version of the GraphAr metadata supported by this SDK.
	maxSupportedMinor = 0
)

// VersionInfo represents a semantic version of the GraphAr metadata.
type VersionInfo struct {
	Major int `yaml:"major"`
	Minor int `yaml:"minor"`
	Patch int `yaml:"patch"`
}

// isSupported checks if the metadata version is supported by this SDK.
func (v VersionInfo) isSupported() bool {
	if v.Major > maxSupportedMajor {
		return false
	}
	if v.Major == maxSupportedMajor && v.Minor > maxSupportedMinor {
		return false
	}
	return true
}

// parseVersion parses a version string like "0.1.0" or "gar/v1" into VersionInfo.
func parseVersion(s string) (VersionInfo, error) {
	if s == versionGarV1 {
		return VersionInfo{Major: 1, Minor: 0, Patch: 0}, nil
	}
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return VersionInfo{}, fmt.Errorf("%w: invalid version %q", ErrParse, s)
	}
	ints := make([]int, 3)
	for i, p := range parts {
		v, err := strconv.Atoi(p)
		if err != nil {
			return VersionInfo{}, fmt.Errorf("%w: invalid version component %q: %v", ErrParse, p, err)
		}
		ints[i] = v
	}
	return VersionInfo{Major: ints[0], Minor: ints[1], Patch: ints[2]}, nil
}

func (v VersionInfo) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}

// UnmarshalYAML implements the yaml.Unmarshaler interface to support parsing from string.
func (v *VersionInfo) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		// Try to unmarshal as struct if string fails
		type rawVersion VersionInfo
		return unmarshal((*rawVersion)(v))
	}
	parsed, err := parseVersion(s)
	if err != nil {
		return err
	}
	*v = parsed
	return nil
}
