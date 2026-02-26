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

func TestPropertyGroups(t *testing.T) {
	p1 := &Property{Name: "id", DataType: DataTypeInt64, Primary: true, Nullable: false}
	p2 := &Property{Name: "name", DataType: DataTypeString, Primary: false, Nullable: true}
	pg1 := &PropertyGroup{
		Properties: []*Property{p1, p2},
		FileType:   FileTypeParquet,
		Prefix:     "p1/",
	}

	p3 := &Property{Name: "age", DataType: DataTypeInt32, Primary: false, Nullable: true}
	pg2 := &PropertyGroup{
		Properties: []*Property{p3},
		FileType:   FileTypeCSV,
		Prefix:     "p2/",
	}

	groups := newPropertyGroups([]*PropertyGroup{pg1, pg2})

	t.Run("HasProperty", func(t *testing.T) {
		if !groups.hasProperty("id") {
			t.Errorf("expected to have property id")
		}
		if !groups.hasProperty("age") {
			t.Errorf("expected to have property age")
		}
		if groups.hasProperty("nonexistent") {
			t.Errorf("did not expect to have nonexistent property")
		}
	})

	t.Run("PropertyGroupNum", func(t *testing.T) {
		if groups.propertyGroupNum() != 2 {
			t.Errorf("expected 2 groups, got %d", groups.propertyGroupNum())
		}
	})

	t.Run("PropertyDetails", func(t *testing.T) {
		pType, err := groups.propertyType("id")
		if err != nil || pType != DataTypeInt64 {
			t.Errorf("expected DataTypeInt64 for id, got %v, err: %v", pType, err)
		}

		card, err := groups.cardinality("id")
		if err != nil || card != "" {
			t.Errorf("expected empty cardinality for id, got %v, err: %v", card, err)
		}

		isPrimary, err := groups.isPrimaryKey("id")
		if err != nil || !isPrimary {
			t.Errorf("expected id to be primary key, got %v, err: %v", isPrimary, err)
		}

		isNullable, err := groups.isNullableKey("id")
		if err != nil || isNullable {
			t.Errorf("expected id to be non-nullable, got %v, err: %v", isNullable, err)
		}

		isPrimaryName, err := groups.isPrimaryKey("name")
		if err != nil || isPrimaryName {
			t.Errorf("expected name to not be primary key, got %v, err: %v", isPrimaryName, err)
		}

		isNullableName, err := groups.isNullableKey("name")
		if err != nil || !isNullableName {
			t.Errorf("expected name to be nullable, got %v, err: %v", isNullableName, err)
		}

		pAge := &Property{Name: "age", DataType: DataTypeInt32, Cardinality: CardinalitySingle}
		pgAge := newPropertyGroups([]*PropertyGroup{{Properties: []*Property{pAge}, FileType: FileTypeParquet}})
		cardAge, err := pgAge.cardinality("age")
		if err != nil || cardAge != CardinalitySingle {
			t.Errorf("expected CardinalitySingle for age, got %v, err: %v", cardAge, err)
		}
	})

	t.Run("PropertyGroupList", func(t *testing.T) {
		list := groups.propertyGroupList()
		if len(list) != 2 {
			t.Errorf("expected 2 groups in list")
		}
	})

	t.Run("PropertyGroupFor", func(t *testing.T) {
		g, err := groups.propertyGroupFor("id")
		if err != nil || g.Prefix != "p1/" {
			t.Errorf("expected group with prefix p1/ for id, err: %v", err)
		}

		g2, err := groups.propertyGroupFor("age")
		if err != nil || g2.Prefix != "p2/" {
			t.Errorf("expected group with prefix p2/ for age, err: %v", err)
		}

		_, err = groups.propertyGroupFor("nonexistent")
		if err == nil {
			t.Errorf("did not expect group for nonexistent property")
		}
	})

	t.Run("HasPropertyGroup", func(t *testing.T) {
		if !groups.hasPropertyGroup(pg1) {
			t.Errorf("expected to find existing property group pg1")
		}

		otherPg := &PropertyGroup{Prefix: "other/", FileType: FileTypeCSV}
		if groups.hasPropertyGroup(otherPg) {
			t.Errorf("did not expect to find other property group")
		}
	})

	t.Run("ErrorCases", func(t *testing.T) {
		_, err := groups.propertyType("nonexistent")
		if err == nil {
			t.Errorf("expected error for nonexistent property")
		}
	})
}
