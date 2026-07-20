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

package types

import (
	"fmt"
	"strings"
)

// TypeID enumerates the primitive categories of DataType. The on-disk contract
// is the string form (ParseDataType / DataType.String); do not rely on the
// numeric ID for interchange.
type TypeID uint8

// TypeID values.
const (
	BoolType TypeID = iota
	Int32Type
	Int64Type
	FloatType
	DoubleType
	StringType
	ListType
	DateType
	TimestampType
	UserDefinedType
)

// listPrefix / listSuffix delimit the on-disk spelling of a list type
// ("list<int32>"). Shared by String (write) and ParseDataType (read).
const (
	listPrefix = "list<"
	listSuffix = ">"
)

// DataType is the value-typed description of a GraphAr property's data type.
//
// Simple types are constructed with the helpers (Bool, Int32, Int64, Float,
// Double, String, Date, Timestamp). List types wrap a child DataType via List.
// UserDefined carries a free-form name.
//
// DataType is immutable: the Child pointer is treated as read-only and is
// never mutated after construction.
//
// The zero value is the unset sentinel: IsZero() reports it, and String()
// renders it as "unset" (which ParseDataType rejects), so a forgotten Type
// gets caught instead of passing as a real one.
type DataType struct {
	id    TypeID
	valid bool      // true when constructed via any factory or ParseDataType
	child *DataType // only meaningful when id == ListType
	name  string    // only meaningful when id == UserDefinedType
}

// Bool returns the boolean DataType.
func Bool() DataType { return DataType{id: BoolType, valid: true} }

// Int32 returns the 32-bit signed integer DataType.
func Int32() DataType { return DataType{id: Int32Type, valid: true} }

// Int64 returns the 64-bit signed integer DataType.
func Int64() DataType { return DataType{id: Int64Type, valid: true} }

// Float returns the 32-bit floating point DataType.
func Float() DataType { return DataType{id: FloatType, valid: true} }

// Double returns the 64-bit floating point DataType.
func Double() DataType { return DataType{id: DoubleType, valid: true} }

// String returns the UTF-8 string DataType.
func String() DataType { return DataType{id: StringType, valid: true} }

// Date returns the date DataType (days since the Unix epoch).
func Date() DataType { return DataType{id: DateType, valid: true} }

// Timestamp returns the timestamp DataType (milliseconds since the Unix epoch).
func Timestamp() DataType { return DataType{id: TimestampType, valid: true} }

// List returns a list DataType with the given element type.
func List(child DataType) DataType {
	c := child
	return DataType{id: ListType, valid: true, child: &c}
}

// UserDefined returns a user-defined DataType with the given name.
// The name is the on-disk spelling and must be non-empty.
func UserDefined(name string) DataType {
	return DataType{id: UserDefinedType, valid: true, name: name}
}

// ID returns the type category.
func (d DataType) ID() TypeID { return d.id }

// ValueType returns the element type for ListType; the zero DataType and
// false for non-list types.
func (d DataType) ValueType() (DataType, bool) {
	if d.id != ListType || d.child == nil {
		return DataType{}, false
	}
	return *d.child, true
}

// UserDefinedName returns the user-defined type name when ID() ==
// UserDefinedType; otherwise the empty string.
func (d DataType) UserDefinedName() string {
	if d.id != UserDefinedType {
		return ""
	}
	return d.name
}

// IsZero reports whether d is the zero (unset) DataType - never produced by a
// constructor; use it to detect a forgotten Property.Type.
func (d DataType) IsZero() bool { return !d.valid }

// Equal reports whether two DataTypes describe the same logical type. The unset
// zero value equals only another unset value, never a constructed type.
func (d DataType) Equal(other DataType) bool {
	if d.valid != other.valid {
		return false
	}
	if d.id != other.id || d.name != other.name {
		return false
	}
	if d.id != ListType {
		return true
	}
	if d.child == nil || other.child == nil {
		return d.child == other.child
	}
	return d.child.Equal(*other.child)
}

// String returns the on-disk spelling (the inverse of ParseDataType for the
// supported set). The unset zero value renders as "unset" (not parseable) and
// unrecognised IDs as "unknown".
func (d DataType) String() string {
	if !d.valid {
		return "unset"
	}
	switch d.id {
	case BoolType:
		return "bool"
	case Int32Type:
		return "int32"
	case Int64Type:
		return "int64"
	case FloatType:
		return "float"
	case DoubleType:
		return "double"
	case StringType:
		return "string"
	case DateType:
		return "date"
	case TimestampType:
		return "timestamp"
	case ListType:
		if d.child == nil {
			return listPrefix + "unknown" + listSuffix
		}
		return listPrefix + d.child.String() + listSuffix
	case UserDefinedType:
		return d.name
	default:
		return "unknown"
	}
}

// Validate reports whether d is well-formed for a strict (write) path. It
// recurses into list elements: a list may only hold a scalar element type from
// the shared subset int32/int64/float/double/string (bool, date, timestamp and
// nested lists are rejected, matching what the readers accept), and a
// user-defined type must carry a non-empty name. The zero value is invalid.
func (d DataType) Validate() error {
	if !d.valid {
		return fmt.Errorf("%w: unset data type", ErrInvalidDataType)
	}
	switch d.id {
	case ListType:
		if d.child == nil || !d.child.valid {
			return fmt.Errorf("%w: list has no element type", ErrInvalidDataType)
		}
		switch d.child.id {
		case Int32Type, Int64Type, FloatType, DoubleType, StringType:
			return nil
		default:
			return fmt.Errorf("%w: list element %q must be int32, int64, float, double or string",
				ErrInvalidDataType, d.child.String())
		}
	case UserDefinedType:
		if err := validateUserDefinedName(d.name); err != nil {
			return fmt.Errorf("%w: %s", ErrInvalidDataType, err)
		}
	}
	return nil
}

// validateUserDefinedName reports why name is unusable as a user-defined type
// name, or nil when it is fine. On top of the validateTypeName character
// rules, the name must not be a spelling ParseDataType already claims for a
// built-in. Callers wrap the result with their own sentinel.
func validateUserDefinedName(name string) error {
	if err := validateTypeName(name); err != nil {
		return err
	}
	if _, err := ParseDataType(name); err == nil {
		return fmt.Errorf("user-defined type name %q collides with a built-in type spelling", name)
	}
	return nil
}

// ParseDataType parses the on-disk spelling produced by DataType.String: the
// closed set bool/int32/int64/float/double/string/date/timestamp and list<...>
// over them. Anything else - including user-defined names, which are
// indistinguishable from typos here - errors with ErrInvalidDataType; build
// user-defined types explicitly with UserDefined.
func ParseDataType(s string) (DataType, error) {
	trimmed := strings.TrimSpace(s)
	switch trimmed {
	case "bool":
		return Bool(), nil
	case "int32":
		return Int32(), nil
	case "int64":
		return Int64(), nil
	case "float":
		return Float(), nil
	case "double":
		return Double(), nil
	case "string":
		return String(), nil
	case "date":
		return Date(), nil
	case "timestamp":
		return Timestamp(), nil
	}
	if strings.HasPrefix(trimmed, listPrefix) && strings.HasSuffix(trimmed, listSuffix) {
		inner := trimmed[len(listPrefix) : len(trimmed)-len(listSuffix)]
		// Reject empty <> and nested user-defined.
		if inner == "" {
			return DataType{}, fmt.Errorf("%w: empty list element type", ErrInvalidDataType)
		}
		child, err := ParseDataType(inner)
		if err != nil {
			return DataType{}, fmt.Errorf("%w: invalid list element %q", ErrInvalidDataType, inner)
		}
		return List(child), nil
	}
	return DataType{}, fmt.Errorf("%w: %q", ErrInvalidDataType, s)
}
