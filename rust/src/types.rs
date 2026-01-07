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

use crate::ffi;
use cxx::SharedPtr;
use std::fmt::{Debug, Display};

// C++ enums
pub use crate::ffi::graphar::Type;

#[derive(Clone)]
pub struct DataType(SharedPtr<ffi::graphar::DataType>);

impl PartialEq for DataType {
    fn eq(&self, other: &Self) -> bool {
        if self.0.is_null() && other.0.is_null() {
            return true;
        }
        if self.0.is_null() || other.0.is_null() {
            return false;
        }

        self.0.Equals(other.0.as_ref().expect("rhs is nullptr"))
    }
}

impl Eq for DataType {}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_null() {
            write!(f, "null")
        } else {
            write!(
                f,
                "{}",
                ffi::graphar::to_type_name(self.0.as_ref().unwrap())
            )
        }
    }
}

impl Debug for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_null() {
            write!(f, "null")
        } else {
            write!(
                f,
                "{}",
                ffi::graphar::to_type_name(self.0.as_ref().unwrap())
            )
        }
    }
}

impl DataType {
    pub fn value_type(&self) -> Self {
        DataType(self.0.value_type().clone())
    }

    pub fn id(&self) -> Type {
        self.0.id()
    }

    pub fn bool() -> Self {
        Self(ffi::graphar::boolean().clone())
    }

    pub fn int32() -> Self {
        Self(ffi::graphar::int32().clone())
    }

    pub fn int64() -> Self {
        Self(ffi::graphar::int64().clone())
    }

    pub fn float32() -> Self {
        Self(ffi::graphar::float32().clone())
    }

    pub fn float64() -> Self {
        Self(ffi::graphar::float64().clone())
    }

    pub fn string() -> Self {
        Self(ffi::graphar::string().clone())
    }

    pub fn date() -> Self {
        Self(ffi::graphar::date().clone())
    }

    pub fn timestamp() -> Self {
        Self(ffi::graphar::timestamp().clone())
    }

    pub fn list(value_type: &DataType) -> Self {
        Self(ffi::graphar::list(&value_type.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl DataType {
        fn null() -> Self {
            Self(SharedPtr::null())
        }
    }

    // `DataType`
    #[test]
    fn test_data_type_equality() {
        let float = DataType::float32();
        let float1 = DataType::float32();
        assert_eq!(float, float1);
        assert_ne!(DataType::null(), float);

        let double = DataType::float64();
        assert_ne!(float, double);

        let list_of_float_1 = DataType::list(&float);
        let list_of_float_2 = DataType::list(&float);
        assert_eq!(list_of_float_1, list_of_float_2);

        let list_of_double = DataType::list(&double);
        assert_ne!(list_of_float_1, list_of_double);
    }

    #[test]
    fn test_data_type_display_and_debug() {
        let bool_type = DataType::bool();
        assert_eq!(format!("{}", bool_type), "bool");
        assert_eq!(format!("{:?}", bool_type), "bool");

        let int32 = DataType::int32();
        assert_eq!(format!("{}", int32), "int32");
        assert_eq!(format!("{:?}", int32), "int32");

        let int64 = DataType::int64();
        assert_eq!(format!("{}", int64), "int64");
        assert_eq!(format!("{:?}", int64), "int64");

        let float32 = DataType::float32();
        assert_eq!(format!("{}", float32), "float");
        assert_eq!(format!("{:?}", float32), "float");

        let float64 = DataType::float64();
        assert_eq!(format!("{}", float64), "double");
        assert_eq!(format!("{:?}", float64), "double");

        let string = DataType::string();
        assert_eq!(format!("{}", string), "string");
        assert_eq!(format!("{:?}", string), "string");

        let date = DataType::date();
        assert_eq!(format!("{}", date), "date");
        assert_eq!(format!("{:?}", date), "date");

        let timestamp = DataType::timestamp();
        assert_eq!(format!("{}", timestamp), "timestamp");
        assert_eq!(format!("{:?}", timestamp), "timestamp");

        let list_of_int32 = DataType::list(&int32);
        assert_eq!(format!("{}", list_of_int32), "list<int32>");
        assert_eq!(format!("{:?}", list_of_int32), "list<int32>");

        let nested_list = DataType::list(&list_of_int32);
        assert_eq!(format!("{}", nested_list), "list<list<int32>>");
        assert_eq!(format!("{:?}", nested_list), "list<list<int32>>");

        assert_eq!(format!("{}", int32.value_type()), "null");
        assert_eq!(format!("{:?}", int32.value_type()), "null");
    }

    #[test]
    fn test_data_type_value_type() {
        let bool_type = DataType::bool();
        assert_eq!(bool_type.value_type(), DataType::null());

        let int32 = DataType::int32();
        assert_eq!(int32.value_type(), DataType::null());

        let int64 = DataType::int64();
        assert_eq!(int64.value_type(), DataType::null());

        let float32 = DataType::float32();
        assert_eq!(float32.value_type(), DataType::null());

        let float64 = DataType::float64();
        assert_eq!(float64.value_type(), DataType::null());

        let string = DataType::string();
        assert_eq!(string.value_type(), DataType::null());

        let date = DataType::date();
        assert_eq!(date.value_type(), DataType::null());

        let timestamp = DataType::timestamp();
        assert_eq!(timestamp.value_type(), DataType::null());

        let list_of_int32 = DataType::list(&int32);
        assert_eq!(list_of_int32.value_type(), int32);

        let list_of_float32 = DataType::list(&float32);
        assert_eq!(list_of_float32.value_type(), float32);

        let list_of_string = DataType::list(&string);
        assert_eq!(list_of_string.value_type(), string);

        let nested_list = DataType::list(&list_of_int32);
        assert_eq!(nested_list.value_type(), list_of_int32);
    }

    #[test]
    fn test_data_type_id() {
        let bool_type = DataType::bool();
        assert_eq!(bool_type.id(), Type::Bool);

        let int32 = DataType::int32();
        assert_eq!(int32.id(), Type::Int32);

        let int64 = DataType::int64();
        assert_eq!(int64.id(), Type::Int64);

        let float32 = DataType::float32();
        assert_eq!(float32.id(), Type::Float);

        let float64 = DataType::float64();
        assert_eq!(float64.id(), Type::Double);

        let string = DataType::string();
        assert_eq!(string.id(), Type::String);

        let date = DataType::date();
        assert_eq!(date.id(), Type::Date);

        let timestamp = DataType::timestamp();
        assert_eq!(timestamp.id(), Type::Timestamp);

        let list_of_int32 = DataType::list(&int32);
        assert_eq!(list_of_int32.id(), Type::List);
        assert_eq!(list_of_int32.value_type().id(), Type::Int32);

        let list_of_lists = DataType::list(&list_of_int32);
        assert_eq!(list_of_lists.id(), Type::List);
        assert_eq!(list_of_lists.value_type().id(), Type::List);
    }
}
