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

#[cxx::bridge(namespace = "graphar")]
pub(crate) mod graphar {
    extern "C++" {
        include!("graphar_rs.h");
    }

    /// The main data type enumeration used by GraphAr.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(u32)]
    enum Type {
        /// Boolean.
        #[cxx_name = "BOOL"]
        Bool = 0,
        /// Signed 32-bit integer.
        #[cxx_name = "INT32"]
        Int32,
        /// Signed 64-bit integer.
        #[cxx_name = "INT64"]
        Int64,
        /// 4-byte floating point value.
        #[cxx_name = "FLOAT"]
        Float,
        /// 8-byte floating point value.
        #[cxx_name = "DOUBLE"]
        Double,
        /// UTF-8 variable-length string.
        #[cxx_name = "STRING"]
        String,
        /// List of some logical data type.
        #[cxx_name = "LIST"]
        List,
        /// int32 days since the UNIX epoch.
        #[cxx_name = "DATE"]
        Date,
        /// Exact timestamp encoded with int64 since UNIX epoch in milliseconds.
        #[cxx_name = "TIMESTAMP"]
        Timestamp,
        /// User-defined data type.
        #[cxx_name = "USER_DEFINED"]
        UserDefined,
        /// Sentinel value; do not use as a real type.
        #[cxx_name = "MAX_ID"]
        MaxId,
    }
    // C++ Enum
    unsafe extern "C++" {
        type Type;
    }

    // `DataType`
    unsafe extern "C++" {
        type DataType;

        fn Equals(&self, other: &DataType) -> bool;
        fn value_type(&self) -> &SharedPtr<DataType>;
        fn id(&self) -> Type;
        #[namespace = "graphar_rs"]
        fn to_type_name(data_type: &DataType) -> String;

        fn int32() -> &'static SharedPtr<DataType>;
        fn boolean() -> &'static SharedPtr<DataType>;
        fn int64() -> &'static SharedPtr<DataType>;
        fn float32() -> &'static SharedPtr<DataType>;
        fn float64() -> &'static SharedPtr<DataType>;
        fn string() -> &'static SharedPtr<DataType>;
        fn date() -> &'static SharedPtr<DataType>;
        fn timestamp() -> &'static SharedPtr<DataType>;
        fn list(inner: &SharedPtr<DataType>) -> SharedPtr<DataType>;
    }
}
