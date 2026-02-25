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

use cxx::{ExternType, SharedPtr};

#[repr(transparent)]
#[derive(Clone)]
pub struct SharedPropertyGroup(pub(crate) SharedPtr<graphar::PropertyGroup>);

unsafe impl ExternType for SharedPropertyGroup {
    type Id = cxx::type_id!("graphar::SharedPropertyGroup");
    type Kind = cxx::kind::Opaque;
}

#[repr(transparent)]
#[derive(Clone)]
pub struct SharedAdjacentList(pub(crate) SharedPtr<graphar::AdjacentList>);

unsafe impl ExternType for SharedAdjacentList {
    type Id = cxx::type_id!("graphar::SharedAdjacentList");
    type Kind = cxx::kind::Opaque;
}

#[cxx::bridge(namespace = "graphar")]
pub(crate) mod graphar {
    extern "C++" {
        include!("graphar_rs.h");
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(i32)]
    /// File format for GraphAr chunk files.
    enum FileType {
        /// CSV format.
        #[cxx_name = "CSV"]
        Csv = 0,
        /// Parquet format.
        #[cxx_name = "PARQUET"]
        Parquet = 1,
        /// ORC format.
        #[cxx_name = "ORC"]
        Orc = 2,
        /// JSON format.
        #[cxx_name = "JSON"]
        Json = 3,
    }

    /// The main data type enumeration used by GraphAr.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(i32)]
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

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(i32)]
    /// Cardinality of a property.
    ///
    /// This defines how multiple values are handled for a given property key.
    enum Cardinality {
        /// A single value.
        #[cxx_name = "SINGLE"]
        Single = 0,
        /// A list of values.
        #[cxx_name = "LIST"]
        List = 1,
        /// A set of values.
        #[cxx_name = "SET"]
        Set = 2,
    }

    /// Adjacency list type.
    ///
    /// This corresponds to GraphAr's `graphar::AdjListType` bit flags.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(i32)]
    enum AdjListType {
        /// Unordered adjacency list by source vertex.
        #[cxx_name = "unordered_by_source"]
        UnorderedBySource = 0b0000_0001,
        /// Unordered adjacency list by destination vertex.
        #[cxx_name = "unordered_by_dest"]
        UnorderedByDest = 0b0000_0010,
        /// Ordered adjacency list by source vertex.
        #[cxx_name = "ordered_by_source"]
        OrderedBySource = 0b0000_0100,
        /// Ordered adjacency list by destination vertex.
        #[cxx_name = "ordered_by_dest"]
        OrderedByDest = 0b0000_1000,
    }

    unsafe extern "C++" {
        type FileType;
        type Type;
        type Cardinality;
        type AdjListType;
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

    // `Property`
    unsafe extern "C++" {
        type Property;

        #[namespace = "graphar_rs"]
        fn new_property(
            name: &CxxString,
            type_: SharedPtr<DataType>,
            is_primary: bool,
            is_nullable: bool,
            cardinality: Cardinality,
        ) -> UniquePtr<Property>;
        #[namespace = "graphar_rs"]
        fn property_get_name(prop: &Property) -> &CxxString;
        #[namespace = "graphar_rs"]
        fn property_get_type(prop: &Property) -> &SharedPtr<DataType>;
        #[namespace = "graphar_rs"]
        fn property_is_primary(prop: &Property) -> bool;
        #[namespace = "graphar_rs"]
        fn property_is_nullable(prop: &Property) -> bool;
        #[namespace = "graphar_rs"]
        fn property_get_cardinality(prop: &Property) -> Cardinality;
        #[namespace = "graphar_rs"]
        fn property_clone(prop: &Property) -> UniquePtr<Property>;

        #[namespace = "graphar_rs"]
        fn property_vec_push_property(
            properties: Pin<&mut CxxVector<Property>>,
            prop: UniquePtr<Property>,
        );

        #[namespace = "graphar_rs"]
        fn property_vec_emplace_property(
            properties: Pin<&mut CxxVector<Property>>,
            name: &CxxString,
            data_type: SharedPtr<DataType>,
            is_primary: bool,
            is_nullable: bool,
            cardinality: Cardinality,
        );

        #[namespace = "graphar_rs"]
        fn property_vec_clone(properties: &CxxVector<Property>) -> UniquePtr<CxxVector<Property>>;
    }

    // `PropertyGroup`
    unsafe extern "C++" {
        type PropertyGroup;

        fn GetProperties(&self) -> &CxxVector<Property>;
        fn HasProperty(&self, property_name: &CxxString) -> bool;

        fn CreatePropertyGroup(
            properties: &CxxVector<Property>,
            file_type: FileType,
            prefix: &CxxString,
        ) -> SharedPtr<PropertyGroup>;

        #[namespace = "graphar_rs"]
        fn property_group_vec_push_property_group(
            property_groups: Pin<&mut CxxVector<SharedPropertyGroup>>,
            property_group: SharedPtr<PropertyGroup>,
        );

        #[namespace = "graphar_rs"]
        fn property_group_vec_clone(
            property_groups: &CxxVector<SharedPropertyGroup>,
        ) -> UniquePtr<CxxVector<SharedPropertyGroup>>;
    }

    // `InfoVersion`
    //
    // TODO: upstream C++ APIs still use `int` in a few places for versioning;
    // prefer fixed-width integer types in the public C++ interface.
    unsafe extern "C++" {
        type InfoVersion;
        type ConstInfoVersion;

        #[namespace = "graphar_rs"]
        fn new_const_info_version(version: i32) -> Result<SharedPtr<ConstInfoVersion>>;
    }

    // `VertexInfo`
    unsafe extern "C++" {
        type VertexInfo;

        fn GetType(&self) -> &CxxString;
        fn GetChunkSize(&self) -> i64;
        fn GetPrefix(&self) -> &CxxString;
        fn version(&self) -> &SharedPtr<ConstInfoVersion>;
        fn GetLabels(&self) -> &CxxVector<CxxString>;

        // TODO: upstream C++ uses `int` for this return type; prefer fixed-width.
        fn PropertyGroupNum(&self) -> usize;

        fn GetPropertyGroups(&self) -> &CxxVector<SharedPropertyGroup>;
        fn GetPropertyGroup(&self, property_name: &CxxString) -> SharedPtr<PropertyGroup>;

        // TODO: upstream C++ uses `int` for this parameter; prefer fixed-width.
        fn GetPropertyGroupByIndex(&self, index: usize) -> SharedPtr<PropertyGroup>;

        #[namespace = "graphar_rs"]
        fn create_vertex_info(
            type_: &CxxString,
            chunk_size: i64,
            property_groups: &CxxVector<SharedPropertyGroup>,
            labels: &Vec<String>,
            prefix: &CxxString,
            version: SharedPtr<ConstInfoVersion>,
        ) -> Result<SharedPtr<VertexInfo>>;

        #[namespace = "graphar_rs"]
        fn vertex_info_save(vertex_info: &VertexInfo, path: &CxxString) -> Result<()>;

        #[namespace = "graphar_rs"]
        fn vertex_info_dump(vertex_info: &VertexInfo) -> Result<UniquePtr<CxxString>>;
    }

    // `AdjacentList`
    unsafe extern "C++" {
        type AdjacentList;

        fn GetType(&self) -> AdjListType;
        fn GetFileType(&self) -> FileType;
        fn GetPrefix(&self) -> &CxxString;

        fn CreateAdjacentList(
            type_: AdjListType,
            file_type: FileType,
            path_prefix: &CxxString,
        ) -> SharedPtr<AdjacentList>;
    }

    // `AdjacentListVector`
    unsafe extern "C++" {
        #[namespace = "graphar_rs"]
        fn new_adjacent_list_vec() -> UniquePtr<CxxVector<SharedAdjacentList>>;
        #[namespace = "graphar_rs"]
        fn push_adjacent_list(
            vec: Pin<&mut CxxVector<SharedAdjacentList>>,
            adjacent_list: SharedPtr<AdjacentList>,
        );
    }

    // `EdgeInfo`
    unsafe extern "C++" {
        type EdgeInfo;

        fn GetSrcType(&self) -> &CxxString;
        fn GetEdgeType(&self) -> &CxxString;
        fn GetDstType(&self) -> &CxxString;
        fn GetChunkSize(&self) -> i64;
        fn GetSrcChunkSize(&self) -> i64;
        fn GetDstChunkSize(&self) -> i64;
        fn GetPrefix(&self) -> &CxxString;
        fn IsDirected(&self) -> bool;
        fn version(&self) -> &SharedPtr<ConstInfoVersion>;
        fn HasAdjacentListType(&self, adj_list_type: AdjListType) -> bool;
        fn GetAdjacentList(&self, adj_list_type: AdjListType) -> SharedPtr<AdjacentList>;

        fn PropertyGroupNum(&self) -> usize;
        fn GetPropertyGroups(&self) -> &CxxVector<SharedPropertyGroup>;
        fn GetPropertyGroup(&self, property: &CxxString) -> SharedPtr<PropertyGroup>;
        fn GetPropertyGroupByIndex(&self, index: usize) -> SharedPtr<PropertyGroup>;

        #[namespace = "graphar_rs"]
        #[allow(clippy::too_many_arguments)]
        fn create_edge_info(
            src_type: &CxxString,
            edge_type: &CxxString,
            dst_type: &CxxString,
            chunk_size: i64,
            src_chunk_size: i64,
            dst_chunk_size: i64,
            directed: bool,
            adjacent_lists: &CxxVector<SharedAdjacentList>,
            property_groups: &CxxVector<SharedPropertyGroup>,
            path_prefix: &CxxString,
            version: SharedPtr<ConstInfoVersion>,
        ) -> Result<SharedPtr<EdgeInfo>>;

        #[namespace = "graphar_rs"]
        fn edge_info_save(edge_info: &EdgeInfo, path: &CxxString) -> Result<()>;
        #[namespace = "graphar_rs"]
        fn edge_info_dump(edge_info: &EdgeInfo) -> Result<UniquePtr<CxxString>>;
    }

    unsafe extern "C++" {
        type SharedPropertyGroup = crate::ffi::SharedPropertyGroup;
    }
    impl CxxVector<SharedPropertyGroup> {}

    unsafe extern "C++" {
        type SharedAdjacentList = crate::ffi::SharedAdjacentList;
    }
    impl CxxVector<SharedAdjacentList> {}
}
