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

//! GraphAr writer builders.

use std::pin::Pin;

use cxx::{UniquePtr, let_cxx_string};

use crate::builder::property_value::sealed;
use crate::types::Cardinality;
use crate::{builder::PropertyValue, ffi, info::VertexInfo};

/// A vertex record being constructed for use with [`VerticesBuilder`].
///
/// This is a thin wrapper around GraphAr's `graphar::builder::Vertex`.
pub struct Vertex(pub(crate) UniquePtr<ffi::graphar::BuilderVertex>);

macro_rules! impl_vertex_property_value {
    ($($ty:tt),+ $(,)?) => {
        $(
            impl sealed::Sealed<Vertex> for $ty {}

            impl PropertyValue<Vertex> for $ty {
                fn add_to(self, vertex: &mut Vertex, name: &str) {
                    let_cxx_string!(name = name);
                    pastey::paste! {
                        ffi::graphar::[<vertex_builder_add_property_ $ty>](vertex.pin_mut(), &name, self);
                    }
                }

                fn add_to_with_cardinality(
                    self,
                    vertex: &mut Vertex,
                    name: &str,
                    cardinality: Cardinality,
                ) {
                    let_cxx_string!(name = name);
                    pastey::paste! {
                        ffi::graphar::[<vertex_builder_add_property_ $ty _with_cardinality>](
                            vertex.pin_mut(),
                            cardinality,
                            &name,
                            self,
                        );
                    }
                }
            }
        )+
    };
}

impl_vertex_property_value!(bool, i32, i64, f32, f64);

impl sealed::Sealed<Vertex> for String {}
impl PropertyValue<Vertex> for String {
    fn add_to(self, vertex: &mut Vertex, name: &str) {
        let_cxx_string!(name = name);
        let_cxx_string!(val = self.as_str());
        ffi::graphar::vertex_builder_add_property_string(vertex.pin_mut(), &name, &val);
    }

    fn add_to_with_cardinality(self, vertex: &mut Vertex, name: &str, cardinality: Cardinality) {
        let_cxx_string!(name = name);
        let_cxx_string!(val = self.as_str());
        ffi::graphar::vertex_builder_add_property_string_with_cardinality(
            vertex.pin_mut(),
            cardinality,
            &name,
            &val,
        );
    }
}

impl sealed::Sealed<Vertex> for &str {}
impl PropertyValue<Vertex> for &str {
    fn add_to(self, vertex: &mut Vertex, name: &str) {
        let_cxx_string!(name = name);
        let_cxx_string!(val = self);
        ffi::graphar::vertex_builder_add_property_string(vertex.pin_mut(), &name, &val);
    }

    fn add_to_with_cardinality(self, vertex: &mut Vertex, name: &str, cardinality: Cardinality) {
        let_cxx_string!(name = name);
        let_cxx_string!(val = self);
        ffi::graphar::vertex_builder_add_property_string_with_cardinality(
            vertex.pin_mut(),
            cardinality,
            &name,
            &val,
        );
    }
}

impl Vertex {
    pub(crate) fn as_ref(&self) -> &ffi::graphar::BuilderVertex {
        self.0.as_ref().expect("vertex should be valid")
    }

    /// Create a new vertex record.
    pub fn new() -> Self {
        Self(ffi::graphar::new_vertex_builder())
    }

    /// Returns true if this vertex is empty.
    pub fn is_empty(&self) -> bool {
        ffi::graphar::vertex_builder_is_empty(self.as_ref())
    }

    /// Add a property to this vertex.
    ///
    /// Supported types are: `bool`, `i32`, `i64`, `f32`, `f64`, `String`, `&str`.
    pub fn add_property<S, V>(&mut self, name: S, val: V)
    where
        S: AsRef<str>,
        V: PropertyValue<Vertex>,
    {
        val.add_to(self, name.as_ref());
    }

    /// Add a boolean property.
    pub fn add_property_bool<S: AsRef<str>>(&mut self, name: S, val: bool) {
        self.add_property(name, val);
    }

    /// Add an `i32` property.
    pub fn add_property_i32<S: AsRef<str>>(&mut self, name: S, val: i32) {
        self.add_property(name, val);
    }

    /// Add an `i64` property.
    pub fn add_property_i64<S: AsRef<str>>(&mut self, name: S, val: i64) {
        self.add_property(name, val);
    }

    /// Add an `f32` property.
    pub fn add_property_f32<S: AsRef<str>>(&mut self, name: S, val: f32) {
        self.add_property(name, val);
    }

    /// Add an `f64` property.
    pub fn add_property_f64<S: AsRef<str>>(&mut self, name: S, val: f64) {
        self.add_property(name, val);
    }

    /// Add a string property.
    pub fn add_property_string<S: AsRef<str>, V: AsRef<str>>(&mut self, name: S, val: V) {
        self.add_property(name, val.as_ref());
    }

    /// Add a property to this vertex with the specified cardinality.
    ///
    /// Supported types are: `bool`, `i32`, `i64`, `f32`, `f64`, `String`, `&str`.
    pub fn add_property_with_cardinality<S, V>(&mut self, cardinality: Cardinality, name: S, val: V)
    where
        S: AsRef<str>,
        V: PropertyValue<Vertex>,
    {
        val.add_to_with_cardinality(self, name.as_ref(), cardinality);
    }

    /// Returns true if the property is stored as a multi-value (LIST/SET).
    pub fn is_multi_property<S: AsRef<str>>(&self, name: S) -> bool {
        let_cxx_string!(name = name.as_ref());
        ffi::graphar::vertex_builder_is_multi_property(self.as_ref(), &name)
    }

    /// Returns true if this vertex contains a property with the given name.
    pub fn contains_property<S: AsRef<str>>(&self, name: S) -> bool {
        let_cxx_string!(name = name.as_ref());
        ffi::graphar::vertex_builder_contains_property(self.as_ref(), &name)
    }

    /// Append a value into a LIST property.
    pub fn add_property_list_item<S, V>(&mut self, name: S, val: V)
    where
        S: AsRef<str>,
        V: PropertyValue<Vertex>,
    {
        self.add_property_with_cardinality(Cardinality::List, name, val);
    }

    /// Append multiple values into a LIST property.
    pub fn add_property_list<I, V>(&mut self, name: &str, values: I)
    where
        I: IntoIterator<Item = V>,
        V: PropertyValue<Vertex>,
    {
        for v in values {
            self.add_property_list_item(name, v);
        }
    }

    /// Append a value into a SET property.
    pub fn add_property_set_item<S, V>(&mut self, name: S, val: V)
    where
        S: AsRef<str>,
        V: PropertyValue<Vertex>,
    {
        self.add_property_with_cardinality(Cardinality::Set, name, val);
    }

    /// Append multiple values into a SET property.
    pub fn add_property_set<I, V>(&mut self, name: &str, values: I)
    where
        I: IntoIterator<Item = V>,
        V: PropertyValue<Vertex>,
    {
        for v in values {
            self.add_property_set_item(name, v);
        }
    }

    pub(crate) fn pin_mut(&mut self) -> Pin<&mut ffi::graphar::BuilderVertex> {
        self.0.as_mut().expect("vertex should be valid")
    }
}

impl Default for Vertex {
    fn default() -> Self {
        Self::new()
    }
}

/// A high-level builder for writing a collection of vertices.
pub struct VerticesBuilder(pub(crate) UniquePtr<ffi::graphar::VerticesBuilder>);

impl VerticesBuilder {
    /// Create a new vertices builder.
    ///
    /// The `prefix` is a filesystem prefix string used by GraphAr (it is not a [`std::path::Path`]).
    ///
    /// GraphAr expects `prefix` to end with a trailing slash (`/`).
    pub fn try_new<P: AsRef<str>>(
        vertex_info: &VertexInfo,
        prefix: P,
        start_idx: i64,
    ) -> crate::Result<Self> {
        let prefix = prefix.as_ref();
        if !prefix.ends_with('/') {
            return Err(crate::Error::InvalidArgument {
                name: "prefix",
                reason: "prefix must end with '/'".to_string(),
            });
        }
        let_cxx_string!(prefix = prefix);
        let inner = ffi::graphar::new_vertices_builder(&vertex_info.0, &prefix, start_idx)?;
        Ok(Self(inner))
    }

    /// Create a new vertices builder.
    ///
    /// Panics if the inputs are rejected by GraphAr. Prefer [`VerticesBuilder::try_new`]
    /// if you want to handle errors.
    pub fn new<P: AsRef<str>>(vertex_info: &VertexInfo, prefix: P, start_idx: i64) -> Self {
        Self::try_new(vertex_info, prefix, start_idx).unwrap()
    }

    /// Add a vertex into this builder.
    pub fn add_vertex(&mut self, mut vertex: Vertex) -> crate::Result<()> {
        ffi::graphar::add_vertex(self.pin_mut(), vertex.pin_mut())?;
        Ok(())
    }

    /// Dump all currently buffered vertices into chunk files.
    pub fn dump(&mut self) -> crate::Result<()> {
        ffi::graphar::vertices_dump(self.pin_mut())?;
        Ok(())
    }

    pub(crate) fn pin_mut(&mut self) -> Pin<&mut ffi::graphar::VerticesBuilder> {
        self.0.as_mut().expect("vertices builder should be valid")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::info::InfoVersion;
    use crate::property::{Property, PropertyGroup, PropertyGroupVector, PropertyVec};
    use crate::types::{Cardinality, DataType, FileType};
    use tempfile::tempdir;

    fn make_vertex_info() -> VertexInfo {
        let mut props = PropertyVec::new();
        props.push(Property::new(
            "id_i64",
            DataType::int64(),
            true,
            false,
            Cardinality::Single,
        ));
        props.push(Property::new(
            "active_bool",
            DataType::bool(),
            false,
            false,
            Cardinality::Single,
        ));
        props.push(Property::new(
            "age_i32",
            DataType::int32(),
            false,
            false,
            Cardinality::Single,
        ));
        props.push(Property::new(
            "score_f32",
            DataType::float32(),
            false,
            false,
            Cardinality::Single,
        ));
        props.push(Property::new(
            "rating_f64",
            DataType::float64(),
            false,
            false,
            Cardinality::Single,
        ));
        props.push(Property::new(
            "name_string",
            DataType::string(),
            false,
            false,
            Cardinality::Single,
        ));

        let mut groups = PropertyGroupVector::new();
        groups.push(PropertyGroup::new(props, FileType::Csv, ""));

        let ver = Some(InfoVersion::new(1).unwrap());
        VertexInfo::new("person", 4, groups, vec![], "", ver)
    }

    #[test]
    fn test_vertex_add_property_dispatch_primitives() {
        let mut v = Vertex::default();
        assert!(v.is_empty());
        assert!(!v.contains_property("age_i32"));
        v.add_property("id_i64", 1_i64);
        v.add_property("active_bool", true);
        v.add_property("age_i32", 42_i32);
        v.add_property("score_f32", 0.5_f32);
        v.add_property("rating_f64", 9.5_f64);
        assert!(!v.is_empty());
        assert!(!v.is_multi_property("age_i32"));
        assert!(v.contains_property("age_i32"));
    }

    #[test]
    fn test_vertex_add_property_dispatch_string() {
        let mut v = Vertex::default();
        assert!(v.is_empty());
        v.add_property("name_string", "alice");
        v.add_property("name_string", "bob");
        v.add_property_string("name_string", "carol");
        v.add_property_string("name_string", "dave");
        assert!(!v.is_empty());
        assert!(!v.is_multi_property("name_string"));
    }

    #[test]
    fn test_vertex_add_property_wrapper_methods() {
        let mut v = Vertex::new();
        v.add_property_bool("active_bool", true);
        v.add_property_i32("age_i32", 1);
        v.add_property_i64("id_i64", 2);
        v.add_property_f32("score_f32", 1.0);
        v.add_property_f64("rating_f64", 2.0);
        v.add_property_string("name_string", "alice");
        assert!(!v.is_empty());
    }

    #[test]
    fn test_vertex_add_property_with_cardinality() {
        let mut v = Vertex::new();
        assert!(!v.is_multi_property("tags"));
        assert!(!v.contains_property("tags"));

        v.add_property_with_cardinality(Cardinality::List, "tags", "t0");
        assert!(v.is_multi_property("tags"));
        assert!(v.contains_property("tags"));

        v.add_property_list("nums", [1_i64, 2_i64, 3_i64]);
        assert!(v.is_multi_property("nums"));
        assert!(v.contains_property("nums"));

        v.add_property_set("uniq", ["a", "a", "b"]);
        assert!(v.is_multi_property("uniq"));
        assert!(v.contains_property("uniq"));
    }

    #[test]
    fn test_vertex_add_property_with_cardinality_dispatch() {
        let mut v = Vertex::new();

        v.add_property_with_cardinality(Cardinality::Single, "b_single", true);
        assert!(v.contains_property("b_single"));
        assert!(!v.is_multi_property("b_single"));

        v.add_property_with_cardinality(Cardinality::List, "b_list", true);
        assert!(v.contains_property("b_list"));
        assert!(v.is_multi_property("b_list"));

        v.add_property_with_cardinality(Cardinality::Single, "i32_single", 1_i32);
        assert!(v.contains_property("i32_single"));
        assert!(!v.is_multi_property("i32_single"));

        v.add_property_with_cardinality(Cardinality::List, "i32_list", 1_i32);
        assert!(v.contains_property("i32_list"));
        assert!(v.is_multi_property("i32_list"));

        v.add_property_with_cardinality(Cardinality::Single, "i64_single", 1_i64);
        assert!(v.contains_property("i64_single"));
        assert!(!v.is_multi_property("i64_single"));

        v.add_property_with_cardinality(Cardinality::List, "i64_list", 1_i64);
        assert!(v.contains_property("i64_list"));
        assert!(v.is_multi_property("i64_list"));

        v.add_property_with_cardinality(Cardinality::Single, "f32_single", 1.0_f32);
        assert!(v.contains_property("f32_single"));
        assert!(!v.is_multi_property("f32_single"));

        v.add_property_with_cardinality(Cardinality::List, "f32_list", 1.0_f32);
        assert!(v.contains_property("f32_list"));
        assert!(v.is_multi_property("f32_list"));

        v.add_property_with_cardinality(Cardinality::Single, "f64_single", 1.0_f64);
        assert!(v.contains_property("f64_single"));
        assert!(!v.is_multi_property("f64_single"));

        v.add_property_with_cardinality(Cardinality::List, "f64_list", 1.0_f64);
        assert!(v.contains_property("f64_list"));
        assert!(v.is_multi_property("f64_list"));

        v.add_property_with_cardinality(Cardinality::Single, "s_single", "alice");
        assert!(v.contains_property("s_single"));
        assert!(!v.is_multi_property("s_single"));

        v.add_property_with_cardinality(Cardinality::List, "s_list", "alice");
        assert!(v.contains_property("s_list"));
        assert!(v.is_multi_property("s_list"));

        v.add_property_with_cardinality(Cardinality::Single, "string_single", "bob".to_string());
        assert!(v.contains_property("string_single"));
        assert!(!v.is_multi_property("string_single"));

        v.add_property_with_cardinality(Cardinality::Set, "string_set", "bob".to_string());
        assert!(v.contains_property("string_set"));
        assert!(v.is_multi_property("string_set"));
    }

    #[test]
    fn test_vertices_builder_add_and_dump() {
        let info = make_vertex_info();
        let tmp = tempdir().unwrap();
        let prefix = tmp.path().join("vertices");
        std::fs::create_dir_all(&prefix).unwrap();

        let prefix = format!("{}/", prefix.display());
        let mut b = VerticesBuilder::new(&info, prefix.as_str(), 0);

        let mut v = Vertex::new();
        v.add_property("id_i64", 1_i64);
        v.add_property("active_bool", true);
        v.add_property("age_i32", 42_i32);
        v.add_property("score_f32", 0.5_f32);
        v.add_property("rating_f64", 9.5_f64);
        v.add_property("name_string", "alice");
        b.add_vertex(v).unwrap();

        b.dump().unwrap();
        let dir = tmp.path().join("vertices");
        assert!(std::fs::read_dir(&dir).unwrap().next().is_some());
    }

    #[test]
    fn test_vertices_builder_try_new_rejects_prefix_without_trailing_slash() {
        let info = make_vertex_info();
        let tmp = tempdir().unwrap();
        let prefix = format!("{}", tmp.path().display());
        match VerticesBuilder::try_new(&info, prefix.as_str(), 0) {
            Ok(_) => panic!("VerticesBuilder::try_new should reject prefixes without trailing '/'"),
            Err(err) => assert!(matches!(
                err,
                crate::Error::InvalidArgument { name: "prefix", .. }
            )),
        }
    }

    #[test]
    fn test_vertices_builder_try_new_rejects_negative_start_idx() {
        let info = make_vertex_info();
        let tmp = tempdir().unwrap();

        let prefix = format!("{}/", tmp.path().display());
        match VerticesBuilder::try_new(&info, prefix.as_str(), -1) {
            Ok(_) => panic!("VerticesBuilder::try_new should reject negative start_idx"),
            Err(err) => assert!(matches!(err, crate::Error::Cxx(_))),
        }
    }
}
