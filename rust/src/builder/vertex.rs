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

use std::path::Path;
use std::pin::Pin;

use cxx::{UniquePtr, let_cxx_string};

use crate::builder::property_value::sealed;
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
                    paste::paste! {
                        ffi::graphar::[<vertex_add_property_ $ty>](vertex.pin_mut(), &name, self);
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
        ffi::graphar::vertex_add_property_string(vertex.pin_mut(), &name, &val);
    }
}

impl<'a> sealed::Sealed<Vertex> for &'a str {}
impl<'a> PropertyValue<Vertex> for &'a str {
    fn add_to(self, vertex: &mut Vertex, name: &str) {
        let_cxx_string!(name = name);
        let_cxx_string!(val = self);
        ffi::graphar::vertex_add_property_string(vertex.pin_mut(), &name, &val);
    }
}

impl Vertex {
    /// Create a new vertex record.
    pub fn new() -> Self {
        Self(ffi::graphar::new_vertex_builder())
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
    /// `path_prefix` is an absolute filesystem path used to store chunks.
    ///
    /// Note: `path_prefix` must be valid UTF-8. On Unix, paths can contain
    /// arbitrary bytes; such non-UTF8 paths return [`crate::Error::NonUtf8Path`].
    pub fn try_new<P: AsRef<Path>>(
        vertex_info: &VertexInfo,
        path_prefix: P,
        start_idx: i64,
    ) -> crate::Result<Self> {
        let prefix_str = crate::path_to_utf8_str(path_prefix.as_ref())?;
        let_cxx_string!(prefix = prefix_str);
        let inner = ffi::graphar::new_vertices_builder(&vertex_info.0, &prefix, start_idx)?;
        Ok(Self(inner))
    }

    /// Create a new vertices builder.
    ///
    /// Panics if the inputs are rejected by GraphAr. Prefer [`VerticesBuilder::try_new`]
    /// if you want to handle errors.
    pub fn new<P: AsRef<Path>>(vertex_info: &VertexInfo, path_prefix: P, start_idx: i64) -> Self {
        Self::try_new(vertex_info, path_prefix, start_idx).unwrap()
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
