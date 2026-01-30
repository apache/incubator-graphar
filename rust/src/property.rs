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

//! Rust bindings for GraphAr property types.

use std::ops::Deref;
use std::pin::Pin;

use crate::ffi;
use crate::types::{Cardinality, DataType, FileType};
use cxx::{CxxVector, SharedPtr, UniquePtr, let_cxx_string};

/// A property definition in the GraphAr schema.
///
/// This is a thin wrapper around the C++ `graphar::Property`.
pub struct Property(UniquePtr<ffi::graphar::Property>);

impl Property {
    pub(crate) fn as_ref(&self) -> &ffi::graphar::Property {
        self.0.as_ref().expect("property should be valid")
    }

    /// Create a new property definition.
    ///
    /// Note: In upstream GraphAr C++ (`graphar::Property` constructor), a primary key property is
    /// always treated as non-nullable. Concretely, `is_nullable` is forced to `false` when
    /// `is_primary` is `true` (regardless of the provided `is_nullable` value).
    ///
    /// This method takes `data_type` by value. Clone it if you need to reuse it.
    pub fn new<S: AsRef<str>>(
        name: S,
        data_type: DataType,
        is_primary: bool,
        is_nullable: bool,
        cardinality: Cardinality,
    ) -> Self {
        let_cxx_string!(name = name.as_ref());
        Self(ffi::graphar::new_property(
            &name,
            data_type.0,
            is_primary,
            is_nullable,
            cardinality,
        ))
    }

    /// Return the property name.
    pub fn name(&self) -> String {
        ffi::graphar::property_get_name(self.as_ref()).to_string()
    }

    /// Return the property data type.
    pub fn data_type(&self) -> DataType {
        let ty = ffi::graphar::property_get_type(self.as_ref());
        DataType(ty.clone())
    }

    /// Return whether this property is a primary key.
    pub fn is_primary(&self) -> bool {
        ffi::graphar::property_is_primary(self.as_ref())
    }

    /// Return whether this property is nullable.
    ///
    /// Note: In upstream GraphAr C++ (`graphar::Property` constructor), a primary key property is
    /// always treated as non-nullable, so this returns `false` when `is_primary()` is `true`.
    pub fn is_nullable(&self) -> bool {
        ffi::graphar::property_is_nullable(self.as_ref())
    }

    /// Return the cardinality of this property.
    pub fn cardinality(&self) -> Cardinality {
        ffi::graphar::property_get_cardinality(self.as_ref())
    }
}

/// A builder for constructing a [`Property`].
///
/// This builder is primarily intended to be used with [`PropertyVec::emplace`]
/// to avoid allocating an intermediate `graphar::Property` on the C++ heap.
///
/// Defaults:
/// - `is_primary = false`
/// - `is_nullable = true` (matches upstream GraphAr C++ default)
/// - `cardinality = Cardinality::Single`
pub struct PropertyBuilder<S: AsRef<str>> {
    name: S,
    data_type: DataType,
    is_primary: bool,
    is_nullable: bool,
    cardinality: Cardinality,
}

impl<S: AsRef<str>> PropertyBuilder<S> {
    /// Create a new builder with the given name and data type.
    pub fn new(name: S, data_type: DataType) -> Self {
        Self {
            name,
            data_type,
            is_primary: false,
            is_nullable: true,
            cardinality: Cardinality::Single,
        }
    }

    /// Mark whether this property is a primary key.
    pub fn primary_key(mut self, is_primary: bool) -> Self {
        self.is_primary = is_primary;
        self
    }

    /// Mark whether this property is nullable.
    pub fn nullable(mut self, is_nullable: bool) -> Self {
        self.is_nullable = is_nullable;
        self
    }

    /// Set the cardinality of this property.
    pub fn cardinality(mut self, cardinality: Cardinality) -> Self {
        self.cardinality = cardinality;
        self
    }

    /// Build a [`Property`].
    pub fn build(self) -> Property {
        let Self {
            name,
            data_type,
            is_primary,
            is_nullable,
            cardinality,
        } = self;

        let_cxx_string!(name = name.as_ref());
        Property(ffi::graphar::new_property(
            &name,
            data_type.0,
            is_primary,
            is_nullable,
            cardinality,
        ))
    }
}

/// A vector of properties.
///
/// This is a wrapper around a C++ `std::vector<graphar::Property>`.
pub struct PropertyVec(UniquePtr<CxxVector<ffi::graphar::Property>>);

impl Default for PropertyVec {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for PropertyVec {
    fn clone(&self) -> Self {
        Self(ffi::graphar::property_vec_clone(self.as_ref()))
    }
}

impl Deref for PropertyVec {
    type Target = CxxVector<ffi::graphar::Property>;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl PropertyVec {
    /// Create an empty property vector.
    pub fn new() -> Self {
        Self(CxxVector::new())
    }

    /// Push a property into the vector.
    pub fn push(&mut self, property: Property) {
        ffi::graphar::property_vec_push_property(self.pin_mut(), property.0);
    }

    /// Construct and append a property directly in the underlying C++ vector.
    ///
    /// Compared to `push(builder.build())`, this avoids allocating an
    /// intermediate `graphar::Property` on the C++ heap.
    pub fn emplace<S: AsRef<str>>(&mut self, builder: PropertyBuilder<S>) {
        let PropertyBuilder {
            name,
            data_type,
            is_primary,
            is_nullable,
            cardinality,
        } = builder;

        let_cxx_string!(name = name.as_ref());
        ffi::graphar::property_vec_emplace_property(
            self.pin_mut(),
            &name,
            data_type.0,
            is_primary,
            is_nullable,
            cardinality,
        );
    }

    pub(crate) fn as_ref(&self) -> &CxxVector<ffi::graphar::Property> {
        self.0.as_ref().expect("properties vec should be valid")
    }

    /// Borrow the underlying C++ vector mutably.
    ///
    /// Mutating APIs on `cxx::CxxVector` require a pinned mutable reference.
    ///
    /// # Panics
    ///
    /// Panics if the underlying C++ vector pointer is null.
    pub fn pin_mut(&mut self) -> Pin<&mut CxxVector<ffi::graphar::Property>> {
        self.0.as_mut().expect("properties vec should be valid")
    }
}

/// A group of properties stored in the same file(s).
pub type PropertyGroup = ffi::SharedPropertyGroup;

impl PropertyGroup {
    /// Construct a `PropertyGroup` from a raw C++ shared pointer.
    pub(crate) fn from_inner(inner: SharedPtr<ffi::graphar::PropertyGroup>) -> Self {
        Self(inner)
    }

    /// Create a new property group.
    ///
    /// The `prefix` is a logical prefix string used by GraphAr (it is not a
    /// filesystem path).
    pub fn new<S: AsRef<str>>(properties: PropertyVec, file_type: FileType, prefix: S) -> Self {
        let_cxx_string!(prefix = prefix.as_ref());
        let inner = ffi::graphar::CreatePropertyGroup(properties.as_ref(), file_type, &prefix);
        Self(inner)
    }

    /// Return properties contained in this group.
    pub fn properties(&self) -> Vec<Property> {
        let props_cxx = self.0.GetProperties();
        let mut props = Vec::with_capacity(props_cxx.len());
        for prop in props_cxx {
            props.push(Property(ffi::graphar::property_clone(prop)));
        }

        props
    }

    /// Check whether this group contains the given property.
    pub fn has_property(&self, property_name: &str) -> bool {
        let_cxx_string!(name = property_name);

        self.0.HasProperty(&name)
    }
}

/// A vector of property groups.
pub struct PropertyGroupVector(UniquePtr<CxxVector<PropertyGroup>>);

impl Default for PropertyGroupVector {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for PropertyGroupVector {
    fn clone(&self) -> Self {
        Self(ffi::graphar::property_group_vec_clone(self.as_ref()))
    }
}

impl Deref for PropertyGroupVector {
    type Target = CxxVector<PropertyGroup>;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl PropertyGroupVector {
    /// Create an empty property group vector.
    pub fn new() -> Self {
        Self(CxxVector::new())
    }

    /// Push a property group into the vector.
    pub fn push(&mut self, property_group: PropertyGroup) {
        ffi::graphar::property_group_vec_push_property_group(self.pin_mut(), property_group.0);
    }

    pub(crate) fn as_ref(&self) -> &CxxVector<PropertyGroup> {
        self.0.as_ref().expect("property group vec should be valid")
    }

    /// Borrow the underlying C++ vector mutably.
    ///
    /// Mutating APIs on `cxx::CxxVector` require a pinned mutable reference.
    ///
    /// # Panics
    ///
    /// Panics if the underlying C++ vector pointer is null.
    pub fn pin_mut(&mut self) -> Pin<&mut CxxVector<PropertyGroup>> {
        self.0.as_mut().expect("property group vec should be valid")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Type;

    #[test]
    fn test_property_roundtrip_getters() {
        let p = Property::new("name", DataType::string(), false, true, Cardinality::Single);

        assert_eq!(p.name(), "name");
        assert_eq!(p.data_type().id(), Type::String);
        assert!(!p.is_primary());
        assert!(p.is_nullable());
        assert_eq!(p.cardinality(), Cardinality::Single);
    }

    #[test]
    fn test_property_primary_implies_non_nullable_upstream_behavior() {
        // Upstream GraphAr C++ (`graphar::Property` constructor) forces:
        //   is_nullable = !is_primary && is_nullable
        // This test documents that behavior at the Rust binding layer.
        let p = Property::new("id", DataType::int64(), true, true, Cardinality::Single);

        assert!(p.is_primary());
        assert!(!p.is_nullable());
    }

    #[test]
    fn test_property_builder_default() {
        let built = PropertyBuilder::new("id", DataType::int64()).build();
        assert_eq!(built.name(), "id");
        assert_eq!(built.data_type().id(), Type::Int64);
        assert!(!built.is_primary());
        assert!(built.is_nullable());
        assert_eq!(built.cardinality(), Cardinality::Single);
    }

    #[test]
    fn test_property_builder_emplace_options() {
        let mut props = PropertyVec::new();
        props.emplace(
            PropertyBuilder::new("id", DataType::int64())
                .primary_key(true)
                .nullable(true),
        );
        props.emplace(
            PropertyBuilder::new("tags", DataType::string())
                .nullable(true)
                .cardinality(Cardinality::List),
        );

        let pg = PropertyGroup::new(props, FileType::Parquet, "builder/");
        let mut got = pg.properties();
        got.sort_by_key(|a| a.name());

        assert_eq!(got.len(), 2);
        assert_eq!(got[0].name(), "id");
        assert!(got[0].is_primary());
        assert!(!got[0].is_nullable());
        assert_eq!(got[0].cardinality(), Cardinality::Single);

        assert_eq!(got[1].name(), "tags");
        assert!(!got[1].is_primary());
        assert!(got[1].is_nullable());
        assert_eq!(got[1].cardinality(), Cardinality::List);
    }

    #[test]
    fn test_property_vec_deref() {
        let mut vec = PropertyVec::default();
        assert_eq!(vec.len(), 0);
        assert_eq!(vec.as_ref().len(), 0);
        assert!(vec.as_ref().is_empty());
        assert_eq!(vec.as_ref().capacity(), 0);

        vec.pin_mut().reserve(2);
        assert_eq!(vec.as_ref().capacity(), 2);
    }

    #[test]
    fn test_property_vec_and_group_roundtrip() {
        let mut props = PropertyVec::default();
        props.emplace(PropertyBuilder::new("id", DataType::int64()).primary_key(true));
        props.push(Property::new(
            "name",
            DataType::string(),
            false,
            true,
            Cardinality::Single,
        ));

        let pg = PropertyGroup::new(props, FileType::Parquet, "id_name/");
        let mut got_names: Vec<String> = pg.properties().into_iter().map(|p| p.name()).collect();
        got_names.sort();

        assert_eq!(got_names, vec!["id".to_string(), "name".to_string()]);
        assert!(pg.has_property("id"));
        assert!(pg.has_property("name"));
        assert!(!pg.has_property("missing"));
    }

    #[test]
    fn test_property_group_vector_default_and_deref() {
        let mut vec = PropertyGroupVector::default();
        assert_eq!(vec.len(), 0);
        assert_eq!(vec.as_ref().len(), 0);
        assert!(vec.as_ref().is_empty());
        vec.pin_mut().reserve(2);
        assert_eq!(vec.as_ref().capacity(), 2);
    }

    #[test]
    fn test_property_group_vector_push() {
        let mut props1 = PropertyVec::new();
        props1.push(Property::new(
            "id1",
            DataType::int64(),
            true,
            false,
            Cardinality::Single,
        ));

        let mut props2 = PropertyVec::new();
        props2.push(Property::new(
            "id",
            DataType::int64(),
            true,
            false,
            Cardinality::Single,
        ));

        let pg1 = PropertyGroup::new(props1, FileType::Parquet, "pg1/");
        let pg2 = PropertyGroup::new(props2, FileType::Parquet, "pg2/");

        let mut vec = PropertyGroupVector::default();
        assert_eq!(vec.as_ref().len(), 0);
        assert!(vec.as_ref().is_empty());

        vec.push(pg1);
        assert_eq!(vec.as_ref().len(), 1);
        assert!(!vec.as_ref().is_empty());

        vec.push(pg2);
        assert_eq!(vec.as_ref().len(), 2);
    }

    fn make_property_vec_for_clone() -> PropertyVec {
        let mut props = PropertyVec::new();
        props.emplace(PropertyBuilder::new("id", DataType::int64()).primary_key(true));
        props.push(Property::new(
            "name",
            DataType::string(),
            false,
            true,
            Cardinality::Single,
        ));
        props
    }

    #[test]
    fn test_property_vec_clone_independent_container() {
        let mut original = make_property_vec_for_clone();
        let cloned = original.clone();

        assert_eq!(original.as_ref().len(), 2);
        assert_eq!(cloned.as_ref().len(), 2);

        let id_prop = Property::new("id2", DataType::int64(), true, true, Cardinality::Single);
        original.push(id_prop);
        assert_eq!(original.as_ref().len(), 3);

        // Mutating the original container should not affect the cloned one.
        assert_eq!(cloned.as_ref().len(), 2);

        let pg = PropertyGroup::new(cloned, FileType::Parquet, "clone_check/");
        let mut names: Vec<_> = pg.properties().into_iter().map(|p| p.name()).collect();
        names.sort();
        assert_eq!(names, vec!["id".to_string(), "name".to_string()]);
    }

    #[test]
    fn test_property_group_vector_clone_independent_container() {
        let mut props1 = PropertyVec::new();
        props1.emplace(PropertyBuilder::new("id1", DataType::int64()).primary_key(true));
        let pg1 = PropertyGroup::new(props1, FileType::Parquet, "pg1/");

        let mut props2 = PropertyVec::new();
        props2.emplace(PropertyBuilder::new("id2", DataType::int64()).primary_key(true));
        let pg2 = PropertyGroup::new(props2, FileType::Parquet, "pg2/");

        let mut groups = PropertyGroupVector::new();
        groups.push(pg1);
        groups.push(pg2);

        let cloned = groups.clone();
        assert_eq!(groups.as_ref().len(), 2);
        assert_eq!(cloned.as_ref().len(), 2);

        assert!(cloned.as_ref().get(0).unwrap().has_property("id1"));
        assert!(cloned.as_ref().get(1).unwrap().has_property("id2"));

        let mut props3 = PropertyVec::new();
        props3.emplace(PropertyBuilder::new("id3", DataType::int64()).primary_key(true));
        let pg3 = PropertyGroup::new(props3, FileType::Parquet, "pg3/");
        groups.push(pg3);

        assert_eq!(groups.as_ref().len(), 3);
        assert_eq!(cloned.as_ref().len(), 2);

        let cloned_props = cloned.as_ref().get(0).unwrap().properties();
        assert_eq!(cloned_props.len(), 1);
        assert_eq!(cloned_props[0].name(), "id1");
        assert_eq!(cloned_props[0].data_type().id(), Type::Int64);
    }
}
