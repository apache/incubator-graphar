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

//! Vertex metadata bindings.

use super::version::InfoVersion;
use crate::{cxx_string_to_string, ffi, property::PropertyGroup, property::PropertyGroupVector};
use cxx::{CxxVector, SharedPtr, UniquePtr, let_cxx_string};
use std::borrow::Cow;
use std::path::Path;

/// GraphAr vertex metadata (`graphar::VertexInfo`).
#[derive(Clone)]
pub struct VertexInfo(pub(crate) SharedPtr<ffi::graphar::VertexInfo>);

impl VertexInfo {
    /// Create a builder for [`VertexInfo`].
    ///
    /// This is the preferred API when constructing `VertexInfo` in Rust, since
    /// the raw constructor has many parameters.
    pub fn builder<S: Into<String>>(
        r#type: S,
        chunk_size: i64,
        property_groups: PropertyGroupVector,
    ) -> VertexInfoBuilder {
        VertexInfoBuilder::new(r#type, chunk_size, property_groups)
    }

    /// Create a new `VertexInfo`.
    ///
    /// The `prefix` is a logical prefix string used by GraphAr (it is not a
    /// filesystem path).
    ///
    /// Panics if GraphAr rejects the inputs (including, but not limited to,
    /// `type` being empty or `chunk_size <= 0`). Prefer [`VertexInfo::try_new`]
    /// if you want to handle errors.
    pub fn new<S: AsRef<[u8]>, P: AsRef<[u8]>>(
        r#type: S,
        chunk_size: i64,
        property_groups: PropertyGroupVector,
        labels: Vec<String>,
        prefix: P,
        version: Option<InfoVersion>,
    ) -> Self {
        Self::try_new(r#type, chunk_size, property_groups, labels, prefix, version).unwrap()
    }

    /// Try to create a new `VertexInfo`.
    ///
    /// This returns an error if `type` is empty, `chunk_size <= 0`, or if the
    /// upstream GraphAr implementation rejects the inputs.
    pub fn try_new<S: AsRef<[u8]>, P: AsRef<[u8]>>(
        r#type: S,
        chunk_size: i64,
        property_groups: PropertyGroupVector,
        labels: Vec<String>,
        prefix: P,
        version: Option<InfoVersion>,
    ) -> Result<Self, cxx::Exception> {
        let_cxx_string!(ty = r#type.as_ref());
        let_cxx_string!(prefix = prefix);

        let groups_ref = property_groups
            .as_ref()
            .expect("property group vec should be valid");
        let version = version.map(|v| v.0).unwrap_or_else(SharedPtr::null);

        Ok(Self(ffi::graphar::create_vertex_info(
            &ty, chunk_size, groups_ref, &labels, &prefix, version,
        )?))
    }

    /// Return the vertex type name.
    pub fn type_name(&self) -> String {
        cxx_string_to_string(self.0.GetType())
    }

    /// Return the chunk size.
    pub fn chunk_size(&self) -> i64 {
        self.0.GetChunkSize()
    }

    /// Return the logical prefix.
    pub fn prefix(&self) -> String {
        cxx_string_to_string(self.0.GetPrefix())
    }

    /// Return the optional format version.
    pub fn version(&self) -> Option<InfoVersion> {
        let sp = self.0.version();
        if sp.is_null() {
            None
        } else {
            Some(InfoVersion(sp.clone()))
        }
    }

    /// Return the labels of this vertex type.
    pub fn labels(&self) -> Vec<String> {
        let labels = self.0.GetLabels();
        let mut out = Vec::with_capacity(labels.len());
        for label in labels {
            out.push(cxx_string_to_string(label));
        }
        out
    }

    /// Return the underlying label vector.
    ///
    /// This is an advanced API that exposes `cxx` types and ties the returned
    /// reference to the lifetime of `self`.
    pub fn labels_cxx(&self) -> &CxxVector<cxx::CxxString> {
        self.0.GetLabels()
    }

    /// Return the number of property groups.
    ///
    /// TODO: upstream C++ uses `int` for this return type; prefer fixed-width.
    pub fn property_group_num(&self) -> i32 {
        self.0.PropertyGroupNum()
    }

    /// Return property groups.
    ///
    /// This is an advanced API that exposes `cxx` types and ties the returned
    /// reference to the lifetime of `self`.
    pub fn property_groups_cxx(&self) -> &CxxVector<PropertyGroup> {
        self.0.GetPropertyGroups()
    }

    /// Return property groups.
    ///
    /// This allocates a new `Vec`. Prefer [`VertexInfo::property_groups_iter`]
    /// if you only need to iterate.
    pub fn property_groups(&self) -> Vec<PropertyGroup> {
        self.property_groups_iter().collect()
    }

    /// Iterate over property groups without allocating a `Vec`.
    pub fn property_groups_iter(&self) -> impl Iterator<Item = PropertyGroup> + '_ {
        self.0.GetPropertyGroups().iter().cloned()
    }

    /// Return the property group containing the given property.
    ///
    /// Returns `None` if the property is not found.
    pub fn property_group<S: AsRef<[u8]>>(&self, property_name: S) -> Option<PropertyGroup> {
        let_cxx_string!(name = property_name);

        let sp = self.0.GetPropertyGroup(&name);
        if sp.is_null() {
            None
        } else {
            Some(PropertyGroup::from_inner(sp))
        }
    }

    /// Return the property group at the given index.
    ///
    /// This returns an owned [`PropertyGroup`] (backed by a C++ `shared_ptr`)
    /// without allocating a `Vec`.
    ///
    /// If you only need a borrowed reference and want bounds checking, prefer
    /// [`VertexInfo::property_groups_cxx`] and `cxx::CxxVector::get`, or
    /// [`VertexInfo::property_groups_iter`] with `nth`.
    /// TODO: upstream C++ uses `int` for this parameter; prefer fixed-width.
    ///
    /// Returns `None` if the index is out of range.
    pub fn property_group_by_index(&self, index: i32) -> Option<PropertyGroup> {
        let sp = self.0.GetPropertyGroupByIndex(index);
        if sp.is_null() {
            None
        } else {
            Some(PropertyGroup::from_inner(sp))
        }
    }

    /// Save this `VertexInfo` to the given path.
    ///
    /// On Unix, this passes the raw `OsStr` bytes to C++ to avoid lossy UTF-8
    /// conversion. On non-Unix platforms, this falls back to converting the
    /// path to UTF-8 using [`Path::to_string_lossy`].
    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<(), cxx::Exception> {
        let path = path.as_ref();

        #[cfg(unix)]
        let path_bytes: Cow<[u8]> = {
            use std::os::unix::ffi::OsStrExt;
            Cow::Borrowed(path.as_os_str().as_bytes())
        };

        #[cfg(not(unix))]
        let path_bytes: Cow<[u8]> = Cow::Owned(path.to_string_lossy().into_owned().into_bytes());

        let_cxx_string!(p = path_bytes.as_ref());
        ffi::graphar::vertex_info_save(&self.0, &p)?;
        Ok(())
    }

    /// Dump this `VertexInfo` as YAML string.
    pub fn dump(&self) -> Result<String, cxx::Exception> {
        let dumped: UniquePtr<cxx::CxxString> = ffi::graphar::vertex_info_dump(&self.0)?;
        Ok(dumped.to_string())
    }
}

/// A builder for constructing a [`VertexInfo`].
///
/// Defaults:
/// - `labels = []`
/// - `prefix = ""` (GraphAr may set a default prefix based on type)
/// - `version = None`
pub struct VertexInfoBuilder {
    r#type: String,
    chunk_size: i64,
    property_groups: PropertyGroupVector,
    labels: Vec<String>,
    prefix: Vec<u8>,
    version: Option<InfoVersion>,
}

impl VertexInfoBuilder {
    /// Create a new builder with required fields.
    pub fn new<S: Into<String>>(
        r#type: S,
        chunk_size: i64,
        property_groups: PropertyGroupVector,
    ) -> Self {
        Self {
            r#type: r#type.into(),
            chunk_size,
            property_groups,
            labels: Vec::new(),
            prefix: Vec::new(),
            version: None,
        }
    }

    /// Set vertex labels.
    pub fn labels(mut self, labels: Vec<String>) -> Self {
        self.labels = labels;
        self
    }

    /// Set vertex labels from a string iterator.
    pub fn labels_from_iter<I, S>(mut self, labels: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.labels = labels.into_iter().map(|s| s.as_ref().to_string()).collect();
        self
    }

    /// Push a single label.
    pub fn push_label<S: Into<String>>(mut self, label: S) -> Self {
        self.labels.push(label.into());
        self
    }

    /// Set the logical prefix.
    pub fn prefix<P: AsRef<[u8]>>(mut self, prefix: P) -> Self {
        self.prefix = prefix.as_ref().to_vec();
        self
    }

    /// Set the info format version.
    pub fn version(mut self, version: InfoVersion) -> Self {
        self.version = Some(version);
        self
    }

    /// Set the optional info format version.
    pub fn version_opt(mut self, version: Option<InfoVersion>) -> Self {
        self.version = version;
        self
    }

    /// Build a [`VertexInfo`].
    ///
    /// Panics if GraphAr rejects the builder inputs. Prefer [`VertexInfoBuilder::try_build`]
    /// if you want to handle errors.
    pub fn build(self) -> VertexInfo {
        self.try_build().unwrap()
    }

    /// Try to build a [`VertexInfo`].
    pub fn try_build(self) -> Result<VertexInfo, cxx::Exception> {
        let Self {
            r#type,
            chunk_size,
            property_groups,
            labels,
            prefix,
            version,
        } = self;

        VertexInfo::try_new(r#type, chunk_size, property_groups, labels, prefix, version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::property::{PropertyBuilder, PropertyGroup, PropertyVec};
    use crate::types::{DataType, FileType};
    use tempfile::tempdir;

    fn cxx_string_to_string_for_test(s: &cxx::CxxString) -> String {
        String::from_utf8_lossy(s.as_bytes()).into_owned()
    }

    fn make_property_groups() -> PropertyGroupVector {
        let mut props = PropertyVec::new();
        props.emplace(PropertyBuilder::new("id", DataType::int64()).primary_key(true));
        props.emplace(PropertyBuilder::new("name", DataType::string()));

        let pg = PropertyGroup::new(props, FileType::Parquet, "id_name/");
        let mut groups = PropertyGroupVector::new();
        groups.push(pg);
        groups
    }

    #[test]
    fn test_vertex_info_try_new_error_paths() {
        let groups = PropertyGroupVector::new();

        // type cannot be empty and `chunk_size` cannot less than 1.
        assert!(VertexInfo::try_new("", 1, groups.clone(), vec![], "", None).is_err());
        assert!(VertexInfo::try_new("person", 0, groups.clone(), vec![], "", None).is_err());
    }

    #[test]
    #[should_panic]
    fn test_vertex_info_new_panics_on_invalid_args() {
        let groups = PropertyGroupVector::new();
        // type cannot be empty
        let _ = VertexInfo::new("", 1, groups, vec![], "", None);
    }

    #[test]
    fn test_vertex_info_builder() {
        let version = InfoVersion::new(1).unwrap();
        let groups = make_property_groups();

        // Create `VertexInfo` using builder API.
        let vertex_info = VertexInfo::builder("person", 1024, groups)
            .labels(vec!["l0".to_string()])
            .labels_from_iter(["l1", "l2"])
            .push_label("l3")
            .prefix("person/")
            .version(version)
            .build();

        assert_eq!(vertex_info.type_name(), "person");
        assert_eq!(vertex_info.chunk_size(), 1024);
        assert_eq!(vertex_info.prefix(), "person/");

        assert!(vertex_info.version().is_some());

        let labels = vertex_info.labels();
        assert_eq!(
            labels,
            vec!["l1".to_string(), "l2".to_string(), "l3".to_string()]
        );

        let labels_cxx = vertex_info.labels_cxx();
        assert_eq!(labels_cxx.len(), 3);
        assert_eq!(
            cxx_string_to_string_for_test(labels_cxx.get(0).unwrap()),
            "l1"
        );

        assert_eq!(vertex_info.property_group_num(), 1);

        let groups_cxx = vertex_info.property_groups_cxx();
        assert_eq!(groups_cxx.len(), 1);
        assert!(groups_cxx.get(0).unwrap().has_property("id"));

        let groups_vec = vertex_info.property_groups();
        assert_eq!(groups_vec.len(), 1);

        let groups_iter: Vec<_> = vertex_info.property_groups_iter().collect();
        assert_eq!(groups_iter.len(), 1);
    }

    #[test]
    fn test_vertex_info_property_group_lookups() {
        let groups = make_property_groups();

        let vertex_info = VertexInfo::builder("person", 1024, groups)
            .prefix("person/")
            .version_opt(None)
            .build();

        assert!(vertex_info.version().is_none());

        assert!(vertex_info.property_group("id").is_some());
        assert!(vertex_info.property_group("missing").is_none());

        let by_index = vertex_info.property_group_by_index(0).unwrap();
        assert!(by_index.has_property("id"));

        assert!(vertex_info.property_group_by_index(1).is_none());
        assert!(vertex_info.property_group_by_index(-1).is_none());
    }

    #[test]
    fn test_vertex_info_dump_and_save() {
        let groups = make_property_groups();

        let vertex_info = VertexInfo::builder("person", 1024, groups)
            .labels_from_iter(["l1"])
            .prefix("person/")
            .build();

        let dumped = vertex_info.dump().unwrap();
        assert!(!dumped.is_empty());
        println!("{}", dumped);

        let dir = tempdir().unwrap();
        let path = dir.path().join("vertex_info.yaml");
        vertex_info.save(&path).unwrap();

        let metadata = std::fs::metadata(&path).unwrap();
        assert!(metadata.is_file());
        assert!(metadata.len() > 0);
    }

    #[cfg(unix)]
    #[test]
    fn test_vertex_info_save_non_utf8_path() {
        use std::os::unix::ffi::OsStringExt;

        let groups = make_property_groups();
        let vertex_info = VertexInfo::builder("person", 1024, groups)
            .labels_from_iter(["l1"])
            .prefix("person/")
            .build();

        let dir = tempdir().unwrap();

        let mut path = dir.path().to_path_buf();
        path.push(std::ffi::OsString::from_vec(
            b"vertex_info_\xFF_non_utf8.yaml".to_vec(),
        ));

        std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)
            .unwrap();
        std::fs::remove_file(&path).unwrap();

        vertex_info.save(&path).unwrap();
        let metadata = std::fs::metadata(&path).unwrap();
        assert!(metadata.is_file());
        assert!(metadata.len() > 0);
    }
}
