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

//! GraphAr edge metadata.

use std::path::Path;

use cxx::{CxxVector, SharedPtr, UniquePtr, let_cxx_string};

use crate::ffi;
use crate::info::{AdjListType, AdjacentList, AdjacentListVector, InfoVersion};
use crate::property::{PropertyGroup, PropertyGroupVector};

/// An edge definition in the GraphAr metadata.
#[derive(Clone)]
pub struct EdgeInfo(pub(crate) SharedPtr<ffi::graphar::EdgeInfo>);

impl EdgeInfo {
    /// Create a builder for [`EdgeInfo`].
    ///
    /// This is the preferred API when constructing `EdgeInfo` in Rust, since
    /// the raw constructor has many parameters.
    pub fn builder(
        src_type: impl Into<String>,
        edge_type: impl Into<String>,
        dst_type: impl Into<String>,
        chunk_size: i64,
        src_chunk_size: i64,
        dst_chunk_size: i64,
    ) -> EdgeInfoBuilder {
        EdgeInfoBuilder::new(
            src_type,
            edge_type,
            dst_type,
            chunk_size,
            src_chunk_size,
            dst_chunk_size,
        )
    }

    /// Create a new edge definition.
    ///
    /// The `prefix` is a logical prefix string used by GraphAr (it is not a filesystem path).
    ///
    /// Panics if GraphAr rejects the inputs (including, but not limited to,
    /// empty type names, non-positive chunk sizes, or empty adjacency list
    /// vector). Prefer [`EdgeInfo::try_new`] if you want to handle errors.
    #[allow(clippy::too_many_arguments)]
    pub fn new<P: AsRef<str>>(
        src_type: &str,
        edge_type: &str,
        dst_type: &str,
        chunk_size: i64,
        src_chunk_size: i64,
        dst_chunk_size: i64,
        directed: bool,
        adjacent_lists: AdjacentListVector,
        property_groups: PropertyGroupVector,
        prefix: P,
        version: Option<InfoVersion>,
    ) -> Self {
        Self::try_new(
            src_type,
            edge_type,
            dst_type,
            chunk_size,
            src_chunk_size,
            dst_chunk_size,
            directed,
            adjacent_lists,
            property_groups,
            prefix,
            version,
        )
        .unwrap()
    }

    /// Try to create a new edge definition.
    ///
    /// This returns an error if any required field is invalid (e.g. empty type
    /// names, non-positive chunk sizes, or empty adjacency list vector), or if
    /// the upstream GraphAr implementation rejects the inputs.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new<P: AsRef<str>>(
        src_type: &str,
        edge_type: &str,
        dst_type: &str,
        chunk_size: i64,
        src_chunk_size: i64,
        dst_chunk_size: i64,
        directed: bool,
        adjacent_lists: AdjacentListVector,
        property_groups: PropertyGroupVector,
        prefix: P,
        version: Option<InfoVersion>,
    ) -> crate::Result<Self> {
        let_cxx_string!(src = src_type);
        let_cxx_string!(edge = edge_type);
        let_cxx_string!(dst = dst_type);
        let_cxx_string!(prefix = prefix.as_ref());

        let prop_groups = property_groups.as_ref();
        let version = version.map(|v| v.0).unwrap_or_else(SharedPtr::null);

        let inner = ffi::graphar::create_edge_info(
            &src,
            &edge,
            &dst,
            chunk_size,
            src_chunk_size,
            dst_chunk_size,
            directed,
            adjacent_lists.as_ref(),
            prop_groups,
            &prefix,
            version,
        )?;
        Ok(Self(inner))
    }

    /// Return the source vertex type.
    pub fn src_type(&self) -> String {
        self.0.GetSrcType().to_string()
    }

    /// Return the edge type.
    pub fn edge_type(&self) -> String {
        self.0.GetEdgeType().to_string()
    }

    /// Return the destination vertex type.
    pub fn dst_type(&self) -> String {
        self.0.GetDstType().to_string()
    }

    /// Return the global chunk size.
    pub fn chunk_size(&self) -> i64 {
        self.0.GetChunkSize()
    }

    /// Return the chunk size for the source vertex type.
    pub fn src_chunk_size(&self) -> i64 {
        self.0.GetSrcChunkSize()
    }

    /// Return the chunk size for the destination vertex type.
    pub fn dst_chunk_size(&self) -> i64 {
        self.0.GetDstChunkSize()
    }

    /// Return the logical prefix.
    pub fn prefix(&self) -> String {
        self.0.GetPrefix().to_string()
    }

    /// Return whether this edge is directed.
    pub fn is_directed(&self) -> bool {
        self.0.IsDirected()
    }

    /// Return the optional info version.
    pub fn version(&self) -> Option<InfoVersion> {
        let sp = self.0.version();
        if sp.is_null() {
            None
        } else {
            Some(InfoVersion(sp.clone()))
        }
    }

    /// Return whether this edge has the given adjacency list type.
    pub fn has_adjacent_list_type(&self, adj_list_type: AdjListType) -> bool {
        self.0.HasAdjacentListType(adj_list_type)
    }

    /// Return the adjacency list definition of the given type.
    ///
    /// Returns `None` if the adjacency list type is not found.
    pub fn adjacent_list(&self, adj_list_type: AdjListType) -> Option<AdjacentList> {
        let sp = self.0.GetAdjacentList(adj_list_type);
        if sp.is_null() {
            None
        } else {
            Some(AdjacentList::from_inner(sp))
        }
    }

    /// Return the number of property groups.
    ///
    /// TODO: upstream C++ uses `int` for this return type; prefer fixed-width.
    pub fn property_group_num(&self) -> usize {
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
    /// This allocates a new `Vec`. Prefer [`EdgeInfo::property_groups_iter`]
    /// if you only need to iterate.
    pub fn property_groups(&self) -> Vec<PropertyGroup> {
        self.property_groups_iter().collect()
    }

    /// Iterate over property groups without allocating a `Vec`.
    pub fn property_groups_iter(&self) -> impl Iterator<Item = PropertyGroup> + '_ {
        self.0.GetPropertyGroups().iter().cloned()
    }

    /// Return the property group containing the given property name.
    ///
    /// Returns `None` if the property is not found.
    pub fn property_group<S: AsRef<str>>(&self, property_name: S) -> Option<PropertyGroup> {
        let_cxx_string!(name = property_name.as_ref());
        let sp = self.0.GetPropertyGroup(&name);
        if sp.is_null() {
            None
        } else {
            Some(PropertyGroup::from_inner(sp))
        }
    }

    /// Return the property group at the given index.
    ///
    /// Returns `None` if the index is out of range.
    ///
    /// TODO: upstream C++ uses `int` for this parameter; prefer fixed-width.
    pub fn property_group_by_index(&self, index: usize) -> Option<PropertyGroup> {
        let sp = self.0.GetPropertyGroupByIndex(index);
        if sp.is_null() {
            None
        } else {
            Some(PropertyGroup::from_inner(sp))
        }
    }

    /// Save this edge definition as a YAML file.
    ///
    /// Note: `path` must be valid UTF-8. On Unix, paths can contain arbitrary
    /// bytes; such non-UTF8 paths return [`crate::Error::NonUtf8Path`].
    pub fn save<P: AsRef<Path>>(&self, path: P) -> crate::Result<()> {
        let path_str = crate::path_to_utf8_str(path.as_ref())?;
        let_cxx_string!(p = path_str);
        ffi::graphar::edge_info_save(&self.0, &p)?;
        Ok(())
    }

    /// Dump this edge definition to a YAML string.
    pub fn dump(&self) -> crate::Result<String> {
        let dumped: UniquePtr<cxx::CxxString> = ffi::graphar::edge_info_dump(&self.0)?;
        let dumped = dumped.as_ref().expect("edge info dump should be valid");
        Ok(dumped.to_string())
    }
}

/// A builder for constructing an [`EdgeInfo`].
///
/// This builder is intended to reduce the argument noise of [`EdgeInfo::new`],
/// while keeping the resulting `EdgeInfo` construction explicit and readable.
///
/// Note: You must supply at least one adjacent list (via
/// [`EdgeInfoBuilder::push_adjacent_list`] or [`EdgeInfoBuilder::adjacent_lists`])
/// before calling [`EdgeInfoBuilder::build`] / [`EdgeInfoBuilder::try_build`].
/// If the adjacent list vector is empty, GraphAr will reject the inputs:
/// `try_build` returns an error and `build` panics.
///
/// Defaults:
/// - `directed = false`
/// - `adjacent_lists = []` (must be non-empty before `build` / `try_build`)
/// - `property_groups = []`
/// - `prefix = ""` (GraphAr may set a default prefix based on type names)
/// - `version = None`
pub struct EdgeInfoBuilder {
    src_type: String,
    edge_type: String,
    dst_type: String,
    chunk_size: i64,
    src_chunk_size: i64,
    dst_chunk_size: i64,
    directed: bool,
    adjacent_lists: AdjacentListVector,
    property_groups: PropertyGroupVector,
    prefix: String,
    version: Option<InfoVersion>,
}

impl EdgeInfoBuilder {
    /// Create a new builder with required fields.
    pub fn new(
        src_type: impl Into<String>,
        edge_type: impl Into<String>,
        dst_type: impl Into<String>,
        chunk_size: i64,
        src_chunk_size: i64,
        dst_chunk_size: i64,
    ) -> Self {
        Self {
            src_type: src_type.into(),
            edge_type: edge_type.into(),
            dst_type: dst_type.into(),
            chunk_size,
            src_chunk_size,
            dst_chunk_size,
            directed: false,
            adjacent_lists: AdjacentListVector::new(),
            property_groups: PropertyGroupVector::new(),
            prefix: String::new(),
            version: None,
        }
    }

    /// Set whether this edge is directed.
    pub fn directed(mut self, directed: bool) -> Self {
        self.directed = directed;
        self
    }

    /// Set the logical prefix.
    ///
    /// This is a logical prefix string used by GraphAr (it is not a filesystem path).
    pub fn prefix<P: AsRef<str>>(mut self, prefix: P) -> Self {
        self.prefix = prefix.as_ref().to_owned();
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

    /// Push an adjacent list definition.
    pub fn push_adjacent_list(mut self, adjacent_list: AdjacentList) -> Self {
        self.adjacent_lists.push(adjacent_list);
        self
    }

    /// Replace adjacent lists with the given vector.
    pub fn adjacent_lists(mut self, adjacent_lists: AdjacentListVector) -> Self {
        self.adjacent_lists = adjacent_lists;
        self
    }

    /// Push a property group definition.
    pub fn push_property_group(mut self, property_group: PropertyGroup) -> Self {
        self.property_groups.push(property_group);
        self
    }

    /// Replace property groups with the given vector.
    pub fn property_groups(mut self, property_groups: PropertyGroupVector) -> Self {
        self.property_groups = property_groups;
        self
    }

    /// Build an [`EdgeInfo`].
    ///
    /// Panics if GraphAr rejects the builder inputs. Prefer
    /// [`EdgeInfoBuilder::try_build`] if you want to handle errors.
    pub fn build(self) -> EdgeInfo {
        self.try_build().unwrap()
    }

    /// Try to build an [`EdgeInfo`].
    pub fn try_build(self) -> crate::Result<EdgeInfo> {
        let Self {
            src_type,
            edge_type,
            dst_type,
            chunk_size,
            src_chunk_size,
            dst_chunk_size,
            directed,
            adjacent_lists,
            property_groups,
            prefix,
            version,
        } = self;

        EdgeInfo::try_new(
            &src_type,
            &edge_type,
            &dst_type,
            chunk_size,
            src_chunk_size,
            dst_chunk_size,
            directed,
            adjacent_lists,
            property_groups,
            prefix.as_str(),
            version,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::property::{PropertyBuilder, PropertyGroup, PropertyGroupVector, PropertyVec};
    use crate::types::{DataType, FileType};
    use tempfile::tempdir;

    fn make_adjacent_lists() -> AdjacentListVector {
        let mut lists = AdjacentListVector::new();
        lists.push(AdjacentList::new(
            AdjListType::UnorderedBySource,
            FileType::Parquet,
            Some("adj/"),
        ));
        lists
    }

    fn make_property_groups() -> PropertyGroupVector {
        let mut props = PropertyVec::new();
        props.emplace(PropertyBuilder::new("id", DataType::int64()).primary_key(true));
        props.emplace(PropertyBuilder::new("weight", DataType::float64()));

        let pg = PropertyGroup::new(props, FileType::Parquet, "props/");
        let mut groups = PropertyGroupVector::new();
        groups.push(pg);
        groups
    }

    #[test]
    fn test_edge_info_clone_and_lookup_inputs() {
        let edge_info = EdgeInfoBuilder::new("person", "knows", "person", 1024, 100, 100)
            .adjacent_lists(make_adjacent_lists())
            .property_groups(make_property_groups())
            .prefix("edge/person_knows_person/")
            .build();

        let cloned = edge_info.clone();
        assert_eq!(cloned.src_type(), "person");
        assert_eq!(cloned.edge_type(), "knows");
        assert_eq!(cloned.dst_type(), "person");
        assert_eq!(cloned.prefix(), "edge/person_knows_person/");

        assert!(cloned.property_group(String::from("id")).is_some());
        assert!(cloned.property_group(String::from("missing")).is_none());
    }

    #[test]
    fn test_edge_info_builder_basic() {
        let edge_info = EdgeInfoBuilder::new("person", "knows", "person", 1024, 100, 100)
            .directed(true)
            .prefix("knows/")
            .version(InfoVersion::new(1).unwrap())
            .adjacent_lists(make_adjacent_lists())
            .property_groups(make_property_groups())
            .build();

        assert_eq!(edge_info.src_type(), "person");
        assert_eq!(edge_info.edge_type(), "knows");
        assert_eq!(edge_info.dst_type(), "person");
        assert_eq!(edge_info.chunk_size(), 1024);
        assert_eq!(edge_info.src_chunk_size(), 100);
        assert_eq!(edge_info.dst_chunk_size(), 100);
        assert!(edge_info.is_directed());
        assert_eq!(edge_info.prefix(), "knows/");
        assert!(edge_info.version().is_some());
        assert!(edge_info.has_adjacent_list_type(AdjListType::UnorderedBySource));
        assert!(
            edge_info
                .adjacent_list(AdjListType::UnorderedBySource)
                .is_some()
        );
        assert!(
            edge_info
                .adjacent_list(AdjListType::OrderedBySource)
                .is_none()
        );
        assert_eq!(edge_info.property_group_num(), 1);
        assert!(edge_info.property_group("id").is_some());
        assert!(edge_info.property_group("missing").is_none());
        assert!(edge_info.property_group_by_index(0).is_some());
        assert!(edge_info.property_group_by_index(1).is_none());
        assert!(edge_info.property_group_by_index(usize::MAX).is_none());
    }

    #[test]
    fn test_edge_info_builder_aliases_and_push_helpers() {
        let version = InfoVersion::new(1).unwrap();
        let pg_vec = make_property_groups();
        let pg = pg_vec.get(0).unwrap().clone();

        let mut adj = AdjacentListVector::new();
        adj.push(AdjacentList::new(
            AdjListType::OrderedBySource,
            FileType::Parquet,
            None::<&str>,
        ));
        let list = AdjacentList::new(
            AdjListType::UnorderedBySource,
            FileType::Parquet,
            Some("u/"),
        );

        let edge_info = EdgeInfo::builder("person", "knows", "person", 1024, 100, 100)
            .directed(false)
            .prefix("edge/person_knows_person/")
            .version_opt(Some(version))
            .push_adjacent_list(list)
            .adjacent_lists(adj)
            .push_property_group(pg)
            .property_groups(pg_vec)
            .build();

        assert_eq!(edge_info.prefix(), "edge/person_knows_person/");

        // Covered API that exposes cxx types.
        let groups_cxx = edge_info.property_groups_cxx();
        assert_eq!(groups_cxx.len(), 1);
        assert!(groups_cxx.get(0).unwrap().has_property("id"));
    }

    #[test]
    fn test_edge_info_builder_try_build_error_paths() {
        // adjacent_lists cannot be empty
        let err = EdgeInfoBuilder::new("person", "knows", "person", 1024, 100, 100)
            .property_groups(make_property_groups())
            .try_build()
            .err()
            .unwrap();
        let msg = err.to_string();
        assert!(
            msg.contains("CreateEdgeInfo") && msg.contains("adjacent_lists must not be empty"),
            "unexpected error message: {msg:?}"
        );
    }

    #[test]
    fn test_edge_info_property_groups_roundtrip() {
        let edge_info = EdgeInfoBuilder::new("person", "knows", "person", 1024, 100, 100)
            .adjacent_lists(make_adjacent_lists())
            .property_groups(make_property_groups())
            .build();

        assert!(edge_info.version().is_none());

        let groups = edge_info.property_groups();
        assert_eq!(groups.len(), 1);
        assert!(groups[0].has_property("id"));
        assert!(groups[0].has_property("weight"));
    }

    #[test]
    fn test_edge_info_dump_and_save() {
        let edge_info = EdgeInfoBuilder::new("person", "knows", "person", 1024, 100, 100)
            .directed(true)
            .adjacent_lists(make_adjacent_lists())
            .property_groups(make_property_groups())
            .prefix("edge/person_knows_person/")
            .build();

        let dumped = edge_info.dump().unwrap();
        assert!(!dumped.trim().is_empty(), "dumped={dumped:?}");
        assert!(dumped.contains("person"), "dumped={dumped:?}");
        assert!(dumped.contains("knows"), "dumped={dumped:?}");
        assert!(
            dumped.contains("edge/person_knows_person/"),
            "dumped={dumped:?}"
        );

        let dir = tempdir().unwrap();
        let path = dir.path().join("edge_info.yaml");
        edge_info.save(&path).unwrap();

        let metadata = std::fs::metadata(&path).unwrap();
        assert!(metadata.is_file());
        assert!(metadata.len() > 0);

        let saved = std::fs::read_to_string(&path).unwrap();
        assert!(!saved.trim().is_empty(), "saved={saved:?}");
        assert!(saved.contains("person"), "saved={saved:?}");
        assert!(saved.contains("knows"), "saved={saved:?}");
    }

    #[cfg(unix)]
    #[test]
    fn test_edge_info_save_non_utf8_path() {
        use std::os::unix::ffi::OsStringExt;

        let edge_info = EdgeInfoBuilder::new("person", "knows", "person", 1024, 100, 100)
            .adjacent_lists(make_adjacent_lists())
            .property_groups(make_property_groups())
            .build();

        let dir = tempdir().unwrap();
        let mut path = dir.path().to_path_buf();
        path.push(std::ffi::OsString::from_vec(
            b"edge_info_\xFF.yaml".to_vec(),
        ));

        let err = edge_info.save(&path).err().unwrap();
        assert!(
            matches!(err, crate::Error::NonUtf8Path(_)),
            "unexpected error: {err:?}"
        );
        assert!(std::fs::metadata(&path).is_err());
    }

    #[test]
    fn test_edge_info_try_new_error_paths() {
        let version = InfoVersion::new(1).unwrap();

        // src_type cannot be empty
        let msg = EdgeInfo::try_new(
            "",
            "knows",
            "person",
            1,
            1,
            1,
            true,
            make_adjacent_lists(),
            make_property_groups(),
            "",
            Some(version.clone()),
        )
        .err()
        .unwrap()
        .to_string();
        assert!(
            msg.contains("CreateEdgeInfo") && msg.contains("src_type must not be empty"),
            "unexpected error message: {msg:?}"
        );

        // `chunk_size` cannot be less than 1
        let msg = EdgeInfo::try_new(
            "person",
            "knows",
            "person",
            0,
            1,
            1,
            true,
            make_adjacent_lists(),
            make_property_groups(),
            "",
            Some(version.clone()),
        )
        .err()
        .unwrap()
        .to_string();
        assert!(
            msg.contains("CreateEdgeInfo") && msg.contains("chunk_size must be > 0"),
            "unexpected error message: {msg:?}"
        );

        // adjacent_lists cannot be empty
        let msg = EdgeInfo::try_new(
            "person",
            "knows",
            "person",
            1,
            1,
            1,
            true,
            AdjacentListVector::new(),
            make_property_groups(),
            "",
            Some(version),
        )
        .err()
        .unwrap()
        .to_string();
        assert!(
            msg.contains("CreateEdgeInfo") && msg.contains("adjacent_lists must not be empty"),
            "unexpected error message: {msg:?}"
        );
    }

    #[test]
    #[should_panic]
    fn test_edge_info_new_panics_on_invalid_args() {
        let version = InfoVersion::new(1).unwrap();
        let _ = EdgeInfo::new(
            "",
            "knows",
            "person",
            1,
            1,
            1,
            true,
            make_adjacent_lists(),
            make_property_groups(),
            "",
            Some(version),
        );
    }
}
