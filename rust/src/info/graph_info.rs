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

//! GraphAr graph-level metadata bindings.

use std::path::Path;

use cxx::{CxxVector, SharedPtr, UniquePtr, let_cxx_string};

use crate::ffi;

use super::{EdgeInfo, InfoVersion, VertexInfo};

/// Graph-level metadata (`graphar::GraphInfo`).
#[derive(Clone)]
pub struct GraphInfo(pub(crate) SharedPtr<ffi::graphar::GraphInfo>);

impl GraphInfo {
    /// Create a builder for [`GraphInfo`].
    pub fn builder(name: impl Into<String>) -> GraphInfoBuilder {
        GraphInfoBuilder::new(name)
    }

    /// Create a new `GraphInfo`.
    ///
    /// Panics if GraphAr rejects the inputs. Prefer [`GraphInfo::try_new`] if
    /// you want to handle errors.
    pub fn new<N: AsRef<str>, P: AsRef<str>>(
        name: N,
        vertex_infos: Vec<VertexInfo>,
        edge_infos: Vec<EdgeInfo>,
        labels: Vec<String>,
        prefix: P,
        version: Option<InfoVersion>,
    ) -> Self {
        Self::try_new(name, vertex_infos, edge_infos, labels, prefix, version).unwrap()
    }

    /// Try to create a new `GraphInfo`.
    pub fn try_new<N: AsRef<str>, P: AsRef<str>>(
        name: N,
        vertex_infos: Vec<VertexInfo>,
        edge_infos: Vec<EdgeInfo>,
        labels: Vec<String>,
        prefix: P,
        version: Option<InfoVersion>,
    ) -> crate::Result<Self> {
        let_cxx_string!(name = name.as_ref());
        let_cxx_string!(prefix = prefix.as_ref());

        let mut vertex_info_vec = CxxVector::new();
        vertex_info_vec.pin_mut().reserve(vertex_infos.len());
        for vertex_info in vertex_infos {
            ffi::graphar::vertex_info_vec_push_vertex_info(
                vertex_info_vec.pin_mut(),
                vertex_info.0,
            );
        }
        let vertex_info_vec_ref = vertex_info_vec
            .as_ref()
            .expect("vertex info vector should be valid");

        let mut edge_info_vec: UniquePtr<CxxVector<ffi::SharedEdgeInfo>> = CxxVector::new();
        edge_info_vec.pin_mut().reserve(edge_infos.len());
        for edge_info in edge_infos {
            ffi::graphar::edge_info_vec_push_edge_info(edge_info_vec.pin_mut(), edge_info.0);
        }
        let edge_info_vec_ref = edge_info_vec
            .as_ref()
            .expect("edge info vector should be valid");

        let version = version.map(|v| v.0).unwrap_or_else(SharedPtr::null);
        let inner = ffi::graphar::create_graph_info(
            &name,
            vertex_info_vec_ref,
            edge_info_vec_ref,
            &labels,
            &prefix,
            version,
        )?;
        Ok(Self(inner))
    }

    /// Load graph metadata from a YAML file.
    ///
    /// Note: `path` must be valid UTF-8. On Unix, paths can contain arbitrary
    /// bytes; such non-UTF8 paths return [`crate::Error::NonUtf8Path`].
    pub fn load<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let path_str = crate::path_to_utf8_str(path.as_ref())?;
        let_cxx_string!(p = path_str);
        Ok(Self(ffi::graphar::load_graph_info(&p)?))
    }

    /// Return the graph name.
    pub fn name(&self) -> String {
        self.0.GetName().to_string()
    }

    /// Return graph labels.
    pub fn labels(&self) -> Vec<String> {
        let labels = self.0.GetLabels();
        let mut out = Vec::with_capacity(labels.len());
        for label in labels {
            out.push(label.to_string());
        }
        out
    }

    /// Return the logical prefix.
    pub fn prefix(&self) -> String {
        self.0.GetPrefix().to_string()
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

    /// Return the vertex info with the given type.
    ///
    /// Returns `None` if the type is not found.
    pub fn vertex_info<S: AsRef<str>>(&self, r#type: S) -> Option<VertexInfo> {
        let_cxx_string!(ty = r#type.as_ref());
        let sp = self.0.GetVertexInfo(&ty);
        if sp.is_null() {
            None
        } else {
            Some(VertexInfo(sp))
        }
    }

    /// Return the edge info with the given edge triplet.
    ///
    /// Returns `None` if the edge triplet is not found.
    pub fn edge_info<S1: AsRef<str>, S2: AsRef<str>, S3: AsRef<str>>(
        &self,
        src_type: S1,
        edge_type: S2,
        dst_type: S3,
    ) -> Option<EdgeInfo> {
        let_cxx_string!(src = src_type.as_ref());
        let_cxx_string!(edge = edge_type.as_ref());
        let_cxx_string!(dst = dst_type.as_ref());
        let sp = self.0.GetEdgeInfo(&src, &edge, &dst);
        if sp.is_null() {
            None
        } else {
            Some(EdgeInfo(sp))
        }
    }

    /// Return the index of the vertex info with the given type.
    ///
    /// Returns `None` if the type is not found.
    pub fn vertex_info_index<S: AsRef<str>>(&self, r#type: S) -> Option<usize> {
        let_cxx_string!(ty = r#type.as_ref());
        ffi::graphar::graph_info_vertex_info_index(&self.0, &ty).into()
    }

    /// Return the index of the edge info with the given edge triplet.
    ///
    /// Returns `None` if the edge triplet is not found.
    pub fn edge_info_index<S1: AsRef<str>, S2: AsRef<str>, S3: AsRef<str>>(
        &self,
        src_type: S1,
        edge_type: S2,
        dst_type: S3,
    ) -> Option<usize> {
        let_cxx_string!(src = src_type.as_ref());
        let_cxx_string!(edge = edge_type.as_ref());
        let_cxx_string!(dst = dst_type.as_ref());
        ffi::graphar::graph_info_edge_info_index(&self.0, &src, &edge, &dst).into()
    }

    /// Return the number of vertex infos.
    pub fn vertex_info_num(&self) -> usize {
        self.0.VertexInfoNum()
    }

    /// Return the number of edge infos.
    pub fn edge_info_num(&self) -> usize {
        self.0.EdgeInfoNum()
    }

    /// Return vertex infos.
    ///
    /// This allocates a new `Vec`. Prefer [`GraphInfo::vertex_infos_iter`]
    /// if you only need to iterate.
    pub fn vertex_infos(&self) -> Vec<VertexInfo> {
        self.vertex_infos_iter().collect()
    }

    /// Iterate over vertex infos without allocating a `Vec`.
    pub fn vertex_infos_iter(&self) -> impl Iterator<Item = VertexInfo> + '_ {
        self.0
            .GetVertexInfos()
            .iter()
            .map(|item| VertexInfo(item.0.clone()))
    }

    /// Return edge infos.
    ///
    /// This allocates a new `Vec`. Prefer [`GraphInfo::edge_infos_iter`]
    /// if you only need to iterate.
    pub fn edge_infos(&self) -> Vec<EdgeInfo> {
        self.edge_infos_iter().collect()
    }

    /// Iterate over edge infos without allocating a `Vec`.
    pub fn edge_infos_iter(&self) -> impl Iterator<Item = EdgeInfo> + '_ {
        self.0
            .GetEdgeInfos()
            .iter()
            .map(|item| EdgeInfo(item.0.clone()))
    }

    /// Save this `GraphInfo` to the given path.
    ///
    /// Note: `path` must be valid UTF-8. On Unix, paths can contain arbitrary
    /// bytes; such non-UTF8 paths return [`crate::Error::NonUtf8Path`].
    pub fn save<P: AsRef<Path>>(&self, path: P) -> crate::Result<()> {
        let path_str = crate::path_to_utf8_str(path.as_ref())?;
        let_cxx_string!(p = path_str);
        ffi::graphar::graph_info_save(&self.0, &p)?;
        Ok(())
    }

    /// Dump this `GraphInfo` as a YAML string.
    pub fn dump(&self) -> crate::Result<String> {
        let dumped = ffi::graphar::graph_info_dump(&self.0)?;
        let dumped = dumped.as_ref().expect("graph info dump should be valid");
        Ok(dumped.to_string())
    }
}

/// A builder for constructing a [`GraphInfo`].
///
/// Defaults:
/// - `vertex_infos = []`
/// - `edge_infos = []`
/// - `labels = []`
/// - `prefix = "./"` (matches upstream `graphar::GraphInfo` default and keeps the graph info validated; empty prefix is considered invalid upstream)
/// - `version = None`
pub struct GraphInfoBuilder {
    name: String,
    vertex_infos: Vec<VertexInfo>,
    edge_infos: Vec<EdgeInfo>,
    labels: Vec<String>,
    prefix: String,
    version: Option<InfoVersion>,
}

impl GraphInfoBuilder {
    /// Create a new builder with the required graph name.
    ///
    /// The default `prefix` is `"./"` to match upstream GraphAr C++
    /// `graphar::GraphInfo` behavior.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            vertex_infos: Vec::new(),
            edge_infos: Vec::new(),
            labels: Vec::new(),
            prefix: "./".to_string(),
            version: None,
        }
    }

    /// Push a single vertex info.
    pub fn push_vertex_info(mut self, vertex_info: VertexInfo) -> Self {
        self.vertex_infos.push(vertex_info);
        self
    }

    /// Replace vertex infos with the given vector.
    pub fn vertex_infos(mut self, vertex_infos: Vec<VertexInfo>) -> Self {
        self.vertex_infos = vertex_infos;
        self
    }

    /// Push a single edge info.
    pub fn push_edge_info(mut self, edge_info: EdgeInfo) -> Self {
        self.edge_infos.push(edge_info);
        self
    }

    /// Replace edge infos with the given vector.
    pub fn edge_infos(mut self, edge_infos: Vec<EdgeInfo>) -> Self {
        self.edge_infos = edge_infos;
        self
    }

    /// Set graph labels.
    pub fn labels(mut self, labels: Vec<String>) -> Self {
        self.labels = labels;
        self
    }

    /// Set graph labels from a string iterator.
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
    ///
    /// If unset, [`GraphInfoBuilder`] uses `"./"` by default.
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

    /// Build a [`GraphInfo`].
    ///
    /// Panics if GraphAr rejects the builder inputs. Prefer
    /// [`GraphInfoBuilder::try_build`] if you want to handle errors.
    pub fn build(self) -> GraphInfo {
        self.try_build().unwrap()
    }

    /// Try to build a [`GraphInfo`].
    pub fn try_build(self) -> crate::Result<GraphInfo> {
        let Self {
            name,
            vertex_infos,
            edge_infos,
            labels,
            prefix,
            version,
        } = self;
        GraphInfo::try_new(name, vertex_infos, edge_infos, labels, prefix, version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::info::{AdjListType, AdjacentList, AdjacentListVector};
    use crate::property::{PropertyBuilder, PropertyGroup, PropertyGroupVector, PropertyVec};
    use crate::types::{DataType, FileType};
    use tempfile::tempdir;

    fn make_vertex_info(r#type: &str) -> VertexInfo {
        let mut props = PropertyVec::new();
        props.emplace(PropertyBuilder::new("id", DataType::int64()).primary_key(true));
        let group = PropertyGroup::new(props, FileType::Parquet, format!("{}_props/", r#type));
        let mut groups = PropertyGroupVector::new();
        groups.push(group);

        VertexInfo::builder(r#type, 1024)
            .property_groups(groups)
            .labels_from_iter(["label"])
            .prefix(format!("{}/", r#type))
            .build()
    }

    fn make_edge_info(src_type: &str, edge_type: &str, dst_type: &str) -> EdgeInfo {
        let mut adjacent_lists = AdjacentListVector::new();
        adjacent_lists.push(AdjacentList::new(
            AdjListType::OrderedBySource,
            FileType::Parquet,
            Some("ordered_by_source/"),
        ));

        let mut props = PropertyVec::new();
        props.emplace(PropertyBuilder::new("weight", DataType::float64()));
        let mut groups = PropertyGroupVector::new();
        groups.push(PropertyGroup::new(props, FileType::Parquet, "weight/"));

        EdgeInfo::builder(src_type, edge_type, dst_type, 1024, 100, 100)
            .directed(true)
            .adjacent_lists(adjacent_lists)
            .property_groups(groups)
            .prefix(format!("{src_type}_{edge_type}_{dst_type}/"))
            .build()
    }

    #[test]
    fn test_graph_info_builder_roundtrip_getters_and_lookups() {
        let person = make_vertex_info("person");
        let software = make_vertex_info("software");
        let knows = make_edge_info("person", "knows", "person");

        let graph_info = GraphInfo::builder("my_graph")
            .push_vertex_info(person)
            .push_vertex_info(software)
            .push_edge_info(knows)
            .labels(vec!["l0".to_string()])
            .labels_from_iter(["l1", "l2"])
            .push_label("l3")
            .prefix("graph/")
            .version(InfoVersion::new(1).unwrap())
            .build();

        assert_eq!(graph_info.name(), "my_graph");
        assert_eq!(graph_info.prefix(), "graph/");
        assert!(graph_info.version().is_some());

        assert_eq!(
            graph_info.labels(),
            vec!["l1".to_string(), "l2".to_string(), "l3".to_string()]
        );

        assert_eq!(graph_info.vertex_info_num(), 2);
        assert_eq!(graph_info.edge_info_num(), 1);
        assert_eq!(graph_info.vertex_info_index("person"), Some(0));
        assert_eq!(graph_info.vertex_info_index("software"), Some(1));
        assert_eq!(graph_info.vertex_info_index("missing"), None);

        assert_eq!(
            graph_info.edge_info_index("person", "knows", "person"),
            Some(0)
        );
        assert_eq!(
            graph_info.edge_info_index("person", "unknown", "person"),
            None
        );

        assert!(graph_info.vertex_info("person").is_some());
        assert!(graph_info.vertex_info(String::from("software")).is_some());
        assert!(graph_info.vertex_info("missing").is_none());

        assert!(graph_info.edge_info("person", "knows", "person").is_some());
        assert!(
            graph_info
                .edge_info("person", "missing", "person")
                .is_none()
        );

        let vertex_infos = graph_info.vertex_infos();
        assert_eq!(vertex_infos.len(), 2);
        let vertex_infos_iter: Vec<_> = graph_info.vertex_infos_iter().collect();
        assert_eq!(vertex_infos_iter.len(), 2);

        let edge_infos = graph_info.edge_infos();
        assert_eq!(edge_infos.len(), 1);
        let edge_infos_iter: Vec<_> = graph_info.edge_infos_iter().collect();
        assert_eq!(edge_infos_iter.len(), 1);
    }

    #[test]
    fn test_graph_info_builder_replace_collections_and_version_opt() {
        let person = make_vertex_info("person");
        let software = make_vertex_info("software");
        let knows = make_edge_info("person", "knows", "person");

        let graph_info = GraphInfo::builder("my_graph")
            .push_vertex_info(make_vertex_info("tmp"))
            .vertex_infos(vec![person, software])
            .push_edge_info(make_edge_info("tmp", "tmp", "tmp"))
            .edge_infos(vec![knows])
            .version_opt(None)
            .build();

        assert_eq!(graph_info.vertex_info_num(), 2);
        assert_eq!(graph_info.edge_info_num(), 1);
        assert!(graph_info.version().is_none());
    }

    #[test]
    fn test_graph_info_builder_default_prefix() {
        let graph_info = GraphInfo::builder("my_graph")
            .push_vertex_info(make_vertex_info("person"))
            .push_edge_info(make_edge_info("person", "knows", "person"))
            .build();

        assert_eq!(graph_info.prefix(), "./");
    }

    #[test]
    fn test_graph_info_dump_save_and_load() {
        let graph_info = GraphInfo::builder("my_graph")
            .push_vertex_info(make_vertex_info("person"))
            .push_edge_info(make_edge_info("person", "knows", "person"))
            .prefix("graph/")
            .labels_from_iter(["person"])
            .build();

        let dumped = graph_info.dump().unwrap();
        assert!(!dumped.trim().is_empty(), "dumped={dumped:?}");
        assert!(dumped.contains("name: my_graph"), "dumped={dumped:?}");
        assert!(dumped.contains("prefix: graph/"), "dumped={dumped:?}");
        assert!(
            dumped.contains("person_knows_person.edge.yaml"),
            "dumped={dumped:?}"
        );
        assert!(dumped.contains("person.vertex.yaml"), "dumped={dumped:?}");

        let dir = tempdir().unwrap();
        let graph_path = dir.path().join("my_graph.graph.yaml");
        graph_info.save(&graph_path).unwrap();
        graph_info
            .vertex_info("person")
            .unwrap()
            .save(dir.path().join("person.vertex.yaml"))
            .unwrap();
        graph_info
            .edge_info("person", "knows", "person")
            .unwrap()
            .save(dir.path().join("person_knows_person.edge.yaml"))
            .unwrap();

        let loaded = GraphInfo::load(&graph_path).unwrap();
        assert_eq!(loaded.name(), "my_graph");
        assert_eq!(loaded.prefix(), "graph/");
        assert_eq!(loaded.vertex_info_num(), 1);
        assert_eq!(loaded.edge_info_num(), 1);
        assert!(loaded.vertex_info("person").is_some());
        assert!(loaded.edge_info("person", "knows", "person").is_some());
    }

    #[test]
    fn test_graph_info_try_new_error_paths() {
        let err = GraphInfo::try_new("", vec![], vec![], vec![], "graph/", None)
            .err()
            .unwrap();
        let msg = err.to_string();
        assert!(
            msg.contains("CreateGraphInfo") && msg.contains("name must not be empty"),
            "unexpected error message: {msg:?}"
        );
    }

    #[test]
    #[should_panic]
    fn test_graph_info_new_panics_on_invalid_args() {
        let _ = GraphInfo::new("", vec![], vec![], vec![], "graph/", None);
    }

    #[cfg(unix)]
    #[test]
    fn test_graph_info_save_and_load_non_utf8_path() {
        use std::os::unix::ffi::OsStringExt;

        let graph_info = GraphInfo::builder("my_graph")
            .push_vertex_info(make_vertex_info("person"))
            .push_edge_info(make_edge_info("person", "knows", "person"))
            .build();

        let dir = tempdir().unwrap();
        let mut path = dir.path().to_path_buf();
        path.push(std::ffi::OsString::from_vec(
            b"graph_info_\xFF_non_utf8.yaml".to_vec(),
        ));

        let err = graph_info.save(&path).err().unwrap();
        assert!(
            matches!(err, crate::Error::NonUtf8Path(_)),
            "unexpected error: {err:?}"
        );

        let err = GraphInfo::load(&path).err().unwrap();
        assert!(
            matches!(err, crate::Error::NonUtf8Path(_)),
            "unexpected error: {err:?}"
        );
    }
}
