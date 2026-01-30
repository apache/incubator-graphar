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

//! GraphAr adjacent list metadata.

use std::ops::Deref;

use cxx::{CxxVector, SharedPtr, UniquePtr, let_cxx_string};

use crate::ffi;
use crate::info::AdjListType;
use crate::types::FileType;

/// An adjacency list definition in GraphAr metadata.
pub type AdjacentList = ffi::SharedAdjacentList;

impl AdjacentList {
    /// Construct a `AdjacentList` from a raw C++ shared pointer.
    pub(crate) fn from_inner(inner: SharedPtr<ffi::graphar::AdjacentList>) -> Self {
        Self(inner)
    }

    /// Create a new adjacency list definition.
    ///
    /// If `path_prefix` is `None`, GraphAr will use a default prefix derived
    /// from `ty` (e.g. `ordered_by_source/`).
    pub fn new<P: AsRef<str>>(
        ty: AdjListType,
        file_type: FileType,
        path_prefix: Option<P>,
    ) -> Self {
        let prefix = path_prefix.as_ref().map(|p| p.as_ref()).unwrap_or("");
        let_cxx_string!(prefix = prefix);
        let inner = ffi::graphar::CreateAdjacentList(ty, file_type, &prefix);
        Self(inner)
    }

    /// Returns the adjacency list type.
    pub fn ty(&self) -> AdjListType {
        self.0.GetType()
    }

    /// Returns the chunk file type for this adjacency list.
    pub fn file_type(&self) -> FileType {
        self.0.GetFileType()
    }

    /// Returns the logical prefix for this adjacency list.
    pub fn prefix(&self) -> String {
        self.0.GetPrefix().to_string()
    }
}

/// A vector of adjacency lists.
///
/// This is a wrapper around a C++ `graphar::AdjacentListVector`.
pub struct AdjacentListVector(UniquePtr<CxxVector<AdjacentList>>);

impl Default for AdjacentListVector {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for AdjacentListVector {
    type Target = CxxVector<AdjacentList>;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl AdjacentListVector {
    /// Create an empty adjacency list vector.
    pub fn new() -> Self {
        Self(ffi::graphar::new_adjacent_list_vec())
    }

    /// Push an adjacency list into the vector.
    pub fn push(&mut self, adjacent_list: AdjacentList) {
        ffi::graphar::push_adjacent_list(self.0.pin_mut(), adjacent_list.0);
    }

    /// Borrow the underlying C++ adjacency list vector.
    pub(crate) fn as_ref(&self) -> &CxxVector<AdjacentList> {
        self.0
            .as_ref()
            .expect("adjacent list vector should be valid")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::FileType;

    #[test]
    fn test_adjacent_list_roundtrip_getters() {
        let list = AdjacentList::new(AdjListType::OrderedBySource, FileType::Csv, Some("adj/"));
        assert_eq!(list.ty(), AdjListType::OrderedBySource);
        assert_eq!(list.file_type(), FileType::Csv);
        assert_eq!(list.prefix(), "adj/");
    }

    #[test]
    fn test_adjacent_list_default_prefix() {
        let list = AdjacentList::new(
            AdjListType::OrderedBySource,
            FileType::Parquet,
            None::<&str>,
        );
        assert_eq!(list.prefix(), "ordered_by_source/");
    }

    #[test]
    fn test_adjacent_list_vector_push_and_borrow() {
        let mut v = AdjacentListVector::new();
        let _ = v.as_ref();
        assert!(v.is_empty());

        v.push(AdjacentList::new(
            AdjListType::UnorderedBySource,
            FileType::Parquet,
            Some("u/"),
        ));
        let _ = v.as_ref();
        assert_eq!(v.len(), 1);
    }

    #[test]
    fn test_adjacent_list_vector_default() {
        let mut v = AdjacentListVector::default();
        assert_eq!(v.len(), 0);
        v.push(AdjacentList::new(
            AdjListType::OrderedByDest,
            FileType::Parquet,
            None::<&str>,
        ));
        assert_eq!(v.len(), 1);
        let _ = v.as_ref();
    }
}
