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

//! GraphAr metadata information types.

mod adjacent_list;
mod edge_info;
mod version;
mod vertex_info;

/// Re-export of the C++ `graphar::AdjListType`.
pub use crate::ffi::graphar::AdjListType;

pub use adjacent_list::{AdjacentList, AdjacentListVector};
pub use edge_info::{EdgeInfo, EdgeInfoBuilder};
pub use version::InfoVersion;
pub use vertex_info::{VertexInfo, VertexInfoBuilder};
