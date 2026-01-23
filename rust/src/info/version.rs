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

//! GraphAr info version bindings.

use crate::ffi;
use cxx::SharedPtr;

/// A GraphAr `InfoVersion` value.
///
/// This is a thin wrapper around `std::shared_ptr<const graphar::InfoVersion>`.
#[derive(Clone)]
pub struct InfoVersion(pub(crate) SharedPtr<ffi::graphar::ConstInfoVersion>);

impl InfoVersion {
    /// Create a new `InfoVersion` by version number.
    ///
    /// TODO: upstream C++ constructor takes `int`; prefer fixed-width integer types.
    pub fn new(version: i32) -> Result<Self, cxx::Exception> {
        Ok(Self(ffi::graphar::new_const_info_version(version)?))
    }
}
