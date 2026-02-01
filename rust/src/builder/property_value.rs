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

//! Shared property value abstractions for builders.

#[doc(hidden)]
pub(crate) mod sealed {
    pub trait Sealed<Target> {}
}

/// A value that can be written into a GraphAr property.
///
/// This trait is sealed and cannot be implemented outside this crate.
pub trait PropertyValue<Target>: sealed::Sealed<Target> {
    /// Add this value as a property on the given target.
    fn add_to(self, target: &mut Target, name: &str);
}
