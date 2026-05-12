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

//! Error types for this crate.

use std::fmt;
use std::path::PathBuf;

/// A result type for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// An error returned by this crate.
#[derive(Debug)]
pub enum Error {
    /// An exception raised from the C++ GraphAr implementation and propagated via `cxx`.
    Cxx(cxx::Exception),
    /// A filesystem path is not valid UTF-8.
    NonUtf8Path(PathBuf),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cxx(e) => write!(f, "C++ exception: {e}"),
            Self::NonUtf8Path(path) => write!(f, "path is not valid UTF-8: {}", path.display()),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Cxx(e) => Some(e),
            Self::NonUtf8Path(_) => None,
        }
    }
}

impl From<cxx::Exception> for Error {
    fn from(value: cxx::Exception) -> Self {
        Self::Cxx(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::info::VertexInfo;
    use crate::property::PropertyGroupVector;
    use std::error::Error as StdError;

    #[test]
    fn test_non_utf8_path_display_and_source() {
        let err = Error::NonUtf8Path(std::path::PathBuf::from("not_utf8_checked_here"));
        let msg = err.to_string();
        assert!(msg.contains("path is not valid UTF-8"), "msg={msg:?}");
        assert!(StdError::source(&err).is_none());
    }

    #[test]
    fn test_cxx_error_display_source_and_from() {
        let groups = PropertyGroupVector::new();
        let err = match VertexInfo::try_new("", 1, groups, vec![], "", None) {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };

        let cxx_exc = match err {
            Error::Cxx(e) => e,
            other => panic!("expected Error::Cxx, got {other:?}"),
        };

        let err = Error::from(cxx_exc);
        let msg = err.to_string();
        assert!(msg.contains("C++ exception:"), "msg={msg:?}");
        assert!(StdError::source(&err).is_some());
    }
}
