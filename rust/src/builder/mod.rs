//! High-level builders for writing GraphAr data.

/// Vertex builders for writing vertex chunks.
pub mod vertex;

mod property_value;

/// A value that can be written into a GraphAr property.
pub use property_value::PropertyValue;

pub use vertex::{Vertex, VerticesBuilder};
