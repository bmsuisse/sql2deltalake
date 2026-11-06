//! The load logic of sql2deltalake, free of Python: SQL generation for the source,
//! Arrow normalization, primary-key snapshot diffs and the load state machine.

pub mod error;
pub mod host;
pub mod keys;
pub mod load;
pub mod normalize;
pub mod scalar;
pub mod sqlgen;
pub mod table;

pub use error::{Error, Result};

/// Version of the core crate, exposed to Python as `odbc2deltalake._core.__version__`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
