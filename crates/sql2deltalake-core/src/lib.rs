//! The load logic of sql2deltalake, free of Python: SQL generation for the source,
//! Arrow normalization, primary-key snapshot diffs and the load state machine.

/// Version of the core crate, exposed to Python as `odbc2deltalake._core.__version__`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
