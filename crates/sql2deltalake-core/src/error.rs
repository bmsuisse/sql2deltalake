use std::fmt;

/// Errors of the core. `External` carries an error raised by a host callback (a Python
/// exception), so the host can re-raise the original.
#[derive(Debug)]
pub enum Error {
    Msg(String),
    Arrow(arrow_schema::ArrowError),
    External(Box<dyn std::error::Error + Send + Sync>),
    /// An error raised by a host callback that the load must not handle (a Python
    /// `KeyboardInterrupt` or `SystemExit`): no fallback, no retry, straight to the caller.
    Interrupted(Box<dyn std::error::Error + Send + Sync>),
    /// A failed load whose state (`latest_pk_version`, `primary_keys_ts`) could not be put back
    /// to the last successful load: the load's error and what the restore failed with.
    NotRestored {
        load: Box<Error>,
        restore: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    pub fn msg(m: impl Into<String>) -> Error {
        Error::Msg(m.into())
    }

    /// Whether a fallback may handle the error (warn and go on); false for interrupts.
    pub fn is_recoverable(&self) -> bool {
        match self {
            Error::Interrupted(_) => false,
            Error::NotRestored { load, .. } => load.is_recoverable(),
            _ => true,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Msg(m) => f.write_str(m),
            Error::Arrow(e) => write!(f, "{e}"),
            Error::External(e) | Error::Interrupted(e) => write!(f, "{e}"),
            Error::NotRestored { load, restore } => {
                write!(f, "{load} (and the load state could not be restored: {restore})")
            }
        }
    }
}

impl std::error::Error for Error {}

impl From<arrow_schema::ArrowError> for Error {
    fn from(e: arrow_schema::ArrowError) -> Self {
        // an error raised by a callback can come back through an Arrow stream
        Error::Arrow(e)
    }
}
