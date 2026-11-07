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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    pub fn msg(m: impl Into<String>) -> Error {
        Error::Msg(m.into())
    }

    /// Whether a fallback may handle the error (warn and go on); false for interrupts.
    pub fn is_recoverable(&self) -> bool {
        !matches!(self, Error::Interrupted(_))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Msg(m) => f.write_str(m),
            Error::Arrow(e) => write!(f, "{e}"),
            Error::External(e) | Error::Interrupted(e) => write!(f, "{e}"),
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
