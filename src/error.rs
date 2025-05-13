use crate::protocol::{ErrorMessageBody, MessageBody};
use std::io;
use thiserror::Error as ThisError;

/// [source](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#errors).
#[derive(Debug, ThisError)]
pub enum Error {
    #[error("timeout")]
    Timeout,

    #[error("not supported: {0}")]
    NotSupported(String),

    #[error("temporarily unavailable")]
    TemporarilyUnavailable,

    #[error("malformed request")]
    MalformedRequest,

    #[error("crash")]
    Crash,

    #[error("abort")]
    Abort,

    #[error("key does not exist")]
    KeyDoesNotExist,

    #[error("key already exists")]
    KeyAlreadyExists,

    #[error("precondition failed")]
    PreconditionFailed,

    #[error("txn conflict")]
    TxnConflict,

    #[error("{1}")]
    Custom(i32, String),

    #[error("io error")]
    Io(#[from] io::Error),

    #[error("json error")]
    Json(#[from] serde_json::Error),

    #[error("invalid message body")]
    InvalidMessageBody,

    #[error("cluster already initialized")]
    ClusterAlreadyInitialized,
}

impl Error {
    #[must_use] pub fn code(&self) -> i32 {
        match self {
            Error::Timeout => 0,
            Error::NotSupported(_) => 10,
            Error::TemporarilyUnavailable => 11,
            Error::MalformedRequest => 12,
            Error::Crash => 13,
            Error::Abort => 14,
            Error::KeyDoesNotExist => 20,
            Error::KeyAlreadyExists => 21,
            Error::PreconditionFailed => 22,
            Error::TxnConflict => 30,
            Error::Io(_) => -1,
            Error::Json(_) => -1,
            Error::Custom(code, _) => *code,
            Error::InvalidMessageBody => -32,
            Error::ClusterAlreadyInitialized => -33,
        }
    }

    #[must_use] pub fn description(&self) -> &str {
        match self {
            Error::Timeout => "timeout",
            Error::NotSupported(t) => t.as_str(),
            Error::TemporarilyUnavailable => "temporarily unavailable",
            Error::MalformedRequest => "malformed request",
            Error::Crash => "crash",
            Error::Abort => "abort",
            Error::KeyDoesNotExist => "key does not exist",
            Error::KeyAlreadyExists => "key already exists",
            Error::PreconditionFailed => "precondition failed",
            Error::TxnConflict => "txn conflict",
            Error::Custom(_, text) => text.as_str(),
            Error::Io(_) => "io error",
            Error::Json(_) => "json error",
            Error::InvalidMessageBody => "invalid message body",
            Error::ClusterAlreadyInitialized => "cluster already initialized",
        }
    }
}

impl From<&MessageBody> for Error {
    fn from(value: &MessageBody) -> Self {
        if !value.is_error() {
            return Error::Custom(-1, "serialized response that was not an error".to_string());
        }

        match value.as_obj::<ErrorMessageBody>() {
            Err(t) => Error::Custom(Error::Crash.code(), t.to_string()),
            Ok(obj) => Error::from(obj),
        }
    }
}

impl From<ErrorMessageBody> for Error {
    fn from(value: ErrorMessageBody) -> Self {
        match value.code {
            0 => Error::Timeout,
            10 => Error::NotSupported(value.text),
            11 => Error::TemporarilyUnavailable,
            12 => Error::MalformedRequest,
            13 => Error::Crash,
            14 => Error::Abort,
            20 => Error::KeyDoesNotExist,
            21 => Error::KeyAlreadyExists,
            22 => Error::PreconditionFailed,
            30 => Error::TxnConflict,
            code => Error::Custom(code, value.text),
        }
    }
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        self.code() == other.code() && self.description() == other.description()
    }
}

impl Eq for Error {}

#[cfg(test)]
mod test {
    use crate::protocol::MessageBody;
    use crate::runtime::Result;
    use crate::Error;

    #[test]
    fn parse_non_error() -> Result<()> {
        let raw = r#"{"type":"none","msg_id":1}"#;
        let msg: MessageBody = serde_json::from_str(raw)?;
        assert!(!msg.is_error());

        let err = Error::from(&msg);
        assert_eq!(err.code(), -1);

        Ok(())
    }

    #[test]
    fn parse_not_supported_error() -> Result<()> {
        let raw = r#"{"type":"error","msg_id":1, "code": 10}"#;
        let msg: MessageBody = serde_json::from_str(raw)?;
        assert!(msg.is_error());

        let err = Error::from(&msg);
        let expected = Error::NotSupported(String::new());
        assert_eq!(err, expected);

        Ok(())
    }
}
