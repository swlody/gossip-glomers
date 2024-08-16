use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "code")]
enum ErrorType {
    #[serde(rename = "0")]
    #[error("Timeout")]
    Timeout,

    #[serde(rename = "1")]
    #[error("Node not found")]
    NodeNotFound,

    #[serde(rename = "10")]
    #[error("Not supported")]
    NotSupported,

    #[serde(rename = "11")]
    #[error("Temporarily unavailable")]
    TemporarilyUnavailable,

    #[serde(rename = "12")]
    #[error("Malformed request")]
    MalformedRequest,

    #[serde(rename = "13")]
    #[error("Crash")]
    Crash,

    #[serde(rename = "14")]
    #[error("Abort")]
    Abort,

    #[serde(rename = "20")]
    #[error("Key does not exist")]
    KeyDoesNotExist,

    #[serde(rename = "21")]
    #[error("Key already exists")]
    KeyAlreadyExists,

    #[serde(rename = "22")]
    #[error("Precondition failed")]
    PreconditionFailed,

    #[serde(rename = "23")]
    #[error("Transaction  conflict")]
    TxnConflict,
}

#[derive(Error, Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename = "error")]
#[error("Error: {error_type}: {text}")]
pub struct Error {
    text: String,
    #[serde(flatten)]
    #[source]
    error_type: ErrorType,
}

#[allow(dead_code)]
impl Error {
    fn timeout(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            error_type: ErrorType::Timeout,
        }
    }

    fn node_not_found(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            error_type: ErrorType::NodeNotFound,
        }
    }

    fn not_supported(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            error_type: ErrorType::NotSupported,
        }
    }

    fn temporarily_unavailable(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            error_type: ErrorType::TemporarilyUnavailable,
        }
    }

    fn malformed_request(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            error_type: ErrorType::MalformedRequest,
        }
    }

    fn crash(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            error_type: ErrorType::Crash,
        }
    }

    fn abort(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            error_type: ErrorType::Abort,
        }
    }

    fn key_does_not_exist(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            error_type: ErrorType::KeyDoesNotExist,
        }
    }

    fn key_already_exists(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            error_type: ErrorType::KeyAlreadyExists,
        }
    }

    fn precondition_failed(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            error_type: ErrorType::PreconditionFailed,
        }
    }

    fn txn_conflict(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            error_type: ErrorType::TxnConflict,
        }
    }
}
