use std::io;

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum GlomerError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error("{0}")]
    Abort(String),
}

#[derive(Error, Serialize, Deserialize, Copy, Clone, Debug)]
#[serde(tag = "code")]
pub enum MaelstromErrorType {
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
pub struct MaelstromError {
    pub text: String,
    #[serde(flatten)]
    #[source]
    pub error_type: MaelstromErrorType,
}

impl From<GlomerError> for MaelstromError {
    fn from(error: GlomerError) -> Self {
        match error {
            GlomerError::Io(error) => match error.kind() {
                io::ErrorKind::TimedOut => Self::timeout(error.to_string()),
                _ => Self::crash(error.to_string()),
            },
            _ => Self::crash(error.to_string()),
        }
    }
}

#[allow(dead_code)]
impl MaelstromError {
    pub fn timeout(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            error_type: MaelstromErrorType::Timeout,
        }
    }

    pub fn node_not_found(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            error_type: MaelstromErrorType::NodeNotFound,
        }
    }

    pub fn not_supported(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            error_type: MaelstromErrorType::NotSupported,
        }
    }

    pub fn temporarily_unavailable(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            error_type: MaelstromErrorType::TemporarilyUnavailable,
        }
    }

    pub fn malformed_request(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            error_type: MaelstromErrorType::MalformedRequest,
        }
    }

    pub fn crash(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            error_type: MaelstromErrorType::Crash,
        }
    }

    pub fn abort(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            error_type: MaelstromErrorType::Abort,
        }
    }

    pub fn key_does_not_exist(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            error_type: MaelstromErrorType::KeyDoesNotExist,
        }
    }

    pub fn key_already_exists(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            error_type: MaelstromErrorType::KeyAlreadyExists,
        }
    }

    pub fn precondition_failed(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            error_type: MaelstromErrorType::PreconditionFailed,
        }
    }

    pub fn txn_conflict(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            error_type: MaelstromErrorType::TxnConflict,
        }
    }
}
