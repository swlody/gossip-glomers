use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

// TODO generally be more deliberate about not leaking internal errors
// errors are hard!
#[allow(clippy::module_name_repetitions)]
#[derive(Error, Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename = "error")]
pub struct MaelstromError {
    pub text: String,
    pub code: u32,
}

impl From<serde_json::Error> for MaelstromError {
    fn from(err: serde_json::Error) -> Self {
        Self {
            text: err.to_string(),
            code: error_type::MALFORMED_REQUEST,
        }
    }
}

impl fmt::Display for MaelstromError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let code_name = match self.code {
            error_type::TIMEOUT => "Timeout",
            error_type::NODE_NOT_FOUND => "NodeNotFound",
            error_type::NOT_SUPPORTED => "NotSupported",
            error_type::TEMPORARILY_UNAVAILABLE => "TemporarilyUnavailable",
            error_type::MALFORMED_REQUEST => "MalformedRequest",
            error_type::CRASH => "Crash",
            error_type::ABORT => "Abort",
            error_type::KEY_DOES_NOT_EXIST => "KeyDoesNotExist",
            error_type::KEY_ALREADY_EXISTS => "KeyAlreadyExists",
            error_type::PRECONDITION_FAILED => "PreconditionFailed",
            error_type::TXN_CONFLICT => "TxnConflict",
            _ => "Unknown",
        };
        write!(f, "Error: {}: '{}'", code_name, self.text)
    }
}

pub mod error_type {
    pub const TIMEOUT: u32 = 0;
    pub const NODE_NOT_FOUND: u32 = 1;
    pub const NOT_SUPPORTED: u32 = 10;
    pub const TEMPORARILY_UNAVAILABLE: u32 = 11;
    pub const MALFORMED_REQUEST: u32 = 12;
    pub const CRASH: u32 = 13;
    pub const ABORT: u32 = 14;
    pub const KEY_DOES_NOT_EXIST: u32 = 20;
    pub const KEY_ALREADY_EXISTS: u32 = 21;
    pub const PRECONDITION_FAILED: u32 = 22;
    pub const TXN_CONFLICT: u32 = 23;
}

#[allow(dead_code)]
impl MaelstromError {
    pub fn timeout(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            code: error_type::TIMEOUT,
        }
    }

    pub fn node_not_found(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            code: error_type::NODE_NOT_FOUND,
        }
    }

    pub fn not_supported(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            code: error_type::NOT_SUPPORTED,
        }
    }

    pub fn temporarily_unavailable(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            code: error_type::TEMPORARILY_UNAVAILABLE,
        }
    }

    pub fn malformed_request(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            code: error_type::MALFORMED_REQUEST,
        }
    }

    pub fn crash(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            code: error_type::CRASH,
        }
    }

    pub fn abort(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            code: error_type::ABORT,
        }
    }

    pub fn key_does_not_exist(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            code: error_type::KEY_DOES_NOT_EXIST,
        }
    }

    pub fn key_already_exists(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            code: error_type::KEY_ALREADY_EXISTS,
        }
    }

    pub fn precondition_failed(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            code: error_type::PRECONDITION_FAILED,
        }
    }

    pub fn txn_conflict(error_text: impl Into<String>) -> Self {
        Self {
            text: error_text.into(),
            code: error_type::TXN_CONFLICT,
        }
    }
}
