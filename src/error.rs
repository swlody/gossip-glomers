use serde::{Deserialize, Serialize};
use thiserror::Error;

#[allow(clippy::module_name_repetitions)]
#[derive(Error, Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename = "error")]
#[error("Error: {code}: {text}")]
pub struct MaelstromError {
    pub text: String,
    pub code: u32,
}

#[allow(dead_code)]
impl MaelstromError {
    pub fn timeout(error_text: impl Into<String>) -> Self {
        Self { text: error_text.into(), code: 0 }
    }

    pub fn node_not_found(error_text: impl Into<String>) -> Self {
        Self { text: error_text.into(), code: 1 }
    }

    pub fn not_supported(error_text: impl Into<String>) -> Self {
        Self { text: error_text.into(), code: 10 }
    }

    pub fn temporarily_unavailable(error_text: impl Into<String>) -> Self {
        Self { text: error_text.into(), code: 1 }
    }

    pub fn malformed_request(error_text: impl Into<String>) -> Self {
        Self { text: error_text.into(), code: 12 }
    }

    pub fn crash(error_text: impl Into<String>) -> Self {
        Self { text: error_text.into(), code: 13 }
    }

    pub fn abort(error_text: impl Into<String>) -> Self {
        Self { text: error_text.into(), code: 14 }
    }

    pub fn key_does_not_exist(error_text: impl Into<String>) -> Self {
        Self { text: error_text.into(), code: 20 }
    }

    pub fn key_already_exists(error_text: impl Into<String>) -> Self {
        Self { text: error_text.into(), code: 21 }
    }

    pub fn precondition_failed(error_text: impl Into<String>) -> Self {
        Self { text: error_text.into(), code: 22 }
    }

    pub fn txn_conflict(error_text: impl Into<String>) -> Self {
        Self { text: error_text.into(), code: 23 }
    }
}
