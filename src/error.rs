use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "code")]
enum ErrorType {
    #[serde(rename = "12")]
    #[error("Malformed request")]
    MalformedRequest,
    #[serde(rename = "14")]
    #[error("Abort")]
    Abort,
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

impl Error {
    pub fn malformed_request(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            error_type: ErrorType::MalformedRequest,
        }
    }

    pub fn abort(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            error_type: ErrorType::Abort,
        }
    }
}
