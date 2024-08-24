use serde::{Deserialize, Serialize};

use crate::error::MaelstromError;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MaelstromMessage<P> {
    pub src: String,
    pub dest: String,
    pub body: Body<P>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Body<P> {
    pub msg_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<u64>,
    #[serde(flatten)]
    pub payload: P,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(untagged)]
pub enum Fallible<P> {
    Ok(P),
    Err(MaelstromError),
}

impl<P> From<Fallible<P>> for Result<P, MaelstromError> {
    fn from(fallible: Fallible<P>) -> Self {
        match fallible {
            Fallible::Ok(ok) => Ok(ok),
            Fallible::Err(err) => Err(err),
        }
    }
}
