use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MaelstromMessage<P> {
    pub src: String,
    pub dest: String,
    pub body: Body<P>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Body<P> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<u64>,
    #[serde(flatten)]
    pub payload: P,
}
