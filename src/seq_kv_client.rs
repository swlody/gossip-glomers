use serde::{Deserialize, Serialize};

use crate::{error::MaelstromError, node::Node};

#[derive(Serialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RequestPayload {
    Read {
        key: String,
    },
    Write {
        key: String,
        value: String,
    },
    #[serde(rename = "cas")]
    CompareAndSwap {
        key: String,
        from: String,
        to: String,
        create_if_not_exists: bool,
    },
}

#[allow(clippy::enum_variant_names)]
#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponsePayload {
    ReadOk {
        value: String,
    },
    WriteOk,
    #[serde(rename = "cas_ok")]
    CompareAndSwapOk,
}

// A client needs a new node templated on the message protocol
// for the Maelstrom seq-kv service
#[derive(Clone)]
pub struct SeqKvClient {
    node: Node,
}

// TODO support other maelstrom services
impl SeqKvClient {
    pub fn new(node: Node) -> Self {
        Self { node }
    }

    pub async fn read(&self, key: String) -> Result<String, MaelstromError> {
        // Issue a read reqeust to seq-kv service and return the response
        let response = self.node.send("seq-kv", RequestPayload::Read { key }, None).await;
        match response {
            Ok(ResponsePayload::ReadOk { value }) => Ok(value),
            Err(err) => Err(err),
            _ => Err(MaelstromError::not_supported("Invalid response to read request")),
        }
    }

    pub async fn read_int(&self, key: String) -> Result<i64, MaelstromError> {
        let response = self.node.send("seq-kv", RequestPayload::Read { key }, None).await;
        match response {
            Ok(ResponsePayload::ReadOk { value }) => Ok(value.parse().map_err(|_| {
                MaelstromError::malformed_request("Invalid response to read request")
            })?),
            Err(err) => Err(err),
            _ => Err(MaelstromError::not_supported("Invalid response to read request")),
        }
    }

    pub async fn write(&self, key: String, value: String) -> Result<(), MaelstromError> {
        let response = self.node.send("seq-kv", RequestPayload::Write { key, value }, None).await;
        match response {
            Ok(ResponsePayload::WriteOk) => Ok(()),
            Err(err) => Err(err),
            _ => Err(MaelstromError::not_supported("Invalid response to write request")),
        }
    }

    pub async fn compare_and_swap(
        &self,
        key: String,
        from: String,
        to: String,
        create_if_not_exists: bool,
    ) -> Result<(), MaelstromError> {
        let response = self
            .node
            .send(
                "seq-kv",
                RequestPayload::CompareAndSwap { key, from, to, create_if_not_exists },
                None,
            )
            .await;
        match response {
            Ok(ResponsePayload::CompareAndSwapOk) => Ok(()),
            Err(err) => Err(err),
            _ => Err(MaelstromError::not_supported("Invalid response to CAS request")),
        }
    }
}
