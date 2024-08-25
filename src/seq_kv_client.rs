use serde::{Deserialize, Serialize};

use crate::{error::MaelstromError, node::Node};

#[derive(Serialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RequestPayload<'a> {
    Read {
        key: &'a str,
    },
    Write {
        key: &'a str,
        value: &'a str,
    },
    #[serde(rename = "cas")]
    CompareAndSwap {
        key: &'a str,
        from: &'a str,
        to: &'a str,
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

    pub async fn read(&self, key: &str) -> Result<String, MaelstromError> {
        // Issue a read reqeust to seq-kv service and return the response
        let response = self.node.send("seq-kv", RequestPayload::Read { key }, None).await;
        match response {
            Ok(ResponsePayload::ReadOk { value }) => Ok(value),
            Err(err) => Err(err),
            _ => Err(MaelstromError::not_supported("Invalid response to read request")),
        }
    }

    pub async fn read_int(&self, key: &str) -> Result<i64, MaelstromError> {
        let response = self.node.send("seq-kv", RequestPayload::Read { key }, None).await;
        match response {
            Ok(ResponsePayload::ReadOk { value }) => Ok(value.parse().map_err(|_| {
                MaelstromError::malformed_request("Invalid response to read request")
            })?),
            Err(err) => Err(err),
            _ => Err(MaelstromError::not_supported("Invalid response to read request")),
        }
    }

    pub async fn write(&self, key: &str, value: &str) -> Result<(), MaelstromError> {
        let response = self.node.send("seq-kv", RequestPayload::Write { key, value }, None).await;
        match response {
            Ok(ResponsePayload::WriteOk) => Ok(()),
            Err(err) => Err(err),
            _ => Err(MaelstromError::not_supported("Invalid response to write request")),
        }
    }

    pub async fn compare_and_swap(
        &self,
        key: &str,
        from: &str,
        to: &str,
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
