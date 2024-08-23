use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use serde::{Deserialize, Serialize};

use crate::{error::MaelstromError, node::Node};

#[derive(Serialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RequestPayload {
    Read { key: String },
    Write { key: String, value: String },
    CompareAndSwap { key: String, from: String, to: String, create_if_not_exists: bool },
}

#[allow(clippy::enum_variant_names)]
#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponsePayload {
    ReadOk { value: String },
    WriteOk,
    CompareAndSwapOk,
}

// A client needs a new node templated on the message protocol
// for the Maelstrom seq-kv service
#[derive(Clone)]
pub struct SeqKvClient {
    node: Node,
}

impl SeqKvClient {
    pub fn new(node: Node) -> Self {
        Self {
            node: Node {
                id: node.id,
                network_ids: node.network_ids,
                next_msg_id: node.next_msg_id,
                cancellation_token: node.cancellation_token,
                response_map: Arc::new(Mutex::new(BTreeMap::new())),
            },
        }
    }

    pub async fn read(&self, key: String) -> Result<String, MaelstromError> {
        // Issue a read reqeust to seq-kv service and return the response
        let response = self
            .node
            .send_generic_dest("seq-kv".to_owned(), RequestPayload::Read { key }, None)
            .await?;
        match response.body.payload {
            ResponsePayload::ReadOk { value } => Ok(value),
            _ => Err(MaelstromError::not_supported("Invalid response type")),
        }
    }

    pub async fn read_int(&self, key: String) -> Result<i64, MaelstromError> {
        let response = self
            .node
            .send_generic_dest("seq-kv".to_owned(), RequestPayload::Read { key }, None)
            .await?;
        match response.body.payload {
            ResponsePayload::ReadOk { value } => Ok(value.parse().unwrap()),
            _ => Err(MaelstromError::not_supported("Invalid response type")),
        }
    }

    pub async fn write(&self, key: String, value: String) -> Result<(), MaelstromError> {
        let response = self
            .node
            .send_generic_dest("seq-kv".to_owned(), RequestPayload::Write { key, value }, None)
            .await?;
        match response.body.payload {
            ResponsePayload::WriteOk => Ok(()),
            _ => Err(MaelstromError::not_supported("Invalid response type")),
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
            .send_generic_dest(
                "seq-kv".to_owned(),
                RequestPayload::CompareAndSwap { key, from, to, create_if_not_exists },
                None,
            )
            .await?;
        match response.body.payload {
            ResponsePayload::CompareAndSwapOk => Ok(()),
            _ => Err(MaelstromError::not_supported("Invalid response type")),
        }
    }
}
