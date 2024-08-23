use std::{collections::BTreeMap, sync::Mutex};

use serde::{Deserialize, Serialize};
use tokio::time::Duration;

use crate::{error::MaelstromError, node::Node};

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum SeqKvPayload {
    Read { key: String },
    Write { key: String, value: String },
    CompareAndSwap { key: String, from: String, to: String, create_if_not_exists: bool },

    ReadOk { value: String },
    WriteOk,
    CompareAndSwapOk,
}

// A client needs a new node templated on the message protocol
// for the Maelstrom seq-kv service
pub struct SeqKvClient {
    node: Node,
}

impl SeqKvClient {
    pub fn new(node: &Node) -> Self {
        Self {
            node: Node {
                id: node.id,
                network_ids: node.network_ids.clone(),
                next_msg_id: node.next_msg_id.clone(),
                cancellation_token: node.cancellation_token.clone(),
                response_map: Mutex::new(BTreeMap::new()),
            },
        }
    }

    pub async fn read(&self, key: String) -> Result<String, MaelstromError> {
        // Issue a read reqeust to seq-kv service and return the response
        let response = self
            .node
            .send_generic_dest(
                "seq-kv".to_owned(),
                SeqKvPayload::Read { key },
                Some(Duration::from_millis(500)),
            )
            .await?;
        match response.body.payload {
            SeqKvPayload::ReadOk { value } => Ok(value),
            _ => Err(MaelstromError::not_supported("Invalid response type")),
        }
    }

    pub async fn read_int(&self, key: String) -> Result<i64, MaelstromError> {
        let response = self
            .node
            .send_generic_dest(
                "seq-kv".to_owned(),
                SeqKvPayload::Read { key },
                Some(Duration::from_millis(500)),
            )
            .await?;
        match response.body.payload {
            SeqKvPayload::ReadOk { value } => Ok(value.parse().unwrap()),
            _ => Err(MaelstromError::not_supported("Invalid response type")),
        }
    }

    pub async fn write(&self, key: String, value: String) -> Result<(), MaelstromError> {
        self.node
            .send_generic_dest(
                "seq-kv".to_owned(),
                SeqKvPayload::Write { key, value },
                Some(Duration::from_millis(500)),
            )
            .await?;
        Ok(())
    }

    pub async fn compare_and_swap(
        &self,
        key: String,
        from: String,
        to: String,
        create_if_not_exists: bool,
    ) -> Result<(), MaelstromError> {
        self.node
            .send_generic_dest(
                "seq-kv".to_owned(),
                SeqKvPayload::CompareAndSwap { key, from, to, create_if_not_exists },
                Some(Duration::from_millis(500)),
            )
            .await?;
        Ok(())
    }
}
