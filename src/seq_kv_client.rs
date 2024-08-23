use std::{collections::BTreeMap, sync::Mutex};

use serde::{Deserialize, Serialize};

use crate::{error::MaelstromError, node::Node};

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum SeqKvPayload {
    Read { key: String },
    ReadOk { value: String },
    Write { key: String, value: String },
    WriteOk,
    CompareAndSwap { key: String, from: String, to: String, create_if_not_exists: bool },
    CompareAndSwapOk,
}

pub struct SeqKvClient {
    node: Node<SeqKvPayload>,
}

impl SeqKvClient {
    pub fn new<P>(node: &Node<P>) -> Self {
        Self {
            node: Node {
                id: node.id.clone(),
                network_ids: node.network_ids.clone(),
                next_msg_id: node.next_msg_id.clone(),
                response_map: Mutex::new(BTreeMap::new()),
            },
        }
    }

    pub async fn read(&self, key: String) -> Result<String, MaelstromError> {
        let response = self
            .node
            .send_generic_dest("seq-kv".to_owned(), SeqKvPayload::Read { key }, None)
            .await?;
        match response.body.payload {
            SeqKvPayload::ReadOk { value } => Ok(value),
            _ => Err(MaelstromError::not_supported("Invalid response type")),
        }
    }

    pub async fn read_int(&self, key: String) -> Result<i64, MaelstromError> {
        let response = self
            .node
            .send_generic_dest("seq-kv".to_owned(), SeqKvPayload::Read { key }, None)
            .await?;
        match response.body.payload {
            SeqKvPayload::ReadOk { value } => Ok(value.parse().unwrap()),
            _ => Err(MaelstromError::not_supported("Invalid response type")),
        }
    }

    pub async fn write(&self, key: String, value: String) -> Result<(), MaelstromError> {
        self.node
            .send_generic_dest("seq-kv".to_owned(), SeqKvPayload::Write { key, value }, None)
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
                None,
            )
            .await?;
        Ok(())
    }
}
