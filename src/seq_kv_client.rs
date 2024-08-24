use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use serde::{Deserialize, Serialize};

use crate::{error::MaelstromError, message::Fallible, node::Node};

#[derive(Serialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RequestPayload {
    Read { key: String },
    Write { key: String, value: String },
    CompareAndSwap { key: String, from: String, to: String, create_if_not_exists: bool },
}

#[allow(clippy::enum_variant_names)]
#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponsePayload {
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
                next_msg_id: node.next_msg_id,
                cancellation_token: node.cancellation_token,
                response_map: Arc::new(Mutex::new(BTreeMap::new())),
            },
        }
    }

    pub async fn read(&self, key: String) -> Result<String, MaelstromError> {
        // Issue a read reqeust to seq-kv service and return the response
        let response = self.node.send("seq-kv", RequestPayload::Read { key }, None).await?;
        match response.body.payload {
            Fallible::Ok(ResponsePayload::ReadOk { value }) => Ok(value),
            // TODO don't panic here, or anywhere reliant on network input!!
            Fallible::Ok(_) => panic!("Invalid response"),
            Fallible::Err(e) => Err(e),
        }
    }

    pub async fn read_int(&self, key: String) -> Result<i64, MaelstromError> {
        let response = self.node.send("seq-kv", RequestPayload::Read { key }, None).await?;
        match response.body.payload {
            Fallible::Ok(ResponsePayload::ReadOk { value }) => Ok(value.parse().unwrap()),
            Fallible::Ok(_) => panic!("Invalid response"),
            Fallible::Err(e) => Err(e),
        }
    }

    pub async fn write(&self, key: String, value: String) -> Result<(), MaelstromError> {
        self.node
            .send::<RequestPayload, ResponsePayload>(
                "seq-kv",
                RequestPayload::Write { key, value },
                None,
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
            .send::<RequestPayload, ResponsePayload>(
                "seq-kv",
                RequestPayload::CompareAndSwap { key, from, to, create_if_not_exists },
                None,
            )
            .await?;
        Ok(())
    }
}
