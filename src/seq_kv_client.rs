use serde::{Deserialize, Serialize};

use crate::{
    error::{error_type, GlomerError, MaelstromError},
    node::Node,
};

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
    name: &'static str,
}

// TODO support other maelstrom services
impl SeqKvClient {
    #[must_use]
    pub const fn new(node: Node) -> Self {
        Self {
            node,
            name: "seq-kv",
        }
    }

    pub async fn read(&self, key: &str) -> Result<String, GlomerError> {
        // Issue a read reqeust to seq-kv service and return the response
        let response = self
            .node
            .send_rpc(self.name, RequestPayload::Read { key }, None)
            .await;
        match response {
            Ok(ResponsePayload::ReadOk { value }) => Ok(value),
            Err(
                e @ GlomerError::Maelstrom(MaelstromError {
                    code: error_type::KEY_DOES_NOT_EXIST,
                    ..
                }),
            ) => Err(e),
            _ => Err(GlomerError::Unsupported(
                "Invalid response to read request".into(),
            )),
        }
    }

    pub async fn read_int(&self, key: &str) -> Result<i64, GlomerError> {
        match self.read(key).await {
            Ok(value) => Ok(value
                .parse::<i64>()
                // TODO Parse error implies error in parsing message
                // this is an error in parsing something that we stored internally,
                // trying to read an int from something that is not an int
                .map_err(|e| GlomerError::Parse(e.to_string()))?),
            Err(e) => Err(e),
        }
    }

    pub async fn write(&self, key: &str, value: &str) -> Result<(), GlomerError> {
        let response = self
            .node
            .send_rpc(self.name, RequestPayload::Write { key, value }, None)
            .await;
        match response {
            Ok(ResponsePayload::WriteOk) => Ok(()),
            Ok(_) | Err(GlomerError::Maelstrom(_)) => Err(GlomerError::Unsupported(
                "Invalid response to write request".into(),
            )),
            Err(e) => Err(e),
        }
    }

    pub async fn compare_and_swap(
        &self,
        key: &str,
        from: &str,
        to: &str,
        create_if_not_exists: bool,
    ) -> Result<(), GlomerError> {
        let response = self
            .node
            .send_rpc(
                self.name,
                RequestPayload::CompareAndSwap {
                    key,
                    from,
                    to,
                    create_if_not_exists,
                },
                None,
            )
            .await;
        match response {
            Ok(ResponsePayload::CompareAndSwapOk) => Ok(()),
            Err(e @ GlomerError::Maelstrom(MaelstromError { code, .. }))
                if code == error_type::PRECONDITION_FAILED
                    || code == error_type::KEY_DOES_NOT_EXIST =>
            {
                Err(e)
            }
            _ => Err(GlomerError::Unsupported(
                "Invalid response to compare and swap request".into(),
            )),
        }
    }
}
