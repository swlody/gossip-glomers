pub mod error;

use std::{
    fmt::Display,
    io::stdin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use error::{GlomerError, MaelstromErrorType};
use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize, Serializer};

use crate::error::MaelstromError;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum NodeKind {
    Node,
    Client,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct NodeId {
    pub kind: NodeKind,
    pub id: u32,
}

impl NodeId {
    #[must_use]
    pub const fn node(id: u32) -> Self {
        Self {
            kind: NodeKind::Node,
            id,
        }
    }

    #[must_use]
    pub const fn client(id: u32) -> Self {
        Self {
            kind: NodeKind::Client,
            id,
        }
    }
}

impl Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            NodeKind::Node => write!(f, "n{}", self.id),
            NodeKind::Client => write!(f, "c{}", self.id),
        }
    }
}

impl Serialize for NodeId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for NodeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        let prefix = &s[..1];
        let value = s[1..].parse::<u32>().map_err(serde::de::Error::custom)?;

        match prefix {
            "c" => Ok(Self::client(value)),
            "n" => Ok(Self::node(value)),
            _ => Err(serde::de::Error::custom("invalid sender prefix")),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MaelstromMessage<P> {
    pub src: NodeId,
    pub dest: NodeId,
    pub body: Body<P>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Body<Payload> {
    pub msg_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<u64>,
    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "type", rename = "init")]
struct Init {
    node_id: NodeId,
    node_ids: Vec<NodeId>,
}

#[derive(Serialize, Clone, Debug)]
#[serde(tag = "type", rename = "init_ok")]
struct InitOk {}

#[derive(Clone, Debug)]
pub struct Node {
    pub id: NodeId,
    pub network_ids: Vec<NodeId>,
    pub next_msg_id: Arc<AtomicU64>,
}

impl Node {
    fn send_impl<R>(
        &self,
        in_reply_to: Option<u64>,
        dest: NodeId,
        payload: R,
    ) -> Result<(), GlomerError>
    where
        R: Serialize,
    {
        let response = MaelstromMessage {
            src: self.id,
            dest,
            body: Body {
                msg_id: self.next_msg_id.fetch_add(1, Ordering::Relaxed),
                in_reply_to,
                payload,
            },
        };
        let response = serde_json::to_string(&response)?;
        println!("{response}");
        Ok(())
    }

    pub fn reply<RequestPayload, R>(
        &self,
        source_msg: &MaelstromMessage<RequestPayload>,
        payload: R,
    ) -> Result<(), GlomerError>
    where
        R: Serialize,
    {
        self.send_impl(Some(source_msg.body.msg_id), source_msg.src, payload)
    }

    pub fn send<R>(&self, dest: NodeId, payload: R) -> Result<(), GlomerError>
    where
        R: Serialize,
    {
        self.send_impl(None, dest, payload)
    }
}

pub trait Handler<P> {
    fn init(node: Node) -> Self;

    fn handle(&self, msg: MaelstromMessage<P>) -> Result<(), MaelstromError>
    where
        P: DeserializeOwned;
}

pub fn run<P, H>() -> Result<(), GlomerError>
where
    P: DeserializeOwned,
    H: Handler<P>,
{
    let mut buffer = String::new();
    stdin().read_line(&mut buffer)?;
    let init_msg = serde_json::from_str::<MaelstromMessage<Init>>(&buffer)?;

    let node = Node {
        id: init_msg.body.payload.node_id,
        network_ids: init_msg.body.payload.node_ids,
        next_msg_id: Arc::new(0.into()),
    };

    node.send_impl(Some(init_msg.body.msg_id), init_msg.src, InitOk {})?;

    let handler = H::init(node.clone());

    for line in stdin().lines() {
        let line = line?;
        // TODO custom deserialization to proper error
        // The problem with this is that if we fail to parse the message,
        // we don't know who to respond to with an error!
        let request_msg = serde_json::from_str::<MaelstromMessage<P>>(&line)?;
        let in_reply_to = request_msg.body.msg_id;
        let reply_dest = request_msg.src;

        if let Err(err) = handler.handle(request_msg) {
            let error_type = err.error_type;
            let error_string = err.to_string();
            node.send_impl(Some(in_reply_to), reply_dest, err)?;

            match error_type {
                MaelstromErrorType::Crash | MaelstromErrorType::Abort => {
                    return Err(GlomerError::Abort(format!("Aborting with: {error_string}")))
                }
                _ => {}
            }
        }
    }

    Ok(())
}
