pub mod error;

use std::{
    io::stdin,
    sync::atomic::{AtomicU64, Ordering},
};

use error::GlomerError;
use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize};

use crate::error::MaelstromError;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum NodeId {
    Node(u32),
    Client(u32),
}

impl NodeId {
    pub fn id(self) -> u32 {
        match self {
            Self::Node(id) => id,
            Self::Client(id) => id,
        }
    }
}

impl Serialize for NodeId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Node(id) => serializer.serialize_str(&format!("n{id}")),
            Self::Client(id) => serializer.serialize_str(&format!("c{id}")),
        }
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
            "c" => Ok(Self::Client(value)),
            "n" => Ok(Self::Node(value)),
            _ => Err(serde::de::Error::custom("invalid sender prefix")),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MaelstromMessage<Payload> {
    src: NodeId,
    dest: NodeId,
    body: Body<Payload>,
}

impl<Payload> MaelstromMessage<Payload> {
    pub fn payload(&self) -> &Payload {
        &self.body.payload
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Body<Payload> {
    msg_id: u64,
    in_reply_to: Option<u64>,
    #[serde(flatten)]
    payload: Payload,
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

#[derive(Debug)]
pub struct Node {
    pub id: NodeId,
    pub network_ids: Vec<NodeId>,
    pub current_msg_id: AtomicU64,
}

impl Node {
    fn send_impl<ResponsePayload>(
        &self,
        in_reply_to: Option<u64>,
        dest: NodeId,
        payload: ResponsePayload,
    ) -> Result<(), GlomerError>
    where
        ResponsePayload: Serialize,
    {
        let response = MaelstromMessage {
            src: self.id,
            dest,
            body: Body {
                msg_id: self.current_msg_id.fetch_add(1, Ordering::Relaxed),
                in_reply_to,
                payload,
            },
        };
        let response = serde_json::to_string(&response)?;
        println!("{response}");
        Ok(())
    }

    pub fn reply<RequestPayload, ResponsePayload>(
        &self,
        source_msg: MaelstromMessage<RequestPayload>,
        payload: ResponsePayload,
    ) -> Result<(), GlomerError>
    where
        ResponsePayload: Serialize,
    {
        self.send_impl(Some(source_msg.body.msg_id), source_msg.src, payload)
    }

    pub fn send<ResponsePayload>(
        &self,
        dest: NodeId,
        payload: ResponsePayload,
    ) -> Result<(), GlomerError>
    where
        ResponsePayload: Serialize,
    {
        self.send_impl(None, dest, payload)
    }
}

pub fn run<RequestPayload, Context>(
    handler: fn(
        MaelstromMessage<RequestPayload>,
        &Node,
        &mut Context,
    ) -> Result<(), MaelstromError>,
    mut context: Context,
) -> Result<(), GlomerError>
where
    RequestPayload: DeserializeOwned,
{
    let mut buffer = String::new();
    stdin().read_line(&mut buffer)?;
    let init_msg = serde_json::from_str::<MaelstromMessage<Init>>(&buffer)?;

    let node = Node {
        id: init_msg.body.payload.node_id,
        network_ids: init_msg.body.payload.node_ids,
        current_msg_id: 0.into(),
    };

    node.send_impl(Some(init_msg.body.msg_id), init_msg.src, InitOk {})?;

    for line in stdin().lines() {
        // TODO custom deserialization to proper error
        // The problem with this is that if we fail to parse the message,
        // we don't know who to respond to with an error!
        let request_msg = serde_json::from_str::<MaelstromMessage<RequestPayload>>(&line?)?;
        let in_reply_to = request_msg.body.msg_id;
        let reply_dest = request_msg.src;

        if let Err(err) = handler(request_msg, &node, &mut context) {
            node.send_impl(Some(in_reply_to), reply_dest, err)?;
        }
    }

    Ok(())
}
