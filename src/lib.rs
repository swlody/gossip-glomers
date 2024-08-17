pub mod error;

use std::io::stdin;

use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize};

use crate::error::Error;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum NodeId {
    Node(u32),
    Client(u32),
}

fn node_id_to_guid_node_id(node_id: NodeId) -> [u8; 6] {
    let (c, n) = match node_id {
        NodeId::Node(n) => (b'n', n),
        NodeId::Client(n) => (b'c', n),
    };
    let mut array = [0; 6];
    array[1] = c;
    array[2..6].copy_from_slice(&n.to_be_bytes()[0..4]);
    array
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

impl<T> MaelstromMessage<T> {
    pub fn reply_with_payload<Payload: Serialize>(
        &self,
        payload: Payload,
    ) -> MaelstromMessage<Payload> {
        MaelstromMessage {
            src: self.dest,
            dest: self.src,
            body: Body {
                msg_id: self.body.msg_id,
                in_reply_to: Some(self.body.msg_id),
                payload,
            },
        }
    }

    pub fn payload(&self) -> &T {
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

#[derive(Clone, Debug)]
pub struct Node<Context> {
    pub id: NodeId,
    pub guid_id: [u8; 6],
    pub network_ids: Vec<NodeId>,
    pub current_msg_id: u64,
    pub ctx: Context,
}

pub fn run<RequestPayload, Context, ResponsePayload>(
    handler: fn(
        &MaelstromMessage<RequestPayload>,
        &mut Node<Context>,
    ) -> Result<ResponsePayload, Error>,
    context: Context,
) where
    RequestPayload: DeserializeOwned,
    ResponsePayload: Serialize,
{
    let mut buffer = String::new();
    stdin().read_line(&mut buffer).unwrap();

    let init_msg = serde_json::from_str::<MaelstromMessage<Init>>(&buffer).unwrap();

    let mut ctx = Node {
        id: init_msg.body.payload.node_id,
        guid_id: node_id_to_guid_node_id(init_msg.body.payload.node_id),
        network_ids: init_msg.body.payload.node_ids,
        current_msg_id: 1,
        ctx: context,
    };

    let init_response = MaelstromMessage {
        src: init_msg.body.payload.node_id,
        dest: init_msg.src,
        body: Body {
            msg_id: ctx.current_msg_id,
            in_reply_to: Some(init_msg.body.msg_id),
            payload: InitOk {},
        },
    };
    ctx.current_msg_id += 1;

    let init_response_str = serde_json::to_string(&init_response).unwrap();
    println!("{init_response_str}");

    for line in stdin().lines() {
        let line = line.unwrap();
        // TODO custom deserialization to proper error
        let request_msg = serde_json::from_str::<MaelstromMessage<RequestPayload>>(&line).unwrap();

        let response = handler(&request_msg, &mut ctx);

        match response {
            Ok(response_payload) => {
                let response_msg = request_msg.reply_with_payload(response_payload);
                let response_str = serde_json::to_string(&response_msg).unwrap();
                println!("{response_str}");
            }
            Err(err) => {
                let error_msg = request_msg.reply_with_payload(err);
                let error_str = serde_json::to_string(&error_msg).unwrap();
                println!("{error_str}");
            }
        }

        ctx.current_msg_id += 1;
    }
}
