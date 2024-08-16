pub mod error;

use std::io::{stdin, BufRead as _};

use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize};

use crate::error::Error;

#[derive(Copy, Clone, Debug)]
pub struct GuidNodeId(pub [u8; 6]);

#[derive(Copy, Clone, Debug)]
enum NodeId {
    Node(u32),
    Client(u32),
}

impl From<NodeId> for GuidNodeId {
    fn from(value: NodeId) -> Self {
        let (c, n) = match value {
            NodeId::Node(n) => (b'n', n),
            NodeId::Client(n) => (b'c', n),
        };
        let mut array = [0; 6];
        array[1] = c;
        array[2..6].copy_from_slice(&n.to_be_bytes()[0..4]);
        Self(array)
    }
}

impl Serialize for NodeId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            NodeId::Node(id) => serializer.serialize_str(&format!("n{id}")),
            NodeId::Client(id) => serializer.serialize_str(&format!("c{id}")),
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
            "c" => Ok(NodeId::Client(value)),
            "n" => Ok(NodeId::Node(value)),
            _ => Err(serde::de::Error::custom("invalid sender prefix")),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct MaelstromMessage<T> {
    src: NodeId,
    dest: NodeId,
    body: Body<T>,
}

impl<T> MaelstromMessage<T> {
    fn reply_with_payload<U: Serialize>(self, payload: U) -> MaelstromMessage<U> {
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
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Body<T> {
    msg_id: u64,
    in_reply_to: Option<u64>,
    #[serde(flatten)]
    payload: T,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename = "init")]
struct Init {
    node_id: NodeId,
    node_ids: Vec<NodeId>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename = "init_ok")]
struct InitOk {}

pub fn run<RequestPayload: DeserializeOwned + Clone, ResponsePayload: Serialize>(
    handler: fn(&RequestPayload, GuidNodeId) -> Result<ResponsePayload, Error>,
) {
    let mut stdin = stdin().lock();
    let mut buffer = String::new();
    stdin.read_line(&mut buffer).unwrap();

    let init_msg = serde_json::from_str::<MaelstromMessage<Init>>(&buffer).unwrap();

    let our_node_id = GuidNodeId::from(init_msg.body.payload.node_id);

    let mut current_msg_id = 1;

    let init_response = MaelstromMessage {
        src: init_msg.body.payload.node_id,
        dest: init_msg.src,
        body: Body {
            msg_id: current_msg_id,
            in_reply_to: Some(init_msg.body.msg_id),
            payload: InitOk {},
        },
    };
    current_msg_id += 1;

    let init_response_str = serde_json::to_string(&init_response).unwrap();
    println!("{}", init_response_str);

    for line in stdin.lines() {
        let line = line.unwrap();
        // TODO custom deserialization to proper error
        let request_msg = serde_json::from_str::<MaelstromMessage<RequestPayload>>(&line).unwrap();

        let response = handler(&request_msg.body.payload, our_node_id);

        match response {
            Ok(response_payload) => {
                let response_msg = request_msg.reply_with_payload(response_payload);
                let response_str = serde_json::to_string(&response_msg).unwrap();
                println!("{}", response_str);
            }
            Err(err) => {
                let error_msg = request_msg.reply_with_payload(err);
                let error_str = serde_json::to_string(&error_msg).unwrap();
                println!("{}", error_str);
            }
        }

        current_msg_id += 1;
    }
}
