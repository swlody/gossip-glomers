pub mod error;

use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
    io::stdin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use error::{GlomerError, MaelstromErrorType};
use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize, Serializer};
use tokio::sync::{oneshot, Mutex};

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
        Self { kind: NodeKind::Node, id }
    }

    #[must_use]
    pub const fn client(id: u32) -> Self {
        Self { kind: NodeKind::Client, id }
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
pub struct Body<P> {
    pub msg_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<u64>,
    #[serde(flatten)]
    pub payload: P,
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
pub struct Node<P> {
    pub id: NodeId,
    pub network_ids: Vec<NodeId>,
    pub next_msg_id: AtomicU64,
    pub response_map: Mutex<BTreeMap<u64, oneshot::Sender<MaelstromMessage<P>>>>,
}

impl<P> Node<P> {
    fn send_impl<R>(
        &self,
        in_reply_to: Option<u64>,
        dest: NodeId,
        payload: &R,
    ) -> Result<u64, GlomerError>
    where
        R: Serialize,
    {
        let msg_id = self.next_msg_id.fetch_add(1, Ordering::Relaxed);
        let response =
            MaelstromMessage { src: self.id, dest, body: Body { msg_id, in_reply_to, payload } };
        let response = serde_json::to_string(&response)?;
        println!("{response}");
        Ok(msg_id)
    }

    pub fn reply(&self, source_msg: &MaelstromMessage<P>, payload: P) -> Result<(), GlomerError>
    where
        P: Serialize,
    {
        self.send_impl(Some(source_msg.body.msg_id), source_msg.src, &payload)?;
        Ok(())
    }

    pub async fn send(&self, dest: NodeId, payload: P) -> Result<MaelstromMessage<P>, GlomerError>
    where
        P: Serialize + DeserializeOwned,
    {
        let mut msg_id = self.send_impl(None, dest, &payload)?;
        let (mut tx, mut rx) = oneshot::channel();
        self.response_map.lock().await.insert(msg_id, tx);
        let mut timeout_interval = tokio::time::interval(tokio::time::Duration::from_millis(100));

        loop {
            tokio::select! {
                _ = timeout_interval.tick() => {
                    let mut guard = self.response_map.lock().await;
                    guard.remove(&msg_id);
                    msg_id = self.send_impl(None, dest, &payload)?;
                    (tx, rx) = oneshot::channel();
                    guard.insert(msg_id, tx);
                }
                response = &mut rx => {
                    return Ok(response?);
                }
            }
        }
    }
}

pub trait Handler<P> {
    fn init(node: Arc<Node<P>>) -> Self;

    fn handle(
        &self,
        msg: MaelstromMessage<P>,
    ) -> impl std::future::Future<Output = Result<(), MaelstromError>> + Send
    where
        P: DeserializeOwned;
}

pub async fn run<P, H>() -> Result<(), GlomerError>
where
    P: DeserializeOwned + Debug + Send + 'static,
    H: Handler<P> + Send + Sync + 'static,
{
    let mut buffer = String::new();
    stdin().read_line(&mut buffer)?;
    let init_msg: MaelstromMessage<Init> = serde_json::from_str::<MaelstromMessage<Init>>(&buffer)?;

    let node = Arc::new(Node {
        id: init_msg.body.payload.node_id,
        network_ids: init_msg.body.payload.node_ids,
        next_msg_id: 0.into(),
        response_map: Mutex::new(BTreeMap::new()),
    });

    node.send_impl(Some(init_msg.body.msg_id), init_msg.src, &InitOk {})?;

    let handler = Arc::new(H::init(node.clone()));
    for line in stdin().lines() {
        let line = line.unwrap();
        // TODO custom deserialization to proper error
        // The problem with this is that if we fail to parse the message,
        // we don't know who to respond to with an error!
        let request_msg = serde_json::from_str::<MaelstromMessage<P>>(&line).unwrap();
        let msg_id = request_msg.body.msg_id;
        let src = request_msg.src;

        let handler = handler.clone();
        let node = node.clone();
        tokio::spawn(async move {
            if let Some(in_reply_to) = request_msg.body.in_reply_to {
                if let Some(tx) = node.response_map.lock().await.remove(&in_reply_to) {
                    tx.send(request_msg).unwrap();
                }
            } else {
                let res = handler.handle(request_msg).await;
                if let Err(err) = res {
                    let error_type = err.error_type;
                    // let error_string: String = err.to_string();
                    node.send_impl(Some(msg_id), src, &err).unwrap();

                    match error_type {
                        MaelstromErrorType::Crash | MaelstromErrorType::Abort => panic!("oops :p"),
                        _ => {}
                    }
                }
            }
        });
    }

    Ok(())
}
