use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, OnceLock, RwLock},
};

use gossip_glomers::{
    error::{GlomerError, MaelstromError},
    Handler, MaelstromMessage, Node, NodeId,
};
use serde::{Deserialize, Serialize};

// Requests from client
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Payload {
    // Client requests
    Broadcast { message: u64 },
    Read,
    Topology { topology: BTreeMap<NodeId, Vec<NodeId>> },

    // Client responses
    BroadcastOk,
    ReadOk { messages: BTreeSet<u64> },
    TopologyOk,

    // Internal
    Gossip { message: u64 },
    GossipOk,
}

struct BroadcastHandler {
    node: Arc<Node<Payload>>,
    seen_messages: RwLock<BTreeSet<u64>>,
    neighbors: OnceLock<Vec<NodeId>>,
}

impl BroadcastHandler {
    async fn gossip(&self, message: u64) -> Result<(), MaelstromError> {
        for &neighbor in self
            .neighbors
            .get()
            .ok_or_else(|| {
                MaelstromError::precondition_failed(
                    "Did not receive a topology message before a broadcast message",
                )
            })?
            .iter()
        {
            self.node.send(neighbor, Payload::Gossip { message }).await?;
        }
        Ok(())
    }
}

impl Handler<Payload> for BroadcastHandler {
    fn init(node: Arc<Node<Payload>>) -> Self {
        Self { node, seen_messages: RwLock::new(BTreeSet::new()), neighbors: OnceLock::new() }
    }

    async fn handle(&self, broadcast_msg: MaelstromMessage<Payload>) -> Result<(), MaelstromError> {
        match &broadcast_msg.body.payload {
            Payload::Broadcast { message } => {
                self.seen_messages.write().unwrap().insert(*message);
                self.node.reply(&broadcast_msg, Payload::BroadcastOk)?;
                self.gossip(*message).await?;
            }
            Payload::Gossip { message } => {
                if self.seen_messages.write().unwrap().insert(*message) {
                    self.gossip(*message).await?;
                }
                self.node.reply(&broadcast_msg, Payload::GossipOk)?;
            }
            Payload::Read => {
                self.node.reply(
                    &broadcast_msg,
                    Payload::ReadOk { messages: self.seen_messages.read().unwrap().clone() },
                )?;
            }
            Payload::Topology { topology } => {
                let neighbors = topology
                    .get(&self.node.id)
                    .ok_or_else(|| MaelstromError::node_not_found("Invalid node in topology"))?
                    .clone();
                self.neighbors
                    .set(neighbors)
                    .map_err(|_| MaelstromError::precondition_failed("Topology already set"))?;

                self.node.reply(&broadcast_msg, Payload::TopologyOk)?;
            }
            Payload::BroadcastOk
            | Payload::ReadOk { .. }
            | Payload::TopologyOk
            | Payload::GossipOk => {
                return Err(MaelstromError::not_supported("Unexpected message type"));
            }
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), GlomerError> {
    gossip_glomers::run::<Payload, BroadcastHandler>().await
}
