use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{OnceLock, RwLock},
};

use gossip_glomers::{
    error::{MaelstromError, MaelstromErrorType},
    message::Body,
    Handler, MaelstromMessage, Node, NodeId,
};
use serde::{Deserialize, Serialize};
use tokio::time::Duration;

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
    node: Node,
    seen_messages: RwLock<BTreeSet<u64>>,
    neighbors: OnceLock<Vec<NodeId>>,
}

impl BroadcastHandler {
    async fn gossip(&self, message: u64, src: Option<NodeId>) -> Result<(), MaelstromError> {
        // For each of our direct neighbors
        // (excluding the one which we received the gossip message from...)
        for &neighbor in self
            .neighbors
            .get()
            .ok_or_else(|| {
                MaelstromError::precondition_failed(
                    "Did not receive a topology message before a broadcast message",
                )
            })?
            .iter()
            .filter(|&&id| src.map_or(true, |src| id != src))
        {
            // Spawn a new task to send gossip message,
            //since it may take a long time to receive a response
            let node = self.node.clone();
            tokio::spawn(async move {
                // Start with 100 second timeout
                let mut timeout = Duration::from_millis(100);
                loop {
                    // Send message and wait for response
                    match node.send(neighbor, Payload::Gossip { message }, Some(timeout)).await {
                        Ok(MaelstromMessage {
                            body: Body { payload: Payload::GossipOk, .. },
                            ..
                        }) => {
                            // On ack, return
                            return Ok(());
                        }
                        Ok(_) => {
                            return Err(MaelstromError::not_supported("Invalid response to gossip"))
                        }
                        Err(e) if e.error_type == MaelstromErrorType::Timeout => {
                            // Backoff timeout by 100ms per failure
                            timeout += Duration::from_millis(100);
                            continue;
                        }
                        Err(e) => {
                            // Bubble up all other errors
                            return Err(e);
                        }
                    }
                }
            });
        }
        Ok(())
    }
}

impl Handler<Payload> for BroadcastHandler {
    fn init(node: Node) -> Self {
        Self { node, seen_messages: RwLock::new(BTreeSet::new()), neighbors: OnceLock::new() }
    }

    async fn handle(&self, broadcast_msg: MaelstromMessage<Payload>) -> Result<(), MaelstromError> {
        match &broadcast_msg.body.payload {
            Payload::Broadcast { message } => {
                // Store message in local set
                self.seen_messages.write().unwrap().insert(*message);
                // Confirm that we received and stored message
                self.node.reply(&broadcast_msg, Payload::BroadcastOk)?;
                // Propagate message to neighbors
                self.gossip(*message, None).await?;
            }
            Payload::Gossip { message } => {
                // Received propagation message, store it in local set
                let inserted = self.seen_messages.write().unwrap().insert(*message);
                // Confirm receipt of gossip message.
                self.node.reply(&broadcast_msg, Payload::GossipOk)?;
                // If we haven't seen the message already, propagate to neighbors.
                // If we have seen it already, we've already propagated, so do nothing.
                if inserted {
                    self.gossip(*message, Some(broadcast_msg.src)).await?;
                }
            }
            Payload::Read => {
                // Respond with list of received messages
                self.node.reply(
                    &broadcast_msg,
                    Payload::ReadOk { messages: self.seen_messages.read().unwrap().clone() },
                )?;
            }
            Payload::Topology { topology } => {
                // Initialization of node topology, store list of direct neighbors locally.
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
async fn main() -> eyre::Result<()> {
    gossip_glomers::run::<Payload, BroadcastHandler>().await
}
