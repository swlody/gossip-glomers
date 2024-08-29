use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{OnceLock, RwLock},
};

use gossip_glomers::{
    error::{GlomerError, MaelstromError},
    node_id, parse_node_id, Handler, MaelstromMessage, Node,
};
use serde::{Deserialize, Serialize};
use tokio::time::Duration;

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RequestPayload {
    // Client requests
    Broadcast {
        message: u64,
    },
    Read,
    Topology {
        topology: BTreeMap<String, Vec<String>>,
    },
    Gossip {
        message: u64,
    },
}

#[allow(clippy::enum_variant_names)]
#[derive(Deserialize, Serialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponsePayload {
    // Client responses
    BroadcastOk,
    ReadOk { messages: Vec<u64> },
    TopologyOk,
    GossipOk,
}

struct BroadcastHandler {
    node: Node,
    seen_messages: RwLock<BTreeSet<u64>>,
    neighbors: OnceLock<Vec<u32>>,
}

impl BroadcastHandler {
    fn gossip(&self, message: u64, src: Option<u32>) -> Result<(), MaelstromError> {
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
            // TODO task tracking like in main node run loop?
            tokio::spawn(async move {
                // Start with 100 second timeout
                let mut timeout = Duration::from_millis(100);
                loop {
                    // Send message and wait for response
                    // TODO should retried messages have the same msg_id?
                    let res = node
                        .send_rpc(
                            &node_id(neighbor),
                            RequestPayload::Gossip { message },
                            Some(timeout),
                        )
                        .await;
                    match res {
                        Ok(ResponsePayload::GossipOk) => {
                            // On ack, return
                            return Ok(());
                        }
                        Err(GlomerError::Timeout) => {
                            // Backoff timeout by 100ms per failure
                            timeout += Duration::from_millis(100);
                            continue;
                        }
                        _ => {
                            return Err(MaelstromError::not_supported(
                                "Invalid response to gossip",
                            ));
                        }
                    }
                }
            });
        }
        Ok(())
    }
}

impl Handler<RequestPayload> for BroadcastHandler {
    async fn handle(
        &self,
        broadcast_msg: &MaelstromMessage<RequestPayload>,
    ) -> Result<(), MaelstromError> {
        // TODO, efficiency: at some interval, send a gossip with set of seen messages
        // ^ wouldn't need to retry on timeout, could just wait til next interval
        // in fact, wouldn't need a confirmation message at all?

        // Stats to beat:
        // :stable-latencies {0 0, 0.5 469, 0.95 674, 0.99 747, 1 808}
        // :msgs-per-op 153.39319
        match &broadcast_msg.body.payload {
            RequestPayload::Broadcast { message } => {
                // Store message in local set
                self.seen_messages.write().unwrap().insert(*message);
                // Propagate message to neighbors
                self.gossip(*message, None)?;
                // Confirm that we received and stored message
                self.node.reply(broadcast_msg, ResponsePayload::BroadcastOk);
            }
            RequestPayload::Gossip { message } => {
                // Received propagation message, store it in local set
                let inserted = self.seen_messages.write().unwrap().insert(*message);
                // If we haven't seen the message already, propagate to neighbors.
                // If we have seen it already, we've already propagated, so do nothing.
                if inserted {
                    self.gossip(*message, Some(parse_node_id(&broadcast_msg.src)?))?;
                }
                // Confirm receipt of gossip message.
                self.node.reply(broadcast_msg, ResponsePayload::GossipOk);
            }
            RequestPayload::Read => {
                // Respond with list of received messages
                self.node.reply(
                    broadcast_msg,
                    ResponsePayload::ReadOk {
                        messages: self.seen_messages.read().unwrap().iter().copied().collect(),
                    },
                );
            }
            RequestPayload::Topology { topology } => {
                // Initialization of node topology, store list of direct neighbors locally.
                let neighbors = topology
                    .get(&node_id(self.node.id))
                    .ok_or_else(|| MaelstromError::node_not_found("Invalid node in topology"))?
                    .iter()
                    .map(|n| parse_node_id(n))
                    .collect::<Result<_, _>>()?;
                self.neighbors
                    .set(neighbors)
                    .map_err(|_| MaelstromError::precondition_failed("Topology already set"))?;

                self.node.reply(broadcast_msg, ResponsePayload::TopologyOk);
            }
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let node = Node::init()?;
    let handler = BroadcastHandler {
        node: node.clone(),
        seen_messages: RwLock::new(BTreeSet::new()),
        neighbors: OnceLock::new(),
    };
    Ok(node.run(handler).await?)
}
