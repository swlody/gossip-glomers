use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
};

use gossip_glomers::{
    error::MaelstromError, node_id, parse_node_id, Handler, MaelstromMessage, Node,
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
        messages: BTreeSet<u64>,
    },
}

#[allow(clippy::enum_variant_names)]
#[derive(Serialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponsePayload<'a> {
    // Client responses
    BroadcastOk,
    ReadOk { messages: &'a BTreeSet<u64> },
    TopologyOk,
}

#[derive(Clone)]
struct BroadcastHandler {
    node: Node,
    seen_messages: Arc<RwLock<BTreeSet<u64>>>,
    neighbors_seen: Arc<RwLock<BTreeMap<u32, BTreeSet<u64>>>>,
}

impl BroadcastHandler {
    async fn gossip(&self) {
        // For each of our direct neighbors
        // (excluding the one which we received the gossip message from...)
        for (&neighbor, messages) in self.neighbors_seen.read().unwrap().iter() {
            // Spawn a new task to send gossip message,
            //since it may take a long time to receive a response
            let node = self.node.clone();

            let messages = self
                .seen_messages
                .read()
                .unwrap()
                .difference(messages)
                .copied()
                .collect();

            // Send message and wait for response
            node.send(&node_id(neighbor), RequestPayload::Gossip { messages });
        }
    }
}

impl Handler<RequestPayload> for BroadcastHandler {
    async fn handle(
        &self,
        broadcast_msg: &MaelstromMessage<RequestPayload>,
    ) -> Result<(), MaelstromError> {
        // Stats to beat:
        // :stable-latencies {0 0, 0.5 469, 0.95 674, 0.99 747, 1 808}
        match &broadcast_msg.body.payload {
            RequestPayload::Broadcast { message } => {
                // Store message in local set
                self.seen_messages.write().unwrap().insert(*message);
                // Confirm that we received and stored message
                self.node.reply(broadcast_msg, ResponsePayload::BroadcastOk);
            }
            RequestPayload::Gossip { messages } => {
                // Received propagation message, store it in local set
                self.seen_messages.write().unwrap().extend(messages);
                self.neighbors_seen
                    .write()
                    .unwrap()
                    .get_mut(&parse_node_id(&broadcast_msg.src).unwrap())
                    .unwrap()
                    .extend(messages)
            }
            RequestPayload::Read => {
                // Respond with list of received messages
                self.node.reply(
                    broadcast_msg,
                    ResponsePayload::ReadOk {
                        messages: &self.seen_messages.read().unwrap(),
                    },
                );
            }
            RequestPayload::Topology { topology } => {
                // Initialization of node topology, store list of direct neighbors locally.
                {
                    let mut guard = self.neighbors_seen.write().unwrap();
                    for neighbor in topology
                        .get(&node_id(self.node.id))
                        .ok_or_else(|| MaelstromError::node_not_found("Invalid node in topology"))?
                        .iter()
                        .map(|n| parse_node_id(n))
                    {
                        guard.insert(neighbor?, BTreeSet::new());
                    }
                }

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
        seen_messages: Arc::new(RwLock::new(BTreeSet::new())),
        neighbors_seen: Arc::new(RwLock::new(BTreeMap::new())),
    };
    let closed = Arc::new(AtomicBool::new(false));

    let handler_clone = handler.clone();
    let closed_clone = closed.clone();
    let handle = tokio::spawn(async move {
        while !closed_clone.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(100)).await;
            handler_clone.gossip().await;
        }
    });

    let run_result = node.run(handler).await;
    closed.store(true, Ordering::Relaxed);

    handle.await.unwrap();
    Ok(run_result?)
}
