use std::{
    cell::{OnceCell, RefCell},
    collections::{HashMap, HashSet},
};

use gossip_glomers::{
    error::{GlomerError, MaelstromError},
    Handler, MaelstromMessage, Node, NodeId,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RequestPayload {
    Broadcast {
        message: u64,
    },
    Read,
    Topology {
        topology: HashMap<NodeId, Vec<NodeId>>,
    },
    Gossip {
        message: u64,
    },
}

#[allow(clippy::enum_variant_names)]
#[derive(Serialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponsePayload {
    BroadcastOk,
    ReadOk { messages: HashSet<u64> },
    TopologyOk,
}

struct BroadcastHandler {
    node: Node,
    seen_messages: RefCell<HashSet<u64>>,
    neighbors: OnceCell<Vec<NodeId>>,
}

impl BroadcastHandler {
    fn gossip(&self, message: u64) -> Result<(), MaelstromError> {
        for &neighbor in self.neighbors.get().ok_or_else(|| {
            MaelstromError::precondition_failed(
                "Did not receive a topology message before a broadcast message",
            )
        })? {
            self.node
                .send(neighbor, RequestPayload::Gossip { message })?;
        }
        Ok(())
    }
}

impl Handler<RequestPayload> for BroadcastHandler {
    fn init(node: Node) -> Self {
        Self {
            node,
            seen_messages: RefCell::new(HashSet::new()),
            neighbors: OnceCell::new(),
        }
    }

    fn handle(
        &self,
        broadcast_msg: MaelstromMessage<RequestPayload>,
    ) -> Result<(), MaelstromError> {
        match broadcast_msg.payload() {
            RequestPayload::Broadcast { message } => {
                self.seen_messages.borrow_mut().insert(*message);
                self.gossip(*message)?;
                self.node
                    .reply(&broadcast_msg, ResponsePayload::BroadcastOk)?;
            }
            RequestPayload::Gossip { message } => {
                if self.seen_messages.borrow_mut().insert(*message) {
                    self.gossip(*message)?;
                }
            }
            RequestPayload::Read => self.node.reply(
                &broadcast_msg,
                ResponsePayload::ReadOk {
                    // TODO zero copy?
                    messages: self.seen_messages.borrow().clone(),
                },
            )?,
            RequestPayload::Topology { topology } => {
                self.neighbors
                    .set(
                        topology
                            .get(&self.node.id)
                            .ok_or_else(|| {
                                MaelstromError::node_not_found("Invalid node in topology")
                            })?
                            .clone(),
                    )
                    .map_err(|_| MaelstromError::precondition_failed("Topology already set"))?;
                self.node
                    .reply(&broadcast_msg, ResponsePayload::TopologyOk)?;
            }
        }

        Ok(())
    }
}

fn main() -> Result<(), GlomerError> {
    gossip_glomers::run::<RequestPayload, BroadcastHandler>()
}
