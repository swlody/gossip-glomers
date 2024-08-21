use std::{
    cell::{OnceCell, RefCell},
    collections::{BTreeMap, BTreeSet},
};

use gossip_glomers::{
    error::{GlomerError, MaelstromError},
    Handler, MaelstromMessage, Node, NodeId,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RequestPayload {
    Broadcast { message: u64 },
    Read,
    Topology { topology: BTreeMap<NodeId, Vec<NodeId>> },
    Gossip { messages: BTreeSet<u64> },
    GossipAck { messages: BTreeSet<u64> },
}

#[allow(clippy::enum_variant_names)]
#[derive(Serialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponsePayload<'a> {
    BroadcastOk,
    ReadOk { messages: &'a BTreeSet<u64> },
    TopologyOk,
    // TODO avoid copying types that are both sent and received
    Gossip { messages: &'a BTreeSet<u64> },
    GossipAck { messages: &'a BTreeSet<u64> },
}

struct BroadcastHandler {
    node: Node,
    seen_messages: RefCell<BTreeSet<u64>>,
    neighbors: OnceCell<Vec<NodeId>>,
    unacked_messages: RefCell<BTreeMap<NodeId, BTreeSet<u64>>>,
}

impl BroadcastHandler {
    fn gossip(
        &self,
        messages: &BTreeSet<u64>,
        source: Option<NodeId>,
    ) -> Result<(), MaelstromError> {
        let mut unacked_messages = self.unacked_messages.borrow_mut();
        for &neighbor in self
            .neighbors
            .get()
            .ok_or_else(|| {
                MaelstromError::precondition_failed(
                    "Did not receive a topology message before a broadcast message",
                )
            })?
            .iter()
            // Don't gossip back to whichever node we received the gossip message from
            .filter(|&&n| match source {
                Some(source) => source != n,
                None => true,
            })
        {
            let neighbor_unacked = unacked_messages.get_mut(&neighbor).unwrap();
            neighbor_unacked.extend(messages);

            self.node.send(neighbor, ResponsePayload::Gossip { messages: neighbor_unacked })?;
        }
        Ok(())
    }
}

impl Handler<RequestPayload> for BroadcastHandler {
    fn init(node: Node) -> Self {
        let map = node
            .network_ids
            .iter()
            .map(|&id| (id, BTreeSet::new()))
            .collect::<BTreeMap<NodeId, BTreeSet<u64>>>();
        Self {
            node,
            seen_messages: RefCell::new(BTreeSet::new()),
            neighbors: OnceCell::new(),
            unacked_messages: RefCell::new(map.clone()),
        }
    }

    fn handle(
        &self,
        broadcast_msg: MaelstromMessage<RequestPayload>,
    ) -> Result<(), MaelstromError> {
        match &broadcast_msg.body.payload {
            RequestPayload::Broadcast { message } => {
                self.seen_messages.borrow_mut().insert(*message);
                self.node.reply(&broadcast_msg, ResponsePayload::BroadcastOk)?;
                self.gossip(&BTreeSet::from([*message]), None)?;
            }
            RequestPayload::Gossip { messages } => {
                self.node.reply(&broadcast_msg, ResponsePayload::GossipAck { messages })?;
                let mut seen_messages = self.seen_messages.borrow_mut();
                let new_messages =
                    messages.difference(&seen_messages).copied().collect::<BTreeSet<u64>>();
                seen_messages.extend(new_messages.clone());
                self.gossip(&new_messages, Some(broadcast_msg.src))?;
            }
            RequestPayload::GossipAck { messages } => {
                self.unacked_messages
                    .borrow_mut()
                    .get_mut(&broadcast_msg.src)
                    .unwrap()
                    .retain(|m| !messages.contains(m));
            }
            RequestPayload::Read => {
                self.node.reply(
                    &broadcast_msg,
                    ResponsePayload::ReadOk { messages: &self.seen_messages.borrow() },
                )?;
            }
            RequestPayload::Topology { topology } => {
                let neighbors = topology
                    .get(&self.node.id)
                    .ok_or_else(|| MaelstromError::node_not_found("Invalid node in topology"))?
                    .clone();
                self.neighbors
                    .set(neighbors)
                    .map_err(|_| MaelstromError::precondition_failed("Topology already set"))?;

                self.node.reply(&broadcast_msg, ResponsePayload::TopologyOk)?;
            }
        }

        Ok(())
    }
}

fn main() -> Result<(), GlomerError> {
    gossip_glomers::run::<RequestPayload, BroadcastHandler>()
}
