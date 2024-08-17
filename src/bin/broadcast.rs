use std::collections::{HashMap, HashSet};

use gossip_glomers::{error::Error, MaelstromMessage, Node, NodeId};
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

struct Context {
    seen_messages: HashSet<u64>,
    neighbors: Vec<NodeId>,
}

fn gossip(node: &mut Node<Context>, message: u64) {
    for i in 0..node.ctx.neighbors.len() {
        node.send(node.ctx.neighbors[i], RequestPayload::Gossip { message });
    }
}

fn handler(
    broadcast_msg: &MaelstromMessage<RequestPayload>,
    node: &mut Node<Context>,
) -> Result<(), Error> {
    match broadcast_msg.payload() {
        RequestPayload::Broadcast { message } => {
            node.ctx.seen_messages.insert(*message);
            gossip(node, *message);
            node.reply(broadcast_msg, ResponsePayload::BroadcastOk);
        }
        RequestPayload::Gossip { message } => {
            if node.ctx.seen_messages.insert(*message) {
                gossip(node, *message);
            }
        }
        RequestPayload::Read => node.reply(
            broadcast_msg,
            ResponsePayload::ReadOk {
                // TODO zero copy?
                messages: node.ctx.seen_messages.clone(),
            },
        ),
        RequestPayload::Topology { topology } => {
            node.ctx.neighbors = topology.get(&node.id).unwrap().clone();
            node.reply(broadcast_msg, ResponsePayload::TopologyOk)
        }
    }

    Ok(())
}

fn main() {
    gossip_glomers::run::<_, _, ResponsePayload>(
        handler,
        Context {
            seen_messages: HashSet::new(),
            neighbors: Vec::new(),
        },
    );
}
