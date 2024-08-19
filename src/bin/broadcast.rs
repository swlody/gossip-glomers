use std::collections::{HashMap, HashSet};

use gossip_glomers::{
    error::{GlomerError, MaelstromError},
    MaelstromMessage, Node, NodeId,
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

struct Context {
    seen_messages: HashSet<u64>,
    neighbors: Vec<NodeId>,
}

fn gossip(node: &Node, neighbors: &[NodeId], message: u64) -> Result<(), MaelstromError> {
    for &neighbor in neighbors {
        node.send(neighbor, RequestPayload::Gossip { message })?;
    }
    Ok(())
}

fn handler(
    broadcast_msg: MaelstromMessage<RequestPayload>,
    node: &Node,
    ctx: &mut Context,
) -> Result<(), MaelstromError> {
    match broadcast_msg.payload() {
        RequestPayload::Broadcast { message } => {
            ctx.seen_messages.insert(*message);
            gossip(node, &ctx.neighbors, *message)?;
            node.reply(broadcast_msg, ResponsePayload::BroadcastOk)?
        }
        RequestPayload::Gossip { message } => {
            if ctx.seen_messages.insert(*message) {
                gossip(node, &ctx.neighbors, *message)?;
            }
        }
        RequestPayload::Read => node.reply(
            broadcast_msg,
            ResponsePayload::ReadOk {
                // TODO zero copy?
                messages: ctx.seen_messages.clone(),
            },
        )?,
        RequestPayload::Topology { topology } => {
            ctx.neighbors = topology
                .get(&node.id)
                .ok_or_else(|| MaelstromError::node_not_found("Invalid node in topology"))?
                .clone();
            node.reply(broadcast_msg, ResponsePayload::TopologyOk)?;
        }
    }

    Ok(())
}

fn main() -> Result<(), GlomerError> {
    gossip_glomers::run(
        handler,
        Context {
            seen_messages: HashSet::new(),
            neighbors: Vec::new(),
        },
    )
}
