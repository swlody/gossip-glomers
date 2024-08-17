use std::collections::HashMap;

use gossip_glomers::{error::Error, MaelstromMessage, Node, NodeId};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RequestPayload {
    Broadcast {
        message: u64,
    },
    Read,
    Topology {
        topology: HashMap<NodeId, Vec<NodeId>>,
    },
}

#[allow(clippy::enum_variant_names)]
#[derive(Serialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponsePayload {
    BroadcastOk,
    ReadOk { messages: Vec<u64> },
    TopologyOk,
}

struct Context {
    seen_messages: Vec<u64>,
    neighbors: Vec<NodeId>,
}

fn handler(
    message: &MaelstromMessage<RequestPayload>,
    node: &mut Node<Context>,
) -> Result<ResponsePayload, Error> {
    match message.payload() {
        RequestPayload::Broadcast { message } => {
            node.ctx.seen_messages.push(*message);

            for _neighbor in &node.ctx.neighbors {
                // TODO gossip!
            }

            Ok(ResponsePayload::BroadcastOk)
        }
        RequestPayload::Read => Ok(ResponsePayload::ReadOk {
            messages: node.ctx.seen_messages.clone(),
        }),
        RequestPayload::Topology { topology } => {
            node.ctx.neighbors = topology.get(&node.id).unwrap().clone();
            Ok(ResponsePayload::TopologyOk)
        }
    }
}

fn main() {
    gossip_glomers::run(
        handler,
        Context {
            seen_messages: Vec::new(),
            neighbors: Vec::new(),
        },
    );
}
