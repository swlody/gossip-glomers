use gossip_glomers::{error::Error, MaelstromMessage, Node, NodeId};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "type", rename = "generate")]
struct Generate {}

#[derive(Serialize, Clone, Debug)]
#[serde(tag = "type", rename = "generate_ok")]
struct GenerateOk {
    id: Uuid,
}

fn make_uuid(node_id: NodeId) -> Uuid {
    // Uuid v6 expects 6 bytes for the node ID. We are assuming 4 bytes for our
    // NodeIDs, so the last 4 characters of the GUID will always be '0000'.
    // This optimizes down to a single mov on x86 :)
    let array = node_id.id().to_le_bytes();
    Uuid::now_v6(&[array[0], array[1], array[2], array[3], 0, 0])
}

fn handler(generate_msg: MaelstromMessage<Generate>, node: &Node, _: &mut ()) -> Result<(), Error> {
    node.reply(
        generate_msg,
        GenerateOk {
            id: make_uuid(node.id),
        },
    );
    Ok(())
}

fn main() {
    gossip_glomers::run(handler, ());
}
