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

fn node_id_to_guid_node_id(node_id: NodeId) -> [u8; 6] {
    let mut array = [0; 6];
    array[1] = b'n';
    array[2..6].copy_from_slice(&node_id.id().to_le_bytes()[0..4]);
    array
}

fn handler(generate_msg: MaelstromMessage<Generate>, node: &mut Node<()>) -> Result<(), Error> {
    node.reply(
        generate_msg,
        GenerateOk {
            id: Uuid::now_v6(&node_id_to_guid_node_id(node.id)),
        },
    );
    Ok(())
}

fn main() {
    gossip_glomers::run::<_, _, GenerateOk>(handler, ());
}
