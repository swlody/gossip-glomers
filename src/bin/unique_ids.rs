use gossip_glomers::{error::Error, MaelstromMessage, Node};
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

fn handler(generate_msg: &MaelstromMessage<Generate>, node: &mut Node<()>) -> Result<(), Error> {
    node.reply(
        &generate_msg,
        GenerateOk {
            id: Uuid::now_v6(&node.guid_id),
        },
    );
    Ok(())
}

fn main() {
    gossip_glomers::run::<_, _, GenerateOk>(handler, ());
}
