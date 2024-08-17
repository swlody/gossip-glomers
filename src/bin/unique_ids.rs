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

fn handler(_: &MaelstromMessage<Generate>, ctx: &mut Node<()>) -> Result<GenerateOk, Error> {
    Ok(GenerateOk {
        id: Uuid::now_v6(&ctx.guid_id),
    })
}

fn main() {
    gossip_glomers::run(handler, ());
}
