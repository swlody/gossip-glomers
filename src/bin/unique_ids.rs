use gossip_glomers::{error::Error, GuidNodeId};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename = "generate")]
struct Generate {}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename = "generate_ok")]
struct GenerateOk {
    id: Uuid,
}

fn generate_handler(_: &Generate, node_id: GuidNodeId) -> Result<GenerateOk, Error> {
    Ok(GenerateOk {
        id: Uuid::now_v6(&node_id.0),
    })
}

fn main() {
    gossip_glomers::run(generate_handler);
}
