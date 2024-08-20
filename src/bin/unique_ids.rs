use gossip_glomers::{
    error::{GlomerError, MaelstromError},
    Handler, MaelstromMessage, Node, NodeId,
};
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

struct UniqueIdHandler {
    node: Node,
}

impl Handler<Generate> for UniqueIdHandler {
    fn init(node: Node) -> Self {
        Self { node }
    }

    fn handle(&self, generate_msg: MaelstromMessage<Generate>) -> Result<(), MaelstromError> {
        self.node.reply(
            generate_msg,
            GenerateOk {
                id: make_uuid(self.node.id),
            },
        )?;
        Ok(())
    }
}

fn main() -> Result<(), GlomerError> {
    gossip_glomers::run::<Generate, UniqueIdHandler>()
}
