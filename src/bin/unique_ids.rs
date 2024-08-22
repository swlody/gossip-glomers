use std::sync::Arc;

use gossip_glomers::{error::MaelstromError, Handler, MaelstromMessage, Node, NodeId};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Payload {
    Generate,
    GenerateOk { id: Uuid },
}

fn make_uuid(node_id: NodeId) -> Uuid {
    // Uuid v6 expects 6 bytes for the node ID. We are assuming 4 bytes for our
    // NodeIDs, so the last 4 characters of the GUID will always be '0000'.
    // This optimizes down to a single mov on x86 :)
    let array = node_id.id.to_le_bytes();
    Uuid::now_v6(&[array[0], array[1], array[2], array[3], 0, 0])
}

struct UniqueIdHandler {
    node: Arc<Node<Payload>>,
}

impl Handler<Payload> for UniqueIdHandler {
    fn init(node: Arc<Node<Payload>>) -> Self {
        Self { node }
    }

    async fn handle(&self, generate_msg: MaelstromMessage<Payload>) -> Result<(), MaelstromError> {
        match &generate_msg.body.payload {
            Payload::Generate => {
                self.node
                    .reply(&generate_msg, Payload::GenerateOk { id: make_uuid(self.node.id) })?;
            }
            Payload::GenerateOk { .. } => {
                return Err(MaelstromError::not_supported("Invalid message type"));
            }
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    gossip_glomers::run::<Payload, UniqueIdHandler>().await
}
