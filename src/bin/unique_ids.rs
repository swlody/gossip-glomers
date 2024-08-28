use gossip_glomers::{error::MaelstromError, Handler, MaelstromMessage, Node};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RequestPayload {
    Generate,
}

#[derive(Serialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponsePayload {
    GenerateOk { id: Uuid },
}

fn make_uuid(node_id: u32) -> Uuid {
    // Uuid v6 expects 6 bytes for the node ID. We are assuming 4 bytes for our
    // NodeIDs, so the last 4 characters of the GUID will always be '0000'.
    // This optimizes down to a single mov on x86 :)
    let array = node_id.to_le_bytes();
    // Use UUID v6 for unique ID. It uses current timestamp and NodeID
    // for input, so it is guaranteed to be unique per message per node.
    Uuid::now_v6(&[array[0], array[1], array[2], array[3], 0, 0])
}

struct UniqueIdHandler {
    node: Node,
}

impl Handler<RequestPayload> for UniqueIdHandler {
    async fn handle(
        &self,
        generate_msg: &MaelstromMessage<RequestPayload>,
    ) -> Result<(), MaelstromError> {
        match &generate_msg.body.payload {
            RequestPayload::Generate => {
                self.node.reply(
                    generate_msg,
                    ResponsePayload::GenerateOk {
                        id: make_uuid(self.node.id),
                    },
                );
            }
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let node = Node::init()?;
    let handler = UniqueIdHandler { node: node.clone() };
    Ok(node.run(handler).await?)
}
