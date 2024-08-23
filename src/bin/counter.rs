use std::sync::Arc;

use gossip_glomers::{
    error::MaelstromError, seq_kv_client::SeqKvClient, Handler, MaelstromMessage, Node,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Payload {
    Add { delta: i64 },
    Read,

    AddOk,
    ReadOk { value: i64 },
}

struct CounterHandler {
    node: Arc<Node<Payload>>,
    client: Arc<SeqKvClient>,
}

impl Handler<Payload> for CounterHandler {
    fn init(node: Arc<Node<Payload>>) -> Self {
        Self { node: node.clone(), client: Arc::new(SeqKvClient::new(&node)) }
    }

    async fn handle(&self, counter_msg: MaelstromMessage<Payload>) -> Result<(), MaelstromError> {
        match counter_msg.body.payload {
            Payload::Add { delta } => {
                self.client.write("val".to_string(), delta.to_string()).await?;
                self.node.reply(&counter_msg, Payload::AddOk)?;
            }
            Payload::Read => {
                let value = self.client.read_int("val".to_string()).await?;
                self.node.reply(&counter_msg, Payload::ReadOk { value })?;
            }
            Payload::AddOk | Payload::ReadOk { .. } => {
                return Err(MaelstromError::not_supported("Unexpected message type"));
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    gossip_glomers::run::<Payload, CounterHandler>().await
}
