use gossip_glomers::{
    error::MaelstromError, seq_kv_client::SeqKvClient, Handler, MaelstromMessage, Node,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RequestPayload {
    Add { delta: i64 },
    Read,
}

#[derive(Serialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponsePayload {
    AddOk,
    ReadOk { value: i64 },
}

struct CounterHandler {
    node: Node,
    client: SeqKvClient,
}

impl Handler<RequestPayload> for CounterHandler {
    async fn handle(
        &self,
        counter_msg: MaelstromMessage<RequestPayload>,
    ) -> Result<(), MaelstromError> {
        match counter_msg.body.payload {
            RequestPayload::Add { delta } => {
                self.client.write("counter".to_string(), delta.to_string()).await?;
                self.node.reply(&counter_msg, ResponsePayload::AddOk)?;
            }
            RequestPayload::Read => {
                let value = self.client.read_int("counter".to_string()).await?;
                self.node.reply(&counter_msg, ResponsePayload::ReadOk { value })?;
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let node = Node::init().await?;
    // TODO reduce node cloning?
    let handler = CounterHandler { node: node.clone(), client: SeqKvClient::new(node.clone()) };
    handler.client.write("counter".to_string(), "10".to_string()).await?;
    assert_eq!(10, handler.client.read_int("counter".to_string()).await?);
    node.run(handler).await
}
