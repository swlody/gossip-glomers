use gossip_glomers::{
    error::{MaelstromError, MaelstromErrorType},
    seq_kv_client::SeqKvClient,
    Handler, MaelstromMessage, Node,
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
        counter_msg: &MaelstromMessage<RequestPayload>,
    ) -> Result<(), MaelstromError> {
        match counter_msg.body.payload {
            RequestPayload::Add { delta } => {
                loop {
                    let current_value =
                        self.client.read_int("counter".to_string()).await.unwrap_or(0);
                    let new_value = current_value + delta;
                    let res = self
                        .client
                        .compare_and_swap(
                            "counter".to_string(),
                            if current_value == 0 {
                                "".to_string()
                            } else {
                                current_value.to_string()
                            },
                            new_value.to_string(),
                            true,
                        )
                        .await;
                    match res {
                        Err(MaelstromError {
                            error_type: MaelstromErrorType::PreconditionFailed,
                            ..
                        }) => continue,
                        // TODO no panic here
                        Err(_) => panic!("Invalid response"),
                        Ok(_) => break,
                    }
                }
                self.node.reply(counter_msg, ResponsePayload::AddOk);
            }
            RequestPayload::Read => {
                let value = self.client.read_int("counter".to_string()).await.unwrap_or(0);
                self.node.reply(counter_msg, ResponsePayload::ReadOk { value });
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
    node.run(handler).await
}
