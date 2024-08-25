use gossip_glomers::{
    error::{
        error_type::{self},
        MaelstromError,
    },
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
        // TODO timeout and keep local count?
        // "given a few seconds without writes, converge on the correct value"
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
                            current_value.to_string(),
                            new_value.to_string(),
                            true,
                        )
                        .await;
                    match res {
                        Err(MaelstromError { code: error_type::PRECONDITION_FAILED, .. }) => {
                            continue;
                        }
                        Err(MaelstromError { code: error_type::KEY_DOES_NOT_EXIST, .. }) => {
                            break;
                        }
                        Ok(()) => {
                            break;
                        }
                        Err(e) => return Err(MaelstromError::not_supported(e.to_string())),
                    }
                }
                self.node.reply(counter_msg, ResponsePayload::AddOk);
            }
            RequestPayload::Read => {
                let value = match self.client.read_int("counter".to_string()).await {
                    Ok(v) => v,
                    Err(MaelstromError { code: error_type::KEY_DOES_NOT_EXIST, .. }) => 0,
                    Err(e) => return Err(MaelstromError::not_supported(e.to_string())),
                };
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
