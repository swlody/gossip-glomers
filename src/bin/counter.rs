use gossip_glomers::{
    error::{
        error_type::{self},
        GlomerError, MaelstromError,
    },
    seq_kv_client::SeqKvClient,
    Handler, MaelstromMessage, Node,
};
use serde::{Deserialize, Serialize};
use tokio::time::Duration;

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RequestPayload {
    Add { delta: i64 },
    Read,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponsePayload {
    AddOk,
    ReadOk { value: i64 },
}

struct CounterHandler {
    node: Node,
    bucket: String,
    client: SeqKvClient,
}

impl Handler<RequestPayload> for CounterHandler {
    async fn handle(
        &self,
        counter_msg: &MaelstromMessage<RequestPayload>,
    ) -> Result<(), MaelstromError> {
        match counter_msg.body.payload {
            RequestPayload::Add { delta } => {
                let current_value = self.client.read_int(&self.bucket).await.unwrap_or(0);
                let new_value = (current_value + delta).to_string();
                self.client.write(&self.bucket, &new_value).await?;
                self.node.reply(counter_msg, ResponsePayload::AddOk);
            }
            RequestPayload::Read => {
                let mut value = match self.client.read_int(&self.bucket).await {
                    Ok(v) => v,
                    Err(GlomerError::Maelstrom(MaelstromError {
                        code: error_type::KEY_DOES_NOT_EXIST,
                        ..
                    })) => 0,
                    Err(e) => return Err(e.into()),
                };

                if counter_msg.src.starts_with("c") {
                    for node in self.node.node_ids.iter().filter(|id| **id != self.bucket) {
                        // Eventual consistency, if we don't immediately get a response, continue
                        let res = self
                            .node
                            .send_rpc(node, RequestPayload::Read, Some(Duration::from_millis(10)))
                            .await;
                        value += match res {
                            Ok(ResponsePayload::ReadOk { value }) => value,
                            Ok(_) => {
                                // TODO we are returning not_supported to the maelstrom client,
                                //not the node that sent us the invalid response!
                                return Err(MaelstromError::not_supported(
                                    "invalid response to read request",
                                )
                                .into());
                            }
                            Err(GlomerError::Timeout) => 0,
                            Err(e) => return Err(e.into()),
                        }
                    }
                }

                self.node
                    .reply(counter_msg, ResponsePayload::ReadOk { value });
            }
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let node = Node::init()?;
    let bucket = format!("n{}", node.id);
    // TODO reduce node cloning?
    let handler = CounterHandler {
        node: node.clone(),
        bucket,
        client: SeqKvClient::new(node.clone()),
    };
    Ok(node.run(handler).await?)
}
