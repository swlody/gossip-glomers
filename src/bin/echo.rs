use std::sync::Arc;

use gossip_glomers::{error::MaelstromError, Handler, MaelstromMessage, Node};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}

struct EchoHandler {
    node: Arc<Node>,
}

impl Handler<Payload> for EchoHandler {
    fn init(node: Arc<Node>) -> Self {
        Self { node }
    }

    async fn handle(&self, echo_msg: MaelstromMessage<Payload>) -> Result<(), MaelstromError> {
        match &echo_msg.body.payload {
            Payload::Echo { echo } => {
                self.node.reply(&echo_msg, Payload::EchoOk { echo: echo.to_string() })?;
            }
            Payload::EchoOk { .. } => {
                return Err(MaelstromError::not_supported("Invalid message type"));
            }
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    gossip_glomers::run::<Payload, EchoHandler>().await
}
