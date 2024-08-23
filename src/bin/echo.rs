use gossip_glomers::{error::MaelstromError, Handler, MaelstromMessage, Node};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RequestPayload {
    Echo { echo: String },
}

#[derive(Serialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponsePayload {
    EchoOk { echo: String },
}

#[derive(Clone)]
struct EchoHandler {
    node: Node,
}

impl Handler<RequestPayload> for EchoHandler {
    fn init(node: Node) -> Self {
        Self { node }
    }

    async fn handle(
        &self,
        echo_msg: MaelstromMessage<RequestPayload>,
    ) -> Result<(), MaelstromError> {
        match &echo_msg.body.payload {
            RequestPayload::Echo { echo } => {
                self.node.reply(&echo_msg, ResponsePayload::EchoOk { echo: echo.to_string() })?;
            }
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    gossip_glomers::run::<RequestPayload, EchoHandler>().await
}
