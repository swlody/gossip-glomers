use gossip_glomers::{
    error::{GlomerError, MaelstromError},
    Handler, MaelstromMessage, Node,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "type", rename = "echo")]
struct Echo {
    echo: String,
}

#[derive(Serialize, Clone, Debug)]
#[serde(tag = "type", rename = "echo_ok")]
struct EchoOk<'a> {
    echo: &'a str,
}
struct EchoHandler {
    node: Node,
}

impl Handler<Echo> for EchoHandler {
    fn init(node: Node) -> Self {
        Self { node }
    }

    fn handle(&self, echo_msg: MaelstromMessage<Echo>) -> Result<(), MaelstromError> {
        self.node.reply(
            &echo_msg,
            EchoOk {
                echo: &echo_msg.body.payload.echo,
            },
        )?;
        Ok(())
    }
}

fn main() -> Result<(), GlomerError> {
    gossip_glomers::run::<Echo, EchoHandler>()
}
