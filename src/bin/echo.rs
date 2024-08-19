use gossip_glomers::{
    error::{GlomerError, MaelstromError},
    MaelstromMessage, Node,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "type", rename = "echo")]
struct Echo {
    echo: String,
}

#[derive(Serialize, Clone, Debug)]
#[serde(tag = "type", rename = "echo_ok")]
struct EchoOk {
    echo: String,
}

fn handler(
    echo_msg: MaelstromMessage<Echo>,
    node: &Node,
    _: &mut (),
) -> Result<(), MaelstromError> {
    let echo = echo_msg.payload().echo.clone();
    node.reply(echo_msg, EchoOk { echo })?;
    Ok(())
}

fn main() -> Result<(), GlomerError> {
    gossip_glomers::run(handler, ())
}
