use gossip_glomers::{error::Error, MaelstromMessage, Node};
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

fn handler(echo_msg: &MaelstromMessage<Echo>, node: &mut Node<()>) -> Result<(), Error> {
    let echo = echo_msg.payload().echo.clone();
    node.reply(echo_msg, EchoOk { echo });
    Ok(())
}

fn main() {
    gossip_glomers::run::<_, _, EchoOk>(handler, ());
}
