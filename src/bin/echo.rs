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

fn handler(echo_message: &MaelstromMessage<Echo>, _ctx: &mut Node<()>) -> Result<EchoOk, Error> {
    let echo = echo_message.payload().echo.clone();
    Ok(EchoOk { echo })
}

fn main() {
    gossip_glomers::run(handler, ());
}
