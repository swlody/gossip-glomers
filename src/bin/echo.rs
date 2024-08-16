use gossip_glomers::{error::Error, MaelstromMessage};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename = "echo")]
struct Echo {
    echo: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename = "echo_ok")]
struct EchoOk {
    echo: String,
}

fn echo_handler(request_msg: MaelstromMessage<Echo>) -> Result<MaelstromMessage<EchoOk>, Error> {
    let echo = request_msg.payload().echo.clone();
    Ok(request_msg.reply_with_payload(EchoOk { echo }))
}

fn main() {
    gossip_glomers::run(echo_handler);
}
