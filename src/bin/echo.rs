use gossip_glomers::MaelstromMessage;
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

fn cb(line: String) -> anyhow::Result<String> {
    let echo_msg = serde_json::from_str::<MaelstromMessage<Echo>>(&line)?;
    let echo = echo_msg.payload().echo.clone();

    let echo_response = echo_msg.reply_with_payload(EchoOk { echo });

    Ok(serde_json::to_string(&echo_response)?)
}

fn main() -> anyhow::Result<()> {
    gossip_glomers::run(cb)
}
