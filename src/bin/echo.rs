use gossip_glomers::{error::Error, GuidNodeId};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Echo {
    echo: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct EchoOk {
    echo: String,
}

fn echo_handler(echo_payload: &Echo, _: GuidNodeId) -> Result<EchoOk, Error> {
    let echo = echo_payload.echo.clone();
    Ok(EchoOk { echo })
}

fn main() {
    gossip_glomers::run(echo_handler);
}
