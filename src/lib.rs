pub mod error;
pub mod message;
pub mod node;
pub mod seq_kv_client;

use std::{
    collections::BTreeMap,
    fmt::Debug,
    future::Future,
    io::{stdin, BufRead as _},
    sync::{Arc, Mutex},
};

use error::MaelstromErrorType;
pub use message::{MaelstromMessage, NodeId};
pub use node::Node;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::signal;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::error::MaelstromError;

#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "type", rename = "init")]
struct Init {
    node_id: NodeId,
    node_ids: Vec<NodeId>,
}

#[derive(Serialize, Clone, Debug)]
#[serde(tag = "type", rename = "init_ok")]
struct InitOk {}

pub trait Handler<P> {
    fn init(node: Arc<Node<P>>) -> Self;

    fn handle(
        &self,
        msg: MaelstromMessage<P>,
    ) -> impl Future<Output = Result<(), MaelstromError>> + Send
    where
        P: DeserializeOwned;
}

pub async fn run<P, H>() -> eyre::Result<()>
where
    P: DeserializeOwned + Debug + Send + 'static,
    H: Handler<P> + Send + Sync + 'static,
{
    let mut buffer = String::new();
    stdin().read_line(&mut buffer)?;
    let init_msg: MaelstromMessage<Init> = serde_json::from_str::<MaelstromMessage<Init>>(&buffer)?;

    let node = Arc::new(Node {
        id: init_msg.body.payload.node_id,
        network_ids: init_msg.body.payload.node_ids,
        next_msg_id: Arc::new(0.into()),
        response_map: Mutex::new(BTreeMap::new()),
    });

    node.send_impl(Some(init_msg.body.msg_id), init_msg.src.to_string(), &InitOk {})?;

    let token = CancellationToken::new();
    let cloned_token = token.clone();
    let tracker = TaskTracker::new();
    tracker.spawn(async move {
        tokio::select! {
            _ = signal::ctrl_c() => { cloned_token.cancel() }
            _ = cloned_token.cancelled() => {}
        }
    });

    let cloned_token = token.clone();
    let handler = Arc::new(H::init(node.clone()));
    for line in stdin().lock().lines() {
        if cloned_token.is_cancelled() {
            break;
        }

        let line = line?;
        // TODO custom deserialization to proper error
        // The problem with this is that if we fail to parse the message,
        // we don't know who to respond to with an error!
        let request_msg = serde_json::from_str::<MaelstromMessage<P>>(&line).unwrap();
        let msg_id = request_msg.body.msg_id;
        let src = request_msg.src;

        let handler = handler.clone();
        let node = node.clone();
        let cloned_token = token.clone();
        tracker.spawn(async move {
            if let Some(in_reply_to) = request_msg.body.in_reply_to {
                if let Some(tx) = node.response_map.lock().unwrap().remove(&in_reply_to) {
                    tx.send(request_msg).unwrap();
                }
            } else {
                let res = tokio::select! {
                    res = handler.handle(request_msg) => res,
                    _ = cloned_token.cancelled() => Ok(()),
                };

                if let Err(err) = res {
                    let error_type = err.error_type;
                    // let error_string: String = err.to_string();
                    node.send_impl(Some(msg_id), src.to_string(), &err).unwrap();

                    match error_type {
                        MaelstromErrorType::Crash | MaelstromErrorType::Abort => {
                            panic!("Unrecoverable error: {}", err.text)
                        }
                        _ => {}
                    }
                }
            }
        });
    }

    token.cancel();
    tracker.close();
    tracker.wait().await;

    Ok(())
}
