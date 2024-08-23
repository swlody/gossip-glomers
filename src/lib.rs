#![feature(box_into_inner)]

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
use serde_json::Value;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::error::MaelstromError;

// Init messages - internally used to initialize the node
#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "type", rename = "init")]
struct Init {
    node_id: NodeId,
    node_ids: Vec<NodeId>,
}

#[derive(Serialize, Clone, Debug)]
#[serde(tag = "type", rename = "init_ok")]
struct InitOk {}

// Handler trait - user needs to impl these methods to handle messages
pub trait Handler<P> {
    // Handle a message - should return () on success otherwise Err(MaelstromError)
    fn handle(
        &self,
        msg: MaelstromMessage<P>,
    ) -> impl Future<Output = Result<(), MaelstromError>> + Send
    where
        P: DeserializeOwned;
}

pub async fn init() -> eyre::Result<Node> {
    let mut buffer = String::new();
    stdin().read_line(&mut buffer)?;
    let init_msg: MaelstromMessage<Init> = serde_json::from_str::<MaelstromMessage<Init>>(&buffer)?;
    let node = Node {
        id: init_msg.body.payload.node_id,
        network_ids: Arc::new(init_msg.body.payload.node_ids),
        next_msg_id: Arc::new(0.into()),
        cancellation_token: CancellationToken::new(),
        response_map: Arc::new(Mutex::new(BTreeMap::new())),
    };

    // Let maelstrom know that we are initialized
    node.send_impl(Some(init_msg.body.msg_id), init_msg.src.to_string(), &InitOk {})?;

    Ok(node)
}

// Main process loop - initializes node then reads messages from stdin in a loop
// Will automatically respond to requests with formatted error on handle() error
pub async fn run<P, H>(node: &Node, handler: H) -> eyre::Result<()>
where
    P: DeserializeOwned + Debug + Send + Sync + 'static,
    H: Handler<P> + Send + Sync + 'static,
{
    // Initialization

    let tracker = TaskTracker::new();
    // Initialize the user's handler, store in Arc to clone for each request
    let handler = Arc::new(handler);
    for line in stdin().lock().lines() {
        let line = line?;
        // Deserialize message from input

        // TODO I don't love this, is there a better way?
        let in_reply_to: Option<u64> = serde_json::from_str::<Value>(&line)
            .unwrap()
            .get("body")
            .unwrap()
            .get("in_reply_to")
            .and_then(|v| serde_json::from_value(v.clone()).ok());

        // Spawn new task to handle input so we can keep processing more messages
        let handler = handler.clone();
        let node = node.clone();
        tracker.spawn(async move {
            // If the received message is in response to an existing message,
            // send the response to whichever task is waiting for it
            if let Some(in_reply_to) = in_reply_to {
                if let Some(tx) = node.response_map.lock().unwrap().remove(&in_reply_to) {
                    if let Err(request_msg) = tx.send(line) {
                        eprintln!(
                            "INFO: Received response after operation timeout: {:?}",
                            request_msg
                        );
                    }
                }
            } else {
                // TODO custom deserialization to proper error
                // The problem with this is that if we fail to parse the message,
                // we don't know who to respond to with an error!
                let request_msg = serde_json::from_str::<MaelstromMessage<P>>(&line).unwrap();

                // Copy these for error handling
                let msg_id = request_msg.body.msg_id;
                let src = request_msg.src;

                let res = tokio::select! {
                    res = handler.handle(request_msg) => res,
                    _ = node.cancellation_token.cancelled() => Ok(()),
                };

                // Serialize and send error message from handler
                if let Err(err) = res {
                    let error_type = err.error_type;
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

    // Graceful shutdown, wait for outstanding tasks to finish
    node.cancellation_token.cancel();
    tracker.close();
    tracker.wait().await;

    Ok(())
}
