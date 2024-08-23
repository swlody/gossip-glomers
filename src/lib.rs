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
    // Allow user to store node in their state to respond to/send messages
    fn init(node: Arc<Node>) -> Self;

    // Handle a message - should return () on success otherwise Err(MaelstromError)
    fn handle(
        &self,
        msg: MaelstromMessage<P>,
    ) -> impl Future<Output = Result<(), MaelstromError>> + Send
    where
        P: DeserializeOwned;
}

// Main process loop - initializes node then reads messages from stdin in a loop
// Will automatically respond to requests with formatted error on handle() error
pub async fn run<P, H>() -> eyre::Result<()>
where
    P: DeserializeOwned + Debug + Send + Sync + 'static,
    H: Handler<P> + Send + Sync + 'static,
{
    // Initialization
    let mut buffer = String::new();
    stdin().read_line(&mut buffer)?;
    let init_msg: MaelstromMessage<Init> = serde_json::from_str::<MaelstromMessage<Init>>(&buffer)?;
    let node = Arc::new(Node {
        id: init_msg.body.payload.node_id,
        network_ids: init_msg.body.payload.node_ids,
        next_msg_id: Arc::new(0.into()),
        cancellation_token: CancellationToken::new(),
        response_map: Mutex::new(BTreeMap::new()),
    });

    // Let maelstrom know that we are initialized
    node.send_impl(Some(init_msg.body.msg_id), init_msg.src.to_string(), &InitOk {})?;

    let tracker = TaskTracker::new();
    // Initialize the user's handler, store in Arc to clone for each request
    let handler = Arc::new(H::init(node.clone()));
    for line in stdin().lock().lines() {
        let line = line?;
        // Deserialize message from input

        // TODO custom deserialization to proper error
        // The problem with this is that if we fail to parse the message,
        // we don't know who to respond to with an error!
        let request_msg = serde_json::from_str::<MaelstromMessage<P>>(&line).unwrap();

        // Copy these for error handling
        let msg_id = request_msg.body.msg_id;
        let src = request_msg.src;

        // Spawn new task to handle input so we can keep processing more messages
        let handler = handler.clone();
        let node = node.clone();
        tracker.spawn(async move {
            // If the received message is in response to an existing message,
            // send the response to whichever task is waiting for it
            if let Some(in_reply_to) = request_msg.body.in_reply_to {
                if let Some(tx) = node.response_map.lock().unwrap().remove(&in_reply_to) {
                    if let Err(request_msg) = tx.send(Box::new(request_msg)) {
                        eprintln!(
                            "INFO: Received response after operation timeout: {:?}",
                            request_msg
                        );
                    }
                }
            } else {
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
