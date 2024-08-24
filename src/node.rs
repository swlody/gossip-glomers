use std::{
    collections::BTreeMap,
    fmt::Debug,
    future::Future,
    io::{stdin, BufRead as _},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    sync::oneshot,
    time::{timeout, Duration},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    error::{MaelstromError, MaelstromErrorType},
    message::{Body, MaelstromMessage, NodeId},
};

#[derive(Debug, Clone)]
pub struct Node {
    // Out NodeId
    pub id: NodeId,
    // Other nodes in the network
    pub network_ids: Arc<Vec<NodeId>>,
    // Monotonically increasing message id
    pub next_msg_id: Arc<AtomicU64>,
    pub cancellation_token: CancellationToken,
    // Mapping from msg_id to channel on which to send response
    pub(super) response_map: Arc<Mutex<BTreeMap<u64, oneshot::Sender<String>>>>,
}

// Same as MaelstromMessage but String for dest instead of NodeId.
// For interacting wtih Maelstrom services.
#[derive(Serialize, Deserialize, Clone, Debug)]
struct GenericDestinationMaelstromMessage<P> {
    src: NodeId,
    dest: String,
    body: Body<P>,
}

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

impl Node {
    pub async fn init() -> eyre::Result<Self> {
        let mut buffer = String::new();
        stdin().read_line(&mut buffer)?;
        let init_msg: MaelstromMessage<Init> =
            serde_json::from_str::<MaelstromMessage<Init>>(&buffer)?;
        let node = Self {
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
    pub async fn run<P, H>(&self, handler: H) -> eyre::Result<()>
    where
        P: DeserializeOwned + Debug + Send + Sync + 'static,
        H: Handler<P> + Send + Sync + 'static,
    {
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
            let node = self.clone();
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
        self.cancellation_token.cancel();
        tracker.close();
        tracker.wait().await;

        Ok(())
    }

    pub(super) fn send_impl<P>(
        &self,
        in_reply_to: Option<u64>,
        dest: String,
        payload: &P,
    ) -> Result<u64, MaelstromError>
    where
        P: Serialize,
    {
        let msg_id = self.next_msg_id.fetch_add(1, Ordering::Relaxed);
        let msg = GenericDestinationMaelstromMessage {
            src: self.id,
            dest,
            body: Body { msg_id, in_reply_to, payload },
        };
        let msg = serde_json::to_string(&msg).unwrap();
        eprintln!("-> {msg}");
        println!("{msg}");
        Ok(msg_id)
    }

    pub fn reply<P, R>(
        &self,
        source_msg: &MaelstromMessage<P>,
        payload: R,
    ) -> Result<(), MaelstromError>
    where
        R: Serialize,
    {
        self.send_impl(Some(source_msg.body.msg_id), source_msg.src.to_string(), &payload)?;
        Ok(())
    }

    pub(super) async fn send_generic_dest<P, R>(
        &self,
        dest: String,
        payload: P,
        timeout_duration: Option<Duration>,
    ) -> Result<MaelstromMessage<R>, MaelstromError>
    where
        P: Serialize,
        R: DeserializeOwned,
    {
        let msg_id = self.send_impl(None, dest.to_string(), &payload)?;
        // Set up channel to receive respone
        let (tx, rx) = oneshot::channel();
        // Store sender on map with msg_id
        self.response_map.lock().unwrap().insert(msg_id, tx);

        if let Some(timeout_duration) = timeout_duration {
            tokio::select! {
                _ = self.cancellation_token.cancelled() => {
                    Err(MaelstromError::node_not_found("Node shut down."))
                }
                res = timeout(timeout_duration, rx) => {
                    match res {
                        Err(_) => {
                            self.response_map.lock().unwrap().remove(&msg_id);
                            Err(MaelstromError::timeout("Timed out waiting for response"))
                        }
                        Ok(response) => {
                            let response = response.unwrap();
                            eprintln!("<- {response}");
                            Ok(serde_json::from_str::<MaelstromMessage<R>>(&response).unwrap())
                        }
                    }
                }
            }
        } else {
            Ok(serde_json::from_str::<MaelstromMessage<R>>(&rx.await.unwrap()).unwrap())
        }
    }

    pub async fn send<P, R>(
        &self,
        dest: NodeId,
        payload: P,
        timeout: Option<Duration>,
    ) -> Result<MaelstromMessage<R>, MaelstromError>
    where
        P: Serialize,
        R: DeserializeOwned,
    {
        self.send_generic_dest(dest.to_string(), payload, timeout).await
    }
}
