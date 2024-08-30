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
    error::{error_type, GlomerError, MaelstromError},
    message::{Body, MaelstromMessage},
};

#[allow(clippy::module_name_repetitions)]
#[must_use]
pub fn node_id(id: u32) -> String {
    format!("n{id}")
}

pub fn parse_node_id(id: &str) -> Result<u32, GlomerError> {
    id.strip_prefix("n")
        .ok_or_else(|| GlomerError::Parse("Invalid node id".into()))?
        .parse()
        .map_err(|_| GlomerError::Parse("Invalid node id".into()))
}

// Handler trait - user needs to impl these methods to handle messages
pub trait Handler<P> {
    // Handle a message - should return () on success otherwise Err(MaelstromError)
    fn handle(
        &self,
        msg: &MaelstromMessage<P>,
    ) -> impl Future<Output = Result<(), MaelstromError>> + Send
    where
        P: DeserializeOwned;
}

// Init messages - internally used to initialize the node
#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "type", rename = "init")]
struct Init {
    node_id: String,
    #[allow(unused)]
    node_ids: Vec<String>,
}

#[derive(Serialize, Clone, Debug)]
#[serde(tag = "type", rename = "init_ok")]
struct InitOk {}

#[derive(Debug, Clone)]
pub struct Node {
    // Out NodeId
    pub id: u32,
    // Monotonically increasing message id
    pub next_msg_id: Arc<AtomicU64>,
    pub cancellation_token: CancellationToken,
    // Mapping from msg_id to channel on which to send response
    pub(super) response_map: Arc<Mutex<BTreeMap<u64, oneshot::Sender<String>>>>,
}

impl Node {
    pub fn init() -> Result<Self, GlomerError> {
        let mut buffer = String::new();
        stdin().read_line(&mut buffer)?;
        let init_msg: MaelstromMessage<Init> =
            serde_json::from_str::<MaelstromMessage<Init>>(&buffer)?;
        let node = Self {
            id: parse_node_id(&init_msg.body.payload.node_id)?,
            next_msg_id: Arc::new(0.into()),
            cancellation_token: CancellationToken::new(),
            response_map: Arc::new(Mutex::new(BTreeMap::new())),
        };

        // Let maelstrom know that we are initialized
        node.reply(&init_msg, InitOk {});

        Ok(node)
    }

    // Main process loop - initializes node then reads messages from stdin in a loop
    // Will automatically respond to requests with formatted error on handle() error
    pub async fn run<P, H>(&self, handler: H) -> Result<(), GlomerError>
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

            // Spawn new task to handle input so we can keep processing more messages
            let handler = handler.clone();
            let node = self.clone();
            tracker.spawn(async move {
                // TODO I don't love this, is there a better way?
                let in_reply_to: Option<u64> = serde_json::from_str::<Value>(&line)
                    .unwrap()
                    .get("body")
                    .unwrap()
                    .get("in_reply_to")
                    .and_then(|v| serde_json::from_value(v.clone()).ok());

                // If the received message is in response to an existing message,
                // send the response to whichever task is waiting for it
                if let Some(in_reply_to) = in_reply_to {
                    let mut guard = node.response_map.lock().unwrap();
                    if let Some(tx) = guard.remove(&in_reply_to) {
                        if let Err(request_msg) = tx.send(line) {
                            eprintln!(
                                "INFO: Received response after operation timeout: {request_msg:?}"
                            );
                        }
                    }
                } else {
                    // TODO custom deserialization to proper error
                    // The problem with this is that if we fail to parse the message,
                    // we don't know who to respond to with an error!
                    let request_msg = serde_json::from_str::<MaelstromMessage<P>>(&line).unwrap();

                    let res = tokio::select! {
                        res = handler.handle(&request_msg) => res,
                        () = node.cancellation_token.cancelled() => Ok(()),
                    };

                    // Serialize and send error message from handler
                    if let Err(err) = res {
                        let error_type = err.code;
                        node.fire_and_forget(None, request_msg.body.msg_id, request_msg.src, &err);

                        match error_type {
                            error_type::CRASH | error_type::ABORT => {
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

    fn fire_and_forget<P>(
        &self,
        msg_id: Option<u64>,
        in_reply_to: Option<u64>,
        dest: String,
        payload: &P,
    ) where
        P: Serialize,
    {
        let msg = MaelstromMessage {
            src: node_id(self.id),
            dest,
            body: Body {
                msg_id,
                in_reply_to,
                payload,
            },
        };
        let msg = serde_json::to_string(&msg).unwrap();
        println!("{msg}");
    }

    pub fn reply<P, R>(&self, source_msg: &MaelstromMessage<P>, payload: R)
    where
        R: Serialize,
    {
        self.fire_and_forget(
            None,
            source_msg.body.msg_id,
            source_msg.src.to_string(),
            &payload,
        );
    }

    pub fn send<P>(&self, dest: &str, payload: P)
    where
        P: Serialize + Debug,
    {
        // Don't include a msg_id because we aren't expecting a response
        self.fire_and_forget(None, None, dest.to_string(), &payload);
    }

    pub async fn send_rpc<P, R>(
        &self,
        dest: &str,
        payload: P,
        timeout_duration: Option<Duration>,
    ) -> Result<R, GlomerError>
    where
        P: Serialize + Debug + Send,
        R: DeserializeOwned + Debug,
    {
        let msg_id = self.next_msg_id.fetch_add(1, Ordering::Relaxed);
        self.fire_and_forget(Some(msg_id), None, dest.to_string(), &payload);
        // Set up channel to receive respone
        let (tx, rx) = oneshot::channel();
        // Store sender on map with msg_id
        self.response_map.lock().unwrap().insert(msg_id, tx);

        if let Some(timeout_duration) = timeout_duration {
            tokio::select! {
                () = self.cancellation_token.cancelled() => {
                    Err(GlomerError::Abort("Node shut down.".into()))
                }
                res = timeout(timeout_duration, rx) => {
                    match res {
                        Err(_) => {
                            self.response_map.lock().unwrap().remove(&msg_id);
                            Err(GlomerError::Timeout)
                        }
                        Ok(response) => {
                            let untagged = serde_json::from_str::<UntaggedRpcMessage<R>>(&response.unwrap())?;
                            match untagged.body.payload {
                                UntaggedResult::Ok(payload) => Ok(payload),
                                UntaggedResult::Err(err) => Err(GlomerError::Maelstrom(err)),
                            }
                        }
                    }
                }
            }
        } else {
            let untagged = serde_json::from_str::<UntaggedRpcMessage<R>>(&rx.await.unwrap())?;
            match untagged.body.payload {
                UntaggedResult::Ok(payload) => Ok(payload),
                UntaggedResult::Err(err) => Err(GlomerError::Maelstrom(err)),
            }
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(untagged)]
enum UntaggedResult<P> {
    Ok(P),
    Err(MaelstromError),
}

// Intermediate type for serialization/deserialization
// Since using Result<P, MaelstromError> results in a tagged json object,
// we need to use an untagged version of Result and then convert.
#[derive(Serialize, Deserialize, Clone, Debug)]
struct UntaggedRpcMessage<P> {
    src: String,
    dest: String,
    body: Body<UntaggedResult<P>>,
}
