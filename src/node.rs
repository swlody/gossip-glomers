use std::{
    collections::BTreeMap,
    fmt::Debug,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::{
    sync::oneshot,
    time::{timeout, Duration},
};
use tokio_util::sync::CancellationToken;

use crate::{
    error::MaelstromError,
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

impl Node {
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
        let response = GenericDestinationMaelstromMessage {
            src: self.id,
            dest,
            body: Body { msg_id, in_reply_to, payload },
        };
        let response = serde_json::to_string(&response).unwrap();
        println!("{response}");
        Ok(msg_id)
    }

    pub fn reply<P>(
        &self,
        source_msg: &MaelstromMessage<P>,
        payload: P,
    ) -> Result<(), MaelstromError>
    where
        P: Serialize,
    {
        self.send_impl(Some(source_msg.body.msg_id), source_msg.src.to_string(), &payload)?;
        Ok(())
    }

    // TODO go back to separate request/response types?
    pub(super) async fn send_generic_dest<P>(
        &self,
        dest: String,
        payload: P,
        timeout_duration: Option<Duration>,
    ) -> Result<MaelstromMessage<P>, MaelstromError>
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static,
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
                        Ok(response) => Ok(serde_json::from_str::<MaelstromMessage<P>>(&response.unwrap()).unwrap()),
                    }
                }
            }
        } else {
            Ok(serde_json::from_str::<MaelstromMessage<P>>(&rx.await.unwrap()).unwrap())
        }
    }

    pub async fn send<P>(
        &self,
        dest: NodeId,
        payload: P,
        timeout: Option<Duration>,
    ) -> Result<MaelstromMessage<P>, MaelstromError>
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        self.send_generic_dest(dest.to_string(), payload, timeout).await
    }
}
