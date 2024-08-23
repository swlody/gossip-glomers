use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use serde::{Deserialize, Serialize};
use tokio::{
    sync::oneshot,
    time::{timeout, Duration},
};
use tokio_util::sync::CancellationToken;

use crate::{
    error::MaelstromError,
    message::{Body, MaelstromMessage, NodeId},
};

#[derive(Debug)]
pub struct Node<P> {
    pub id: NodeId,
    pub network_ids: Vec<NodeId>,
    pub next_msg_id: Arc<AtomicU64>,
    pub cancellation_token: CancellationToken,
    pub(super) response_map: Mutex<BTreeMap<u64, oneshot::Sender<MaelstromMessage<P>>>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct GenericDestinationMaelstromMessage<P> {
    src: NodeId,
    dest: String,
    body: Body<P>,
}

impl<P> Node<P> {
    pub(super) fn send_impl<R>(
        &self,
        in_reply_to: Option<u64>,
        dest: String,
        payload: &R,
    ) -> Result<u64, MaelstromError>
    where
        R: Serialize,
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

    pub fn reply(&self, source_msg: &MaelstromMessage<P>, payload: P) -> Result<(), MaelstromError>
    where
        P: Serialize,
    {
        self.send_impl(Some(source_msg.body.msg_id), source_msg.src.to_string(), &payload)?;
        Ok(())
    }

    pub(super) async fn send_generic_dest(
        &self,
        dest: String,
        payload: P,
        timeout_duration: Option<Duration>,
    ) -> Result<MaelstromMessage<P>, MaelstromError>
    where
        P: Serialize,
    {
        let msg_id = self.send_impl(None, dest.to_string(), &payload)?;
        let (tx, rx) = oneshot::channel();
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
                        Ok(response) => Ok(response.unwrap()),
                    }
                }
            }
        } else {
            Ok(rx.await.unwrap())
        }
    }

    pub async fn send(
        &self,
        dest: NodeId,
        payload: P,
        timeout: Option<Duration>,
    ) -> Result<MaelstromMessage<P>, MaelstromError>
    where
        P: Serialize,
    {
        self.send_generic_dest(dest.to_string(), payload, timeout).await
    }
}
