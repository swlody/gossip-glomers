use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
};

use serde::{Deserialize, Serialize};
use tokio::{
    sync::oneshot,
    time::{interval, Duration},
};

use crate::{
    error::MaelstromError,
    message::{Body, MaelstromMessage, NodeId},
};

#[derive(Debug)]
pub struct Node<P> {
    pub id: NodeId,
    pub network_ids: Vec<NodeId>,
    pub next_msg_id: AtomicU64,
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

    pub(super) async fn send_generic_dest<R>(
        &self,
        dest: String,
        payload: R,
    ) -> Result<MaelstromMessage<P>, MaelstromError>
    where
        R: Serialize,
    {
        let mut msg_id = self.send_impl(None, dest.to_string(), &payload)?;
        let (mut tx, mut rx) = oneshot::channel();
        self.response_map.lock().unwrap().insert(msg_id, tx);
        let mut retry_interval = interval(Duration::from_millis(100));
        // How many times to tick before retrying
        let mut retry_tick_count = 0;
        let mut ticks_since_last_retry = 0;

        loop {
            tokio::select! {
                _ = retry_interval.tick() => {
                    if ticks_since_last_retry == retry_tick_count {
                        // Backoff, require one extra second for each retry
                        retry_tick_count += 1;
                        ticks_since_last_retry = 0;
                        let mut guard = self.response_map.lock().unwrap();
                        guard.remove(&msg_id);
                        msg_id = self.send_impl(None, dest.to_string(), &payload)?;
                        (tx, rx) = oneshot::channel();
                        guard.insert(msg_id, tx);
                    } else {
                        ticks_since_last_retry += 1;
                    }
                }
                response = &mut rx => {
                    return Ok(response.unwrap());
                }
            }
        }
    }

    pub async fn send(
        &self,
        dest: NodeId,
        payload: P,
    ) -> Result<MaelstromMessage<P>, MaelstromError>
    where
        P: Serialize,
    {
        self.send_generic_dest(dest.to_string(), payload).await
    }
}
