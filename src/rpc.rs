use crate::protocol::Message;
use crate::Result;
use crate::Runtime;
use tokio::sync::oneshot::Receiver;
use tokio::sync::{oneshot, OnceCell};

pub struct RpcResult {
    runtime: Runtime,
    rx: OnceCell<Receiver<Message>>,
    msg_id: u64,
}

impl RpcResult {
    #[must_use]
    pub fn new(msg_id: u64, rx: Receiver<Message>, runtime: Runtime) -> RpcResult {
        RpcResult {
            runtime,
            rx: OnceCell::new_with(Some(rx)),
            msg_id,
        }
    }
}

impl Drop for RpcResult {
    fn drop(&mut self) {
        drop(self.rx.take());
        drop(self.runtime.remove_pending_reply(self.msg_id));
    }
}

pub(crate) async fn rpc(runtime: Runtime, msg_id: u64, req: Result<String>) -> Result<RpcResult> {
    let (tx, rx) = oneshot::channel::<Message>();

    let _ = runtime.insert_pending_reply(msg_id, tx).await;
    if let Err(err) = runtime.stdout(req?.as_str()).await {
        let _ = runtime.remove_pending_reply(msg_id).await;
        return Err(err);
    }
    Ok(RpcResult::new(msg_id, rx, runtime))
}
