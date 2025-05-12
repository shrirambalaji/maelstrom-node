#![allow(dead_code)]

use crate::error::Error;
use crate::executor::{Executor, RuntimeExecutor};
use crate::protocol::{ErrorMessageBody, InitMessageBody, Message};
use crate::{rpc_err_to_response, RPCResult};
use async_trait::async_trait;
use futures::FutureExt;
use log::{debug, error, info, warn};
use serde::Serialize;
use serde_json::Value;
use simple_error::bail;
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{AcqRel, Release};
use std::sync::Arc;
use tokio::io::{stdin, stdout, AsyncBufReadExt, AsyncWriteExt, BufReader, Stdout};
use tokio::select;
use tokio::sync::oneshot::Sender;
use tokio::sync::{mpsc, Mutex, OnceCell};
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub struct Runtime {
    context: Arc<Context>,
    executor: RuntimeExecutor,
}

struct Context {
    msg_id: AtomicU64,
    membership: OnceCell<MembershipState>,
    node: OnceCell<Arc<dyn Node>>,
    pending_replies: Mutex<HashMap<u64, Sender<Message>>>,
    stdout: Mutex<Stdout>,
}

#[async_trait]
pub trait Node: Sync + Send {
    async fn process(&self, runtime: Runtime, request: Message) -> Result<()>;
}

#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct MembershipState {
    pub node_id: String,
    pub nodes: Vec<String>,
}

impl Runtime {
    pub fn init<F: Future>(future: F) -> F::Output {
        let tokio_runtime = tokio::runtime::Runtime::new().unwrap();
        let _guard: tokio::runtime::EnterGuard<'_> = tokio_runtime.enter();
        tokio_runtime.block_on(future)
    }
}

impl Runtime {
    pub fn new() -> Self {
        Runtime::default()
    }

    pub fn with_node(self, node: Arc<dyn Node + Send + Sync>) -> Self {
        assert!(
            self.context.node.set(node).is_ok(),
            "runtime node is already initialized"
        );
        self
    }

    pub async fn send_raw(&self, msg: &str) -> Result<()> {
        {
            let mut out = self.context.stdout.lock().await;
            out.write_all(msg.as_bytes()).await?;
            out.write_all(b"\n").await?;
        }
        info!("Sent {msg}");
        Ok(())
    }

    pub async fn send(&self, to: impl Into<String>, message: impl Serialize) -> Result<()> {
        let msg = crate::protocol::message(self.node_id(), to, message)?;
        let ans = serde_json::to_string(&msg)?;
        self.send_raw(ans.as_str()).await
    }

    pub async fn reply(&self, req: Message, resp: impl Serialize) -> Result<()> {
        let mut msg = crate::protocol::message(self.node_id(), req.src, resp)?;
        msg.body.in_reply_to = req.body.msg_id;

        if !msg.body.extra.contains_key("type") && !req.body.typ.is_empty() {
            let key = "type".to_string();
            let value = Value::String(req.body.typ + "_ok");
            msg.body.extra.insert(key, value);
        }

        let answer = serde_json::to_string(&msg)?;
        self.send_raw(answer.as_str()).await
    }

    pub async fn reply_ok(&self, req: Message) -> Result<()> {
        self.reply(req, Runtime::empty_response()).await
    }

    /// `rpc()` makes a remote call to another node via message passing interface.
    /// `RPCResult` is immediately canceled on drop.
    pub fn rpc(
        &self,
        to: impl Into<String>,
        request: impl Serialize,
    ) -> impl Future<Output = Result<RPCResult>> {
        let msg = crate::protocol::message(self.node_id(), to, request);
        let req_msg_id = self.next_msg_id();
        let req_res: Result<String> = match msg {
            Ok(mut t) => {
                t.body.msg_id = req_msg_id;
                match serde_json::to_string(&t) {
                    Ok(s) => Ok(s),
                    Err(e) => Err(Box::new(e)),
                }
            }
            Err(e) => Err(e),
        };

        crate::rpc(self.clone(), req_msg_id, req_res)
    }

    pub fn execute_rpc(&self, to: impl Into<String>, request: impl Serialize + 'static) {
        self.executor.spawn(self.rpc(to.into(), request));
    }

    pub fn node_id(&self) -> &str {
        if let Some(v) = self.context.membership.get() {
            return v.node_id.as_str();
        }
        ""
    }

    pub fn nodes(&self) -> &[String] {
        if let Some(v) = self.context.membership.get() {
            return v.nodes.as_slice();
        }
        &[]
    }

    pub fn set_membership_state(&self, state: MembershipState) -> Result<()> {
        if let Err(e) = self.context.membership.set(state) {
            bail!("membership is inited: {}", e);
        }
        self.context.msg_id.store(1, Release);
        Ok(())
    }

    pub async fn done(&self) {
        self.executor.done().await;
    }

    pub async fn run(&self) -> Result<()> {
        let (tx_err, mut rx_err) = mpsc::channel::<Result<()>>(1);
        let mut tx_out: Result<()> = Ok(());
        let mut lines = BufReader::new(stdin()).lines();
        loop {
            select! {
                Ok(read) = lines.next_line().fuse() => {
                    match read {
                        Some(line) =>{
                            if line.trim().is_empty() {
                                continue;
                            }

                            info!("Received {line}");

                            let tx_err0 = tx_err.clone();
                            self.executor.spawn(Self::process_input(self.clone(), line).then(|result| async move  {
                                if let Err(e) = result {
                                    if let Some(Error::NotSupported(t)) = e.downcast_ref::<Error>() {
                                        warn!("message type not supported: {t}");
                                    } else {
                                        error!("process_request error: {e}");
                                        let _ = tx_err0.send(Err(e)).await;
                                    }
                                }
                            }));
                        }
                        None => break
                    }
                },
                Some(e) = rx_err.recv() => { tx_out = e; break },
                else => break
            }
        }

        select! {
            () = self.done() => {},
            Some(e) = rx_err.recv() => tx_out = e,
        }

        if tx_out.is_ok() {
            if let Ok(err) = rx_err.try_recv() {
                tx_out = err;
            }
        }

        rx_err.close();

        if let Err(e) = tx_out {
            debug!("node error: {e}");
            return Err(e);
        }

        Ok(())
    }

    // TODO: move to this to input modeule
    async fn process_input(runtime: Runtime, line: String) -> Result<()> {
        let msg = match serde_json::from_str::<Message>(line.as_str()) {
            Ok(v) => v,
            Err(err) => return Err(Box::new(err)),
        };

        if msg.is_reply() {
            let mut replies = runtime.context.pending_replies.lock().await;
            let tx = replies.remove(&msg.body.in_reply_to);
            if let Some(tx) = tx {
                tx.send(msg);
            }
            return Ok(());
        }

        let mut init_source: Option<(String, u64)> = None;
        let is_init = msg.is_init();
        if msg.is_init() {
            init_source = Some((msg.src.clone(), msg.body.msg_id));
            runtime.process_init(&msg)?;
        }

        if let Some(handler) = runtime.context.node.get() {
            // I am not happy we are cloning a msg here, but let it go this time.
            let res = handler.process(runtime.clone(), msg.clone()).await;
            if res.is_err() {
                // rpc error is user level error
                if let Some(user_err) = rpc_err_to_response(&res) {
                    runtime.reply(msg, user_err).await?;
                } else {
                    return res;
                }
            }
        }

        if is_init {
            let (dst, msg_id) = init_source.unwrap();
            let init_resp: Value = serde_json::from_str(
                format!(r#"{{"in_reply_to":{msg_id},"type":"init_ok"}}"#).as_str(),
            )?;
            return runtime.send(dst, init_resp).await;
        }

        Ok(())
    }

    fn process_init(&self, message: &Message) -> Result<()> {
        let raw = message.body.extra.clone();
        let init = serde_json::from_value::<InitMessageBody>(Value::Object(raw))?;
        self.set_membership_state(MembershipState {
            node_id: init.node_id,
            nodes: init.nodes,
        })
    }

    pub fn next_msg_id(&self) -> u64 {
        self.context.msg_id.fetch_add(1, AcqRel)
    }

    pub fn empty_response() -> Value {
        Value::Object(serde_json::Map::default())
    }

    pub(crate) async fn insert_sender(
        &self,
        id: u64,
        tx: Sender<Message>,
    ) -> Option<Sender<Message>> {
        self.context.pending_replies.lock().await.insert(id, tx)
    }

    pub(crate) async fn remove_sender(&self, id: u64) -> Option<Sender<Message>> {
        self.context.pending_replies.lock().await.remove(&id)
    }

    /// All nodes that are not this node.
    pub fn neighbours(&self) -> impl Iterator<Item = &String> {
        let n = self.node_id();
        self.nodes()
            .iter()
            .filter(move |t: &&String| t.as_str() != n)
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Runtime {
            context: Arc::new(Context {
                msg_id: AtomicU64::new(1),
                membership: OnceCell::new(),
                node: OnceCell::new(),
                pending_replies: Mutex::default(),
                stdout: Mutex::new(stdout()),
            }),
            executor: RuntimeExecutor::default(),
        }
    }
}

impl Clone for Runtime {
    fn clone(&self) -> Self {
        Runtime {
            context: self.context.clone(),
            executor: self.executor.clone(),
        }
    }
}

/// Returns a result with `NotSupported` error meaning that `Node.process()`
/// is not aware of specific message type or Ok(()) for init.
#[allow(clippy::needless_pass_by_value)]
pub fn done(runtime: Runtime, message: Message) -> Result<()> {
    if message.is_init() {
        return Ok(());
    }

    let err = Error::NotSupported(message.body.typ.clone());
    let msg: ErrorMessageBody = err.clone().into();

    let runtime0 = runtime.clone();
    runtime.executor.spawn(async move {
        let _ = runtime0.reply(message, msg).await;
    });

    Err(Box::new(err))
}

#[cfg(test)]
mod test {
    use crate::{executor::Executor, protocol::Message, MembershipState, Result, Runtime};
    use async_trait::async_trait;
    use simple_error::bail;

    use super::Node;

    #[derive(Default, Copy, Clone, PartialEq, Eq, Debug)]
    pub struct IOFailingNode {}

    #[async_trait]
    impl Node for IOFailingNode {
        async fn process(&self, _: Runtime, _: Message) -> Result<()> {
            bail!("IOFailingNode: process failed")
        }
    }

    #[test]
    fn membership() -> Result<()> {
        let tokio_runtime = tokio::runtime::Runtime::new()?;

        tokio_runtime.block_on(async move {
            let runtime = Runtime::new();
            let runtime0 = runtime.clone();
            let s1 = MembershipState::example("n0", &["n0", "n1"]);
            let s2 = MembershipState::example("n1", &["n0", "n1"]);
            runtime.executor.spawn(async move {
                runtime0.set_membership_state(s1).unwrap();
                async move {
                    assert!(runtime0.set_membership_state(s2).is_err());
                }
                .await;
            });
            runtime.done().await;
            assert_eq!(
                runtime.node_id(),
                "n0",
                "invalid node id, can't be anything else"
            );
        });
        Ok(())
    }

    impl MembershipState {
        fn example(n: &str, s: &[&str]) -> Self {
            MembershipState {
                node_id: n.to_string(),
                nodes: s.iter().map(|x| (*x).to_string()).collect(),
            }
        }
    }
}
