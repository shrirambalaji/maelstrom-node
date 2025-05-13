#![allow(dead_code)]

use crate::error::Error;
use crate::executor::{Executor, RuntimeExecutor};
use crate::protocol::{ErrorMessageBody, InitMessageBody, Message};
use crate::rpc::RpcResult;
use async_trait::async_trait;
use futures::FutureExt;
use log::{error, warn};
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::ops::Deref;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{AcqRel, Release};
use std::sync::Arc;
use tokio::io::{stdin, stdout, AsyncBufReadExt, AsyncWriteExt, BufReader, Stdout};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{self, Receiver};
use tokio::sync::oneshot::{self};
use tokio::sync::{Mutex, OnceCell};
pub type Result<T> = std::result::Result<T, Error>;

pub struct Runtime(Arc<RuntimeInner>);

pub struct MessageId(AtomicU64);

impl Default for MessageId {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageId {
    #[must_use]
    pub fn new() -> Self {
        Self(AtomicU64::new(1))
    }

    pub fn next(&self) -> u64 {
        self.0.fetch_add(1, AcqRel)
    }

    pub fn reset(&self) {
        self.0.store(1, Release);
    }
}

pub struct RuntimeInner {
    state: Arc<State>,
    executor: RuntimeExecutor,
}

/// Represents the inflight RPC calls that are waiting for a reply.
/// This is used to track pending replies for messages sent to other nodes.
pub struct InflightReplies(Mutex<HashMap<u64, oneshot::Sender<Message>>>);

impl Default for InflightReplies {
    fn default() -> Self {
        Self::new()
    }
}

impl InflightReplies {
    #[must_use]
    pub fn new() -> Self {
        InflightReplies(Mutex::new(HashMap::new()))
    }

    pub async fn insert(
        &self,
        id: u64,
        tx: oneshot::Sender<Message>,
    ) -> Option<oneshot::Sender<Message>> {
        self.0.lock().await.insert(id, tx)
    }

    pub async fn remove(&self, id: u64) -> Option<oneshot::Sender<Message>> {
        self.0.lock().await.remove(&id)
    }
}

struct State {
    /// an atomically incrementing message identifier
    msg_id: MessageId,

    /// a collection of nodes
    cluster: OnceCell<Cluster>,

    /// the current node
    node: OnceCell<Arc<dyn Node>>,

    /// a collection of pending replies
    pending_replies: InflightReplies,

    /// a mutex for stdout
    stdout: Mutex<Stdout>,
}

#[async_trait]
pub trait Node: Sync + Send {
    async fn process(&self, runtime: Runtime, request: Message) -> Result<()>;
}

#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct Cluster {
    pub node_id: String,
    pub nodes: Vec<String>,
}

impl Cluster {
    pub fn init(self, runtime: &Runtime) -> Result<()> {
        if let Err(_e) = runtime.state.cluster.set(self) {
            return Err(Error::ClusterAlreadyInitialized);
        }
        runtime.state.msg_id.reset();
        Ok(())
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}

impl Runtime {
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::new(RuntimeInner::default()))
    }

    pub fn with_node(self, node: Arc<dyn Node + Send + Sync>) -> Self {
        assert!(
            self.state.node.set(node).is_ok(),
            "runtime node is already initialized"
        );
        self
    }

    /// writes a message to stdout.
    pub async fn stdout(&self, output: &str) -> Result<()> {
        let mut out = self.state.stdout.lock().await;
        out.write_all(output.as_bytes()).await?;
        out.write_all(b"\n").await?;
        Ok(())
    }

    /// `send_message()` sends a message to another node through stdout.
    pub async fn send_message(&self, msg: Message) -> Result<()> {
        self.stdout(serde_json::to_string(&msg)?.as_str()).await
    }

    /// `send()` sends a message to another node.
    pub async fn send(&self, to: impl Into<String>, message: impl Serialize) -> Result<()> {
        let from = self.node_id();
        let msg = crate::protocol::build_message(from, to, message)?;
        self.send_message(msg).await
    }

    /// `reply()` sends a reply to a message.
    /// It sets the `in_reply_to` field of the message to the original sender's ID.
    pub async fn reply(&self, req: Message, resp: impl Serialize) -> Result<()> {
        let mut msg = crate::protocol::build_message(self.node_id(), req.src, resp)?;
        msg.body.in_reply_to = req.body.msg_id;

        if !msg.body.extra.contains_key("type") && !req.body.body_type.is_empty() {
            let key = "type".to_string();
            let value = Value::String(req.body.body_type + "_ok");
            msg.body.extra.insert(key, value);
        }

        self.send_message(msg).await
    }

    // `reply_ok()` sends a reply with an empty body.
    pub async fn reply_ok(&self, req: Message) -> Result<()> {
        let response = Value::Object(serde_json::Map::default());
        self.reply(req, response).await
    }

    /// `rpc()` makes a remote call to another node via message passing interface.
    /// `RpcResult` is immediately canceled on drop.
    pub fn rpc(
        &self,
        to: impl Into<String>,
        request: impl Serialize,
    ) -> impl Future<Output = Result<RpcResult>> {
        let msg = crate::protocol::build_message(self.node_id(), to, request);
        let msg_id = self.state.msg_id.next();
        let rpc_response: Result<String> = match msg {
            Ok(mut t) => {
                t.body.msg_id = msg_id;
                match serde_json::to_string(&t) {
                    Ok(s) => Ok(s),
                    Err(e) => Err(Error::Json(e)),
                }
            }
            Err(e) => Err(e),
        };

        crate::rpc(self.clone(), msg_id, rpc_response)
    }

    /// `execute_rpc()` executes a rpc call in a separate task.
    pub fn execute_rpc(&self, to: impl Into<String>, request: impl Serialize + 'static) {
        self.executor.spawn(self.rpc(to.into(), request));
    }

    #[must_use]
    pub fn node_id(&self) -> &str {
        if let Some(v) = self.state.cluster.get() {
            return v.node_id.as_str();
        }
        ""
    }

    #[must_use]
    pub fn nodes(&self) -> &[String] {
        if let Some(v) = self.state.cluster.get() {
            return v.nodes.as_slice();
        }
        &[]
    }

    pub async fn done(&self) {
        self.executor.done().await;
    }

    async fn emit_error(&self, error_tx: &mpsc::Sender<Result<()>>, err: Error) {
        if let Error::NotSupported(ref typ) = err {
            warn!("message type not supported: {typ}");
        } else {
            error!("process_input error: {err}");
            let _ = error_tx.send(Err(err)).await;
        }
    }

    async fn on_error(&self, error_rx: &mut Receiver<Result<()>>) -> Result<()> {
        tokio::select! {
            () = self.done().fuse() => {
                match error_rx.try_recv() {
                    Ok(err) => err,
                    Err(TryRecvError::Empty | TryRecvError::Disconnected) => Ok(()),
                }
            }
            maybe_err = error_rx.recv().fuse() => {
                if let Some(err) = maybe_err { return err }
                Ok(())
            }
        }
    }

    pub async fn run(&self) -> Result<()> {
        let mut lines = BufReader::new(stdin()).lines();
        let (error_tx, mut error_rx) = mpsc::channel(1);

        // Let's read lines from stdin and process them.
        // For each line, we spawn a task to process the input, without blocking
        while let Ok(Some(line)) = lines.next_line().await {
            if line.trim().is_empty() {
                continue;
            }

            let rt = self.clone();
            let error_tx: mpsc::Sender<std::result::Result<(), Error>> = error_tx.clone();

            self.executor.spawn(async move {
                match rt.process_input(line).await {
                    Ok(()) => {}
                    Err(e) => rt.emit_error(&error_tx, e).await,
                }
            });
        }

        self.on_error(&mut error_rx).await
    }

    async fn process_input(&self, line: String) -> Result<()> {
        let msg = match serde_json::from_str::<Message>(&line) {
            Ok(v) => v,
            Err(err) => return Err(Error::Json(err)),
        };

        if msg.is_reply() {
            // let's find the sender of the message from our pending replies
            let tx = self
                .state
                .pending_replies
                .remove(msg.body.in_reply_to)
                .await;

            // got em'
            if let Some(tx) = tx {
                let _ = tx.send(msg);
            }
            return Ok(());
        }

        // first message is always init
        if msg.is_init() {
            let init_source = Some((msg.src.clone(), msg.body.msg_id));
            self.process_init(&msg)?;
            let (dst, msg_id) = init_source.unwrap();
            let init_resp: Value = serde_json::from_str(
                format!(r#"{{"in_reply_to":{msg_id},"type":"init_ok"}}"#).as_str(),
            )?;
            return self.send(dst, init_resp).await;
        }

        // check if there's  node handler set
        if let Some(handler) = self.state.node.get() {
            // call the node's process method and reply with the result
            match handler.process(self.clone(), msg.clone()).await {
                Ok(()) => {}
                Err(e) => {
                    let err = ErrorMessageBody::from_error(e);
                    self.reply(msg, err).await?;
                }
            }
        }

        Ok(())
    }

    fn process_init(&self, message: &Message) -> Result<()> {
        let raw = message.body.extra.clone();
        let init = serde_json::from_value::<InitMessageBody>(Value::Object(raw))?;
        let cluster = Cluster {
            node_id: init.node_id,
            nodes: init.nodes,
        };

        cluster.init(self)
    }

    pub(crate) async fn insert_pending_reply(
        &self,
        id: u64,
        tx: oneshot::Sender<Message>,
    ) -> Option<oneshot::Sender<Message>> {
        self.state.pending_replies.insert(id, tx).await
    }

    pub(crate) async fn remove_pending_reply(&self, id: u64) -> Option<oneshot::Sender<Message>> {
        self.state.pending_replies.remove(id).await
    }

    /// All nodes that are not this node.
    pub fn neighbours(&self) -> impl Iterator<Item = &String> {
        let n = self.node_id();
        self.nodes()
            .iter()
            .filter(move |t: &&String| t.as_str() != n)
    }

    pub fn exit(&self, message: Message) -> Result<()> {
        if message.is_init() {
            return Ok(());
        }

        let body_type = message.body.body_type.clone();
        let err: Error = Error::NotSupported(body_type.clone());

        // `NotSupported` error response body
        let response_error = ErrorMessageBody {
            typ: body_type,
            code: err.code(),
            text: err.description().to_string(),
        };

        let rt = self.clone();
        self.executor.spawn(async move {
            let _ = rt.reply(message, response_error).await;
        });

        Err(err)
    }
}

impl Default for RuntimeInner {
    fn default() -> Self {
        RuntimeInner {
            state: Arc::new(State {
                msg_id: MessageId::new(),
                cluster: OnceCell::new(),
                node: OnceCell::new(),
                pending_replies: InflightReplies(Mutex::default()),
                stdout: Mutex::new(stdout()),
            }),
            executor: RuntimeExecutor::default(),
        }
    }
}

impl Deref for Runtime {
    type Target = RuntimeInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Clone for Runtime {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[cfg(test)]
mod test {
    use crate::Error;
    use crate::{executor::Executor, protocol::Message, Cluster, Result, Runtime};
    use async_trait::async_trait;

    use super::Node;

    #[derive(Default, Copy, Clone, PartialEq, Eq, Debug)]
    pub struct IOFailingNode {}

    #[async_trait]
    impl Node for IOFailingNode {
        async fn process(&self, _: Runtime, _: Message) -> Result<()> {
            Err(Error::Custom(
                -32,
                "IOFailingNode: process failed".to_string(),
            ))
        }
    }

    #[test]
    fn cluster() -> Result<()> {
        let tokio_runtime = tokio::runtime::Runtime::new()?;

        tokio_runtime.block_on(async move {
            let runtime = Runtime::new();
            let rt = runtime.clone();

            runtime.executor.spawn(async move {
                let s1 = Cluster::example("n0", &["n0", "n1"]);
                s1.init(&rt).expect("first cluster init should succeed");

                // Second cluster initialization should fail.
                let s2 = Cluster::example("n1", &["n0", "n1"]);
                assert!(s2.init(&rt).is_err(), "second cluster init should error");
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

    impl Cluster {
        fn example(n: &str, s: &[&str]) -> Self {
            Cluster {
                node_id: n.to_string(),
                nodes: s.iter().map(|x| (*x).to_string()).collect(),
            }
        }
    }
}
