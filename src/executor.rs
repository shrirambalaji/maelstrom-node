use std::future::Future;
use std::sync::atomic::{
    AtomicUsize,
    Ordering::{AcqRel, Acquire},
};
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

/// an abstract executor for background tasks + shutdown
pub trait Executor: Clone + Send + Sync + 'static {
    fn spawn<F>(&self, fut: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;

    fn done(&self) -> impl Future<Output = ()> + Send + 'static;
}

#[derive(Clone, Default)]
pub struct RuntimeExecutor {
    counter: Arc<AtomicUsize>,
    notify: Arc<Notify>,
}

impl Executor for RuntimeExecutor {
    fn spawn<F>(&self, fut: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        // bump
        self.counter.fetch_add(1, AcqRel);
        let counter = self.counter.clone();
        let notify = self.notify.clone();

        tokio::spawn(async move {
            let out = fut.await;
            // drop
            if counter.fetch_sub(1, AcqRel) == 1 {
                notify.notify_waiters();
            }
            out
        })
    }

    fn done(&self) -> impl Future<Output = ()> + Send + 'static {
        let ctr = self.counter.clone();
        let notify = self.notify.clone();
        async move {
            if ctr.load(Acquire) == 0 {
                return;
            }
            notify.notified().await;
        }
    }
}
