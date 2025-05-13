# maelstrom-broadcast-node

This is a fork of maelstrom-node stripped down to include only the broadcast implementation, along with the following traits:

- Better error handling, instead of Box<dyn E> - we now have concrete errors
- Simpler Tokio Executor instead of Waitgroup
- Refactoring & naming to reflect simplicity.

# What is Maelstrom?

Maelstrom is a platform for learning distributed systems. It is build around Jepsen and Elle to ensure no properties are
violated. With maelstrom you build nodes that form distributed system that can process different workloads.

# Features

- async (tokio)
- multi-threading
- simple API - single trait fn to implement
- response types auto-deduction, extra data available via Value()
- unknown message types handling

# Example

```rust
use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use std::sync::Arc;

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let handler = Arc::new(Handler::default());
    Runtime::new().with_handler(handler).run().await
}

#[derive(Clone, Default)]
struct Handler {}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        if req.get_type() == "echo" {
            let echo = req.body.clone().with_type("echo_ok");
            return runtime.reply(req, echo).await;
        }

        done(runtime, req)
    }
}
```

# Maelstrom Input

## Request Payload

```json
{
  "src": "c1",
  "dest": "n1",
  "body": {
    "type": "echo",
    "msg_id": 1,
    "echo": "Please echo 35"
  }
}
```

## Response

```json
{
  "src": "n1",
  "dest": "c1",
  "body": {
    "type": "echo_ok",
    "msg_id": 1,
    "in_reply_to": 1,
    "echo": "Please echo 35"
  }
}
```

## Broadcast

```bash
#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let msg: Result<Request> = req.body.as_obj();
        match msg {
            Ok(Request::Read {}) => {
                let data = self.snapshot();
                let msg = Request::ReadOk { messages: data };
                return runtime.reply(req, msg).await;
            }
            Ok(Request::Broadcast { message: element }) => {
                if self.try_add(element) {
                    info!("messages now {}", element);
                    for node in runtime.neighbours() {
                        runtime.call_async(node, Request::Broadcast { message: element });
                    }
                }

                return runtime.reply_ok(req).await;
            }
            Ok(Request::Topology { topology }) => {
                let neighbours = topology.get(runtime.node_id()).unwrap();
                self.inner.lock().unwrap().t = neighbours.clone();
                info!("My neighbors are {:?}", neighbours);
                return runtime.reply_ok(req).await;
            }
            _ => done(runtime, req),
        }
    }
}
```

# Credits

- [maelstrom-rust-node](https://github.com/sitano/maelstrom-rust-node)
- [Gossip Glomers challenge on Fly.io](https://fly.io/dist-sys/)
- [Maelstrom](https://github.com/jepsen-io/maelstrom)
