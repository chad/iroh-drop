//! A client of the control API, over any frame pipe.
//!
//! [`Client::connect_memory`] runs the daemon in the caller's own process —
//! which is how "no background service" stays a supported configuration, and
//! how the whole API becomes testable without a socket.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::frame::{Envelope, Frame, Hello};
use crate::service::{ApiError, Service};

/// Answers the daemon's `Ask` questions. Returning `None` means deny.
///
/// Handlers may block (a terminal prompt does); they are run on a blocking task
/// so they cannot stall the connection. A UI should ignore this and use
/// [`Client::asks`] with [`Client::answer`] instead, which never blocks at all.
pub type AskHandler = Arc<dyn Fn(String, Value) -> Option<Value> + Send + Sync>;

/// A question from the daemon, for clients that answer asynchronously.
#[derive(Clone, Debug)]
pub struct AskRequest {
    /// Correlation id to pass to [`Client::answer`].
    pub id: u64,
    /// Question name, e.g. `offer.accept`.
    pub q: String,
    /// Context for the decision. Untrusted display metadata.
    pub p: Value,
}

/// In-flight `call`s waiting for their reply.
type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Value, ApiError>>>>>;

/// A connected client.
pub struct Client {
    outbound: mpsc::Sender<Frame>,
    next_id: AtomicU64,
    pending: Pending,
    events: broadcast::Sender<Envelope>,
    asks: broadcast::Sender<AskRequest>,
    /// Kept so the reader task lives as long as the client.
    reader: tokio::task::JoinHandle<()>,
    /// What the daemon told us at handshake time.
    pub hello: Value,
}

impl Drop for Client {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

impl Client {
    /// Attach to a service in this process.
    pub async fn connect_memory(
        service: &Arc<Service>,
        hello: Hello,
        on_ask: Option<AskHandler>,
    ) -> Result<Self, ApiError> {
        let (to_daemon, daemon_rx) = mpsc::channel::<Frame>(64);
        let (to_client, client_rx) = mpsc::channel::<Frame>(256);
        service.attach(daemon_rx, to_client);
        Self::start(to_daemon, client_rx, hello, on_ask).await
    }

    /// Drive a client over any pair of frame channels. Transports call this.
    pub(crate) async fn start(
        outbound: mpsc::Sender<Frame>,
        mut inbound: mpsc::Receiver<Frame>,
        hello: Hello,
        on_ask: Option<AskHandler>,
    ) -> Result<Self, ApiError> {
        // Handshake before starting the reader, so the reply cannot race it.
        outbound
            .send(Frame::Req {
                id: 0,
                m: "hello".into(),
                p: serde_json::to_value(&hello).expect("hello serializes"),
            })
            .await
            .map_err(|_| ApiError::new("disconnected", "daemon went away"))?;

        let hello_result = match inbound.recv().await {
            Some(Frame::Res { p, .. }) => p,
            Some(Frame::Err { code, msg, .. }) => return Err(ApiError { code, msg }),
            _ => {
                return Err(ApiError::new(
                    "disconnected",
                    "daemon closed during handshake",
                ))
            }
        };

        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let (events, _) = broadcast::channel(1024);
        let (asks, _) = broadcast::channel(64);

        let reader_pending = Arc::clone(&pending);
        let reader_events = events.clone();
        let reader_asks = asks.clone();
        let reader_out = outbound.clone();
        let reader = tokio::spawn(async move {
            while let Some(frame) = inbound.recv().await {
                match frame {
                    Frame::Res { id, p } => {
                        if let Some(tx) = reader_pending.lock().expect("pending").remove(&id) {
                            let _ = tx.send(Ok(p));
                        }
                    }
                    Frame::Err { id, code, msg } => {
                        if let Some(tx) = reader_pending.lock().expect("pending").remove(&id) {
                            let _ = tx.send(Err(ApiError { code, msg }));
                        }
                    }
                    Frame::Ev { seq, e, p } => {
                        let _ = reader_events.send(Envelope { seq, e, p });
                    }
                    Frame::Ask { id, q, p } => {
                        // Anyone watching `asks()` answers at their leisure.
                        let _ = reader_asks.send(AskRequest {
                            id,
                            q: q.clone(),
                            p: p.clone(),
                        });

                        // A blocking handler runs off this task. Blocking here
                        // would stall every other reply on the connection until
                        // the user decided — which is a frozen UI.
                        if let Some(handler) = on_ask.clone() {
                            let out = reader_out.clone();
                            tokio::task::spawn_blocking(move || {
                                let answer = handler(q.clone(), p);
                                let reply = match answer {
                                    Some(p) => Frame::Res { id, p },
                                    None => Frame::Err {
                                        id,
                                        code: "declined".into(),
                                        msg: format!("{q} declined"),
                                    },
                                };
                                let _ = out.blocking_send(reply);
                            });
                        }
                    }
                    Frame::Req { id, .. } => {
                        let _ = reader_out
                            .send(Frame::Err {
                                id,
                                code: "unsupported".into(),
                                msg: "clients do not serve methods".into(),
                            })
                            .await;
                    }
                }
            }
        });

        Ok(Self {
            outbound,
            next_id: AtomicU64::new(1),
            pending,
            events,
            asks,
            reader,
            hello: hello_result,
        })
    }

    /// Invoke a method and wait for its reply.
    pub async fn call(&self, method: &str, params: Value) -> Result<Value, ApiError> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().expect("pending").insert(id, tx);
        self.outbound
            .send(Frame::Req {
                id,
                m: method.into(),
                p: params,
            })
            .await
            .map_err(|_| ApiError::new("disconnected", "daemon went away"))?;
        rx.await
            .map_err(|_| ApiError::new("disconnected", "no reply"))?
    }

    /// Subscribe to the event stream. Late subscribers can catch up with
    /// `events.replay`.
    pub fn events(&self) -> broadcast::Receiver<Envelope> {
        self.events.subscribe()
    }

    /// Subscribe to consent questions, to answer them with [`Self::answer`].
    ///
    /// This is what a UI wants: the question arrives as a message, the window
    /// keeps rendering, and the answer goes back whenever the human decides.
    pub fn asks(&self) -> broadcast::Receiver<AskRequest> {
        self.asks.subscribe()
    }

    /// Answer a question. `None` declines.
    ///
    /// Not answering is also a refusal: the daemon times out and denies.
    pub async fn answer(&self, id: u64, answer: Option<Value>) {
        let reply = match answer {
            Some(p) => Frame::Res { id, p },
            None => Frame::Err {
                id,
                code: "declined".into(),
                msg: "declined".into(),
            },
        };
        let _ = self.outbound.send(reply).await;
    }

    /// Wait for the first event matching a predicate, with a timeout.
    pub async fn wait_for(
        &self,
        timeout: std::time::Duration,
        pred: impl Fn(&Envelope) -> bool,
    ) -> Result<Envelope, ApiError> {
        let mut rx = self.events();
        tokio::time::timeout(timeout, async move {
            loop {
                match rx.recv().await {
                    Ok(env) if pred(&env) => return Ok(env),
                    Ok(_) => continue,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => {
                        return Err(ApiError::new("disconnected", "event stream closed"))
                    }
                }
            }
        })
        .await
        .map_err(|_| ApiError::new("timeout", "no matching event"))?
    }

    /// Convenience: an accept-everything ask handler, for tests and headless
    /// seeding. Never ship this as a UI default.
    pub fn accept_all() -> AskHandler {
        Arc::new(|_q, _p| Some(json!({"accept": true})))
    }
}
