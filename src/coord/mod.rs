mod chan;
pub(crate) mod config;

use crate::consts::{ALPN_CONEC, MAX_LOOPS};
use crate::types::{ConecConn, ControlMsg, CtrlStream};
use chan::{CoordChan, CoordChanDriver, CoordChanRef};
use config::CoordConfig;

use futures::channel::mpsc;
use futures::prelude::*;
use quinn::{CertificateChain, Endpoint, Incoming, ServerConfigBuilder};
use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

enum CoordEvent {
    Accepted(ConecConn, CtrlStream, String),
    AcceptError(io::Error),
    ChanClose(String),
}

struct CoordInner {
    endpoint: Endpoint,
    incoming: Incoming,
    certs: CertificateChain,
    clients: HashMap<String, CoordChan>,
    driver: Option<Waker>,
    ref_count: usize,
    sender: mpsc::UnboundedSender<CoordEvent>,
    events: mpsc::UnboundedReceiver<CoordEvent>,
}

impl CoordInner {
    /// try to accept a new connection from a client
    fn drive_accept(&mut self, cx: &mut Context) -> io::Result<bool> {
        let mut accepted = 0;
        loop {
            match self.incoming.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "accept failed: unexpected end of Incoming stream",
                    ));
                }
                Poll::Ready(Some(incoming)) => {
                    let sender = self.sender.clone();
                    let certs = self.certs.clone();
                    tokio::spawn(async move {
                        let mut conn = match incoming.await.map_err(|e| {
                            io::Error::new(io::ErrorKind::Other, format!("accept failed: {}", e))
                        }) {
                            Err(e) => {
                                sender.unbounded_send(CoordEvent::AcceptError(e)).unwrap();
                                return;
                            }
                            Ok(conn) => ConecConn::new(conn),
                        };
                        let (ctrl, peer) = match conn.connect_ctrl(certs).await.map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::Other,
                                format!("failed to accept control stream: {}", e),
                            )
                        }) {
                            Err(e) => {
                                sender.unbounded_send(CoordEvent::AcceptError(e)).unwrap();
                                return;
                            }
                            Ok(ctrl_peer) => ctrl_peer,
                        };
                        sender
                            .unbounded_send(CoordEvent::Accepted(conn, ctrl, peer))
                            .unwrap();
                    });
                }
            }
            accepted += 1;
            if accepted >= MAX_LOOPS {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// handle events arriving on self.events
    fn handle_events(&mut self, cx: &mut Context) {
        use CoordEvent::*;
        loop {
            match self.events.poll_next_unpin(cx) {
                Poll::Ready(Some(event)) => match event {
                    AcceptError(e) => {
                        // XXX what do we do here?
                        println!("err: {}", e);
                    }
                    Accepted(conn, mut ctrl, peer) => {
                        if self.clients.get(&peer[..]).is_some() {
                            tokio::spawn(async move {
                                println!("error: name '{}' already in use", peer);
                                ctrl.send(ControlMsg::HelloError("name in use".to_string()))
                                    .await
                                    .ok();
                                ctrl.finish().await.ok();
                                drop(ctrl);
                                drop(conn);
                            });
                        } else {
                            let inner =
                                CoordChanRef::new(conn, ctrl, peer.clone(), self.sender.clone());

                            // spawn channel driver
                            let driver = CoordChanDriver(inner.clone());
                            tokio::spawn(async move { driver.await });

                            self.clients.insert(peer, CoordChan { inner });
                        }
                    }
                    ChanClose(client) => {
                        // client channel closed --- drop it from the queue
                        self.clients.remove(&client);
                    }
                },
                Poll::Ready(None) => unreachable!("CoordInner owns a sender; something is wrong"),
                Poll::Pending => break,
            }
        }
    }
}

// a shared reference to a Coordinator
struct CoordRef(Arc<Mutex<CoordInner>>);

impl CoordRef {
    fn new(endpoint: Endpoint, incoming: Incoming, certs: CertificateChain) -> Self {
        let (sender, events) = mpsc::unbounded();
        Self(Arc::new(Mutex::new(CoordInner {
            endpoint,
            incoming,
            certs,
            clients: HashMap::new(),
            driver: None,
            ref_count: 0,
            sender,
            events,
        })))
    }
}

impl Clone for CoordRef {
    fn clone(&self) -> Self {
        self.lock().unwrap().ref_count += 1;
        Self(self.0.clone())
    }
}

impl Drop for CoordRef {
    fn drop(&mut self) {
        let inner = &mut *self.0.lock().unwrap();
        if let Some(x) = inner.ref_count.checked_sub(1) {
            inner.ref_count = x;
            if x == 0 {
                if let Some(task) = inner.driver.take() {
                    task.wake();
                }
            }
        }
    }
}

impl std::ops::Deref for CoordRef {
    type Target = Mutex<CoordInner>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct Coord {
    inner: CoordRef,
}

impl Coord {
    /// construct a new coord
    pub async fn new(config: CoordConfig) -> io::Result<Self> {
        // build configuration
        let mut qsc = ServerConfigBuilder::default();
        qsc.protocols(ALPN_CONEC);
        qsc.use_stateless_retry(config.stateless_retry);
        if config.keylog {
            qsc.enable_keylog();
        }
        qsc.certificate(config.cert.clone(), config.key)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // build QUIC endpoint
        let mut endpoint = Endpoint::builder();
        endpoint.listen(qsc.build());
        let (endpoint, incoming) = endpoint
            .bind(&config.laddr)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let inner = CoordRef::new(endpoint, incoming, config.cert);
        let driver = CoordDriver(inner.clone());
        tokio::spawn(async move { driver.await });

        Ok(Self { inner })
    }

    pub fn num_clients(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.clients.len()
    }
}

#[must_use = "CoordDriver must be spawned!"]
struct CoordDriver(CoordRef);

impl Future for CoordDriver {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let inner = &mut *self.0.lock().unwrap();
        match &inner.driver {
            Some(w) if w.will_wake(cx.waker()) => (),
            _ => {
                inner.driver = Some(cx.waker().clone());
            }
        };
        loop {
            let mut keep_going = false;
            keep_going |= inner.drive_accept(cx)?;
            inner.handle_events(cx);
            if !keep_going {
                break;
            }
        }
        if inner.ref_count == 0 && inner.clients.is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}
