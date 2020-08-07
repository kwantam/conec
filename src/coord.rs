use super::consts::{ALPN_CONEC, DFLT_PORT, MAX_LOOPS};
use super::types::{ConecConnection, ControlMsg, CtrlStream};

use futures::channel::mpsc;
use futures::prelude::*;
use quinn::{Certificate, CertificateChain, Endpoint, Incoming, PrivateKey, ServerConfigBuilder};
use std::collections::HashMap;
use std::fs::read as fs_read;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task;

#[derive(Clone, Debug)]
pub struct CoordConfig {
    laddr: SocketAddr,
    keylog: bool,
    stateless_retry: bool,
    cert: CertificateChain,
    key: PrivateKey,
}

impl CoordConfig {
    pub fn new(cert_path: PathBuf, key_path: PathBuf) -> io::Result<Self> {
        let key = {
            let tmp = fs_read(&key_path).map_err(|e| {
                io::Error::new(e.kind(), format!("failed to read private key: {}", e))
            })?;
            if key_path.extension().map_or(false, |x| x == "der") {
                PrivateKey::from_der(&tmp).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
            } else {
                PrivateKey::from_pem(&tmp).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
            }
        };
        let cert = {
            let tmp = fs_read(&cert_path).map_err(|e| {
                io::Error::new(e.kind(), format!("failed to read certificate chain: {}", e))
            })?;
            if cert_path.extension().map_or(false, |x| x == "der") {
                CertificateChain::from_certs(Certificate::from_der(&tmp))
            } else {
                CertificateChain::from_pem(&tmp)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
            }
        };
        Ok(Self {
            laddr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), DFLT_PORT),
            keylog: false,
            stateless_retry: false,
            cert,
            key,
        })
    }

    pub fn set_port(&mut self, port: u16) -> &mut Self {
        self.laddr.set_port(port);
        self
    }

    pub fn set_ip(&mut self, ip: IpAddr) -> &mut Self {
        self.laddr.set_ip(ip);
        self
    }

    // log master secret to ENV{SSLKEYLOGFILE}
    pub fn enable_keylog(&mut self) -> &mut Self {
        self.keylog = true;
        self
    }

    // Per QUIC spec, stateless retry defends against client address spoofing.
    pub fn enable_stateless_retry(&mut self) -> &mut Self {
        self.stateless_retry = true;
        self
    }
}

struct CoordChanInner {
    conn: ConecConnection,
    ctrl: CtrlStream,
    peer: String,
    sender: mpsc::UnboundedSender<CoordEvent>,
    ref_count: usize,
    driver: Option<task::Waker>,
}

struct CoordChanRef(Arc<Mutex<CoordChanInner>>);

impl std::ops::Deref for CoordChanRef {
    type Target = Mutex<CoordChanInner>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Clone for CoordChanRef {
    fn clone(&self) -> Self {
        self.lock().unwrap().ref_count += 1;
        Self(self.0.clone())
    }
}

impl Drop for CoordChanRef {
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

impl CoordChanRef {
    fn new(
        conn: ConecConnection,
        ctrl: CtrlStream,
        peer: String,
        sender: mpsc::UnboundedSender<CoordEvent>,
    ) -> Self {
        Self(Arc::new(Mutex::new(CoordChanInner {
            conn,
            ctrl,
            peer,
            sender,
            ref_count: 0,
            driver: None,
        })))
    }
}

pub struct CoordChan {
    inner: CoordChanRef,
}

enum CoordEvent {
    Accepted(ConecConnection, CtrlStream, String),
    Error(io::Error),
}

pub(crate) struct CoordInner {
    endpoint: Endpoint,
    incoming: Incoming,
    clients: HashMap<String, CoordChan>,
    driver: Option<task::Waker>,
    ref_count: usize,
    sender: mpsc::UnboundedSender<CoordEvent>,
    events: mpsc::UnboundedReceiver<CoordEvent>,
}

impl CoordInner {
    /// try to accept a new connection from a client
    fn drive_accept(&mut self, cx: &mut task::Context) -> io::Result<bool> {
        let mut accepted = 0;
        //let mut incoming_p = Pin::new(&mut *self.incoming);
        loop {
            use task::Poll;
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
                    tokio::spawn(async move {
                        let mut conn = match incoming.await.map_err(|e| {
                            io::Error::new(io::ErrorKind::Other, format!("accept failed: {}", e))
                        }) {
                            Err(e) => {
                                sender.unbounded_send(CoordEvent::Error(e)).unwrap();
                                return;
                            }
                            Ok(conn) => ConecConnection::new(conn),
                        };
                        let (ctrl, peer) = match conn.accept_ctrl(None).await.map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::Other,
                                format!("failed to accept control stream: {}", e),
                            )
                        }) {
                            Err(e) => {
                                sender.unbounded_send(CoordEvent::Error(e)).unwrap();
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

    /// handle events arriving on receiver
    fn handle_events(&mut self, cx: &mut task::Context) {
        use task::Poll;
        use CoordEvent::*;
        loop {
            match self.events.poll_next_unpin(cx) {
                Poll::Ready(Some(event)) => match event {
                    Error(e) => {
                        // XXX what do we do here?
                        println!("err: {}", e);
                    }
                    Accepted(conn, mut ctrl, peer) => {
                        // Note: unwrapping get_peer() is OK --- peer is a client, not a Coord
                        if self.clients.get(&peer[..]).is_some() {
                            tokio::spawn(async move {
                                println!("error: name '{}' already in use", peer);
                                ctrl.send(ControlMsg::Error("name already in use".to_string()))
                                    .await
                                    .ok();
                                drop(ctrl);
                                drop(conn);
                            });
                        } else {
                            let inner =
                                CoordChanRef::new(conn, ctrl, peer.clone(), self.sender.clone());
                            // XXX start driver here
                            self.clients.insert(peer, CoordChan { inner });
                        }
                    }
                },
                Poll::Ready(None) => unreachable!("CoordInner owns a sender; something is wrong"),
                Poll::Pending => break,
            }
        }
    }
}

// a shared reference to a Coordinator
pub(crate) struct CoordRef(Arc<Mutex<CoordInner>>);

impl CoordRef {
    pub(crate) fn new(endpoint: Endpoint, incoming: Incoming) -> Self {
        let (sender, events) = mpsc::unbounded();
        Self(Arc::new(Mutex::new(CoordInner {
            endpoint,
            incoming,
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
        qsc.certificate(config.cert, config.key)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // build QUIC endpoint
        let mut endpoint = Endpoint::builder();
        endpoint.listen(qsc.build());
        let (endpoint, incoming) = endpoint
            .bind(&config.laddr)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let inner = CoordRef::new(endpoint, incoming);
        let driver = CoordDriver(inner.clone());
        tokio::spawn(async move { driver.await });

        Ok(Self { inner })
    }

    pub fn num_clients(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.clients.len()
    }
}

#[must_use = "coord driver must be spawned!"]
pub(crate) struct CoordDriver(CoordRef);

impl Future for CoordDriver {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context) -> task::Poll<Self::Output> {
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
            task::Poll::Ready(Ok(()))
        } else {
            task::Poll::Pending
        }
    }
}
