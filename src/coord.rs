use super::consts::{ALPN_CONEC, DFLT_PORT, MAX_LOOPS};
use super::types::{ConecChannel, ConecConnection, CoordEvent};

use anyhow::{anyhow, Context, Result};
use futures::channel::mpsc;
use futures::prelude::*;
use quinn::{Certificate, CertificateChain, Endpoint, Incoming, PrivateKey, ServerConfigBuilder};
use std::fs::read as fs_read;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task;

#[derive(Clone, Debug)]
pub struct CoordConfig {
    laddr: SocketAddr,
    keylog: bool,
    cert: CertificateChain,
    key: PrivateKey,
}

impl CoordConfig {
    pub fn new(cert_path: PathBuf, key_path: PathBuf) -> Result<Self> {
        let key = {
            let tmp = fs_read(&key_path).context("failed to read private key")?;
            if key_path.extension().map_or(false, |x| x == "der") {
                PrivateKey::from_der(&tmp)?
            } else {
                PrivateKey::from_pem(&tmp)?
            }
        };
        let cert = {
            let tmp = fs_read(&cert_path).context("failed to read certificate chain")?;
            if cert_path.extension().map_or(false, |x| x == "der") {
                CertificateChain::from_certs(Certificate::from_der(&tmp))
            } else {
                CertificateChain::from_pem(&tmp)?
            }
        };
        Ok(Self {
            laddr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), DFLT_PORT),
            keylog: false,
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

    pub fn enable_keylog(&mut self) -> &mut Self {
        self.keylog = true;
        self
    }
}

pub(crate) struct CoordInner {
    endpoint: Endpoint,
    incoming: Incoming,
    clients: Vec<ConecChannel>,
    driver: Option<task::Waker>,
    ref_count: usize,
    sender: mpsc::UnboundedSender<CoordEvent>,
    events: mpsc::UnboundedReceiver<CoordEvent>,
}

impl CoordInner {
    /// try to accept a new connection from a client
    fn drive_accept(&mut self, cx: &mut task::Context) -> Result<bool> {
        let mut accepted = 0;
        //let mut incoming_p = Pin::new(&mut *self.incoming);
        loop {
            use task::Poll;
            match self.incoming.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => {
                    return Err(anyhow!("accept failed: unexpected end of Incoming stream"));
                }
                Poll::Ready(Some(incoming)) => {
                    let sender = self.sender.clone();
                    tokio::spawn(async move {
                        let mut conn =
                            match incoming.await.map_err(|e| anyhow!("accept failed: {}", e)) {
                                Err(e) => {
                                    sender.unbounded_send(CoordEvent::Error(e)).unwrap();
                                    return;
                                }
                                Ok(conn) => ConecConnection::new(conn),
                            };
                        let ctrl = match conn
                            .accept_ctrl(None)
                            .await
                            .map_err(|e| anyhow!("failed to accept control stream: {}", e))
                        {
                            Err(e) => {
                                sender.unbounded_send(CoordEvent::Error(e)).unwrap();
                                return;
                            }
                            Ok(ctrl) => ctrl,
                        };
                        sender
                            .unbounded_send(CoordEvent::Accepted(ConecChannel { conn, ctrl }))
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
                    Error(e) => println!("err: {}", e),
                    Accepted(c) => self.clients.push(c),
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
            clients: vec![],
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
    pub(crate) inner: CoordRef,
}

impl Coord {
    /// construct a new coord
    pub async fn new(config: CoordConfig) -> Result<Self> {
        // build configuration
        let mut qsc = ServerConfigBuilder::default();
        qsc.protocols(ALPN_CONEC);
        // qsc.use_stateless_retry(true); // XXX ???
        if config.keylog {
            qsc.enable_keylog();
        }
        qsc.certificate(config.cert, config.key)?;

        // build QUIC endpoint
        let mut endpoint = Endpoint::builder();
        endpoint.listen(qsc.build());
        let (endpoint, incoming) = endpoint.bind(&config.laddr)?;

        let inner = CoordRef::new(endpoint, incoming);
        let driver = CoordDriver(inner.clone());
        tokio::spawn(async move {
            if let Err(e) = driver.await {
                anyhow!("coordinator error: {}", e);
            }
        });

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
    type Output = Result<()>;

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
