// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

/*!
This module defines the Coordinator entity and associated functionality.

See [library documentation](../index.html) for more info on how to instantiate a Coordinator.
*/

mod bchan;
mod chan;
pub(crate) mod config;
mod tls;

use crate::consts::{ALPN_CONEC, MAX_LOOPS};
use crate::types::{tagstream::TaggedInStream, ConecConn, ConecConnError, ControlMsg, CtrlStream, OutStream};
use bchan::{BroadcastChan, BroadcastChanDriver, BroadcastChanEvent, BroadcastChanRef};
pub use chan::CoordChanError;
use chan::{CoordChan, CoordChanDriver, CoordChanEvent, CoordChanRef};
use config::CoordConfig;

use err_derive::Error;
use futures::{channel::mpsc, prelude::*};
use quinn::{
    crypto::rustls::TLSError, Certificate, CertificateChain, ConnectionError, Endpoint, EndpointError, Incoming,
    IncomingBiStreams, PrivateKey, RecvStream, SendStream, ServerConfig, ServerConfigBuilder,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use tokio::task::{JoinError, JoinHandle};

/// Coordinator constructor and driver errors
#[derive(Debug, Error)]
pub enum CoordError {
    /// Transport's incoming connections stream ended
    #[error(display = "Unexpected end of Incoming stream")]
    EndOfIncomingStream,
    /// Error accepting new connection
    #[error(display = "Connection error: {:?}", _0)]
    Connect(#[source] ConnectionError),
    /// Error connecting control channel to new Client
    #[error(display = "Error connecting control channel: {:?}", _0)]
    Control(#[source] ConecConnError),
    /// Error setting up certificate chain
    #[error(display = "Certificate: {:?}", _0)]
    CertificateChain(#[source] TLSError),
    /// Error binding port for transport
    #[error(display = "Binding port: {:?}", _0)]
    Bind(#[source] EndpointError),
    /// Error from JoinHandle future
    #[error(display = "Join eror: {:?}", _0)]
    Join(#[source] JoinError),
    /// Error handing off result to driver
    #[error(display = "Error sending to driver: {:?}", _0)]
    Driver(#[source] mpsc::SendError),
    /// Events channel closed
    #[error(display = "Events channel closed")]
    EventsClosed,
}
def_into_error!(CoordError);

enum CoordEvent {
    Accepted(ConecConn, CtrlStream, IncomingBiStreams, String),
    ChanClose(String),
    BroadcastClose(String),
    NewStreamReq(String, String, u64),
    NewStreamRes(String, u64, Result<(SendStream, RecvStream), ConnectionError>),
    NewChannelReq(String, String, u64, Vec<u8>, SocketAddr),
    NewChannelRes(String, u64, SocketAddr, Vec<u8>),
    NewChannelErr(String, u64),
    NewBroadcastReq(String, OutStream, TaggedInStream),
    BroadcastCountReq(String, String, u64),
    BroadcastCountRes(String, u64, (usize, usize)),
}

struct CoordInner {
    incoming: Incoming,
    clients: HashMap<String, CoordChan>,
    broadcasts: HashMap<String, BroadcastChan>,
    ref_count: usize,
    driver: Option<Waker>,
    sender: mpsc::UnboundedSender<CoordEvent>,
    events: mpsc::UnboundedReceiver<CoordEvent>,
}

impl CoordInner {
    /// try to accept a new connection from a client
    fn drive_accept(&mut self, cx: &mut Context) -> Result<bool, CoordError> {
        let mut accepted = 0;
        loop {
            let conn = match self.incoming.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => Err(CoordError::EndOfIncomingStream),
                Poll::Ready(Some(conn)) => Ok(conn),
            }?;
            let sender = self.sender.clone();
            tokio::spawn(async move {
                use CoordError::*;
                if let Err(e) = async {
                    let (mut conn, mut ibi) = conn.await.map_err(Connect).map(ConecConn::new)?;
                    let (ctrl, peer) = conn.accept_ctrl(&mut ibi).await.map_err(Control)?;
                    sender
                        .unbounded_send(CoordEvent::Accepted(conn, ctrl, ibi, peer))
                        .map_err(|e| Driver(e.into_send_error()))
                }
                .await
                {
                    tracing::warn!("coord drive_accept: {:?}", e);
                }
            });
            accepted += 1;
            if accepted >= MAX_LOOPS {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// handle events arriving on self.events
    fn handle_events(&mut self, cx: &mut Context) -> Result<bool, CoordError> {
        use CoordEvent::*;
        let mut accepted = 0;
        loop {
            let event = match self.events.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => Err(CoordError::EventsClosed),
                Poll::Ready(Some(event)) => Ok(event),
            }?;
            match event {
                Accepted(conn, ctrl, ibi, peer) => {
                    if self.clients.get(&peer).is_some() {
                        tokio::spawn(async move {
                            let mut ctrl = ctrl;
                            ctrl.send(ControlMsg::HelloError("name in use".to_string())).await.ok();
                            ctrl.finish().await.ok();
                            drop(ctrl);
                            drop(conn);
                        });
                    } else {
                        let (inner, sender) =
                            CoordChanRef::new(conn, ctrl, ibi, peer.clone(), self.sender.clone());

                        // spawn channel driver
                        let driver = CoordChanDriver(inner.clone());
                        tokio::spawn(async move { driver.await });
                        self.clients.insert(peer, CoordChan { inner, sender });
                    }
                }
                ChanClose(client) => {
                    self.clients.remove(&client);
                }
                BroadcastClose(chan) => {
                    self.broadcasts.remove(&chan);
                }
                NewStreamReq(from, to, sid) => {
                    if let Some(c_to) = self.clients.get(&to) {
                        c_to.send(CoordChanEvent::NSReq(from, sid));
                    } else if let Some(c_from) = self.clients.get(&from) {
                        c_from.send(CoordChanEvent::NSErr(sid));
                    } else {
                        tracing::warn!("NSReq clients disappeared: {}:{} -> {}", from, sid, to);
                    }
                }
                NewStreamRes(to, sid, result) => {
                    if let Some(c_to) = self.clients.get(&to) {
                        c_to.send(CoordChanEvent::NSRes(sid, result));
                    } else {
                        tracing::warn!("NSRes client disappeared: {}:{}", to, sid);
                    }
                }
                NewChannelReq(from, to, sid, cert, addr) => {
                    if let Some(c_to) = self.clients.get(&to) {
                        c_to.send(CoordChanEvent::NCReq(from, sid, cert, addr));
                    } else if let Some(c_from) = self.clients.get(&from) {
                        c_from.send(CoordChanEvent::NCErr(sid));
                    } else {
                        tracing::warn!("NCReq clients disappeared: {}:{} -> {}", from, sid, to);
                    }
                }
                NewChannelRes(to, sid, addr, cert) => {
                    if let Some(c_to) = self.clients.get(&to) {
                        c_to.send(CoordChanEvent::NCRes(sid, addr, cert));
                    } else {
                        tracing::warn!("NCRes client disappeared: {}:{}", to, sid);
                    }
                }
                NewChannelErr(to, sid) => {
                    if let Some(c_to) = self.clients.get(&to) {
                        c_to.send(CoordChanEvent::NCErr(sid));
                    } else {
                        tracing::warn!("NCErr client disappeared: {}:{}", to, sid);
                    }
                }
                NewBroadcastReq(chan, send, recv) => {
                    if let Some(c_chan) = self.broadcasts.get(&chan) {
                        c_chan.send(BroadcastChanEvent::New(send, recv));
                    } else {
                        let (inner, sender) = BroadcastChanRef::new(chan.clone(), self.sender.clone(), send, recv);
                        let driver = BroadcastChanDriver::new(inner.clone());
                        tokio::spawn(async move { driver.await });
                        let bchan = BroadcastChan { inner, sender };
                        self.broadcasts.insert(chan, bchan);
                    };
                }
                BroadcastCountReq(chan, from, sid) => {
                    if let Some(c_chan) = self.broadcasts.get(&chan) {
                        c_chan.send(BroadcastChanEvent::Count(from, sid));
                    } else if let Some(c_from) = self.clients.get(&from) {
                        c_from.send(CoordChanEvent::BCErr(sid));
                    } else {
                        tracing::warn!("BCReq client disappeared: {}:{} -> {}", from, sid, chan);
                    }
                }
                BroadcastCountRes(to, sid, counts) => {
                    if let Some(c_to) = self.clients.get(&to) {
                        c_to.send(CoordChanEvent::BCRes(sid, counts));
                    } else {
                        tracing::warn!("BCRes client disappeared: {}:{}", to, sid);
                    }
                }
            };
            accepted += 1;
            if accepted >= MAX_LOOPS {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn run_driver(&mut self, cx: &mut Context) -> Result<(), CoordError> {
        let mut iters = 0;
        loop {
            let mut keep_going = false;
            keep_going |= self.drive_accept(cx)?;
            keep_going |= self.handle_events(cx)?;
            if !keep_going {
                break;
            }
            iters += 1;
            if iters >= MAX_LOOPS {
                // break to let other threads run, but reschedule
                cx.waker().wake_by_ref();
                break;
            }
        }
        Ok(())
    }
}

def_ref!(CoordInner, CoordRef, pub(self));
impl CoordRef {
    fn new(incoming: Incoming) -> Self {
        let (sender, events) = mpsc::unbounded();
        Self(Arc::new(Mutex::new(CoordInner {
            incoming,
            clients: HashMap::new(),
            broadcasts: HashMap::new(),
            ref_count: 0,
            driver: None,
            sender,
            events,
        })))
    }
}

def_driver!(pub(self), CoordRef; pub(self), CoordDriver; CoordError);
impl Drop for CoordDriver {
    fn drop(&mut self) {
        let mut inner = self.0.lock().unwrap();
        // fire sale everything must go
        inner.clients.clear();
        inner.sender.close_channel();
        inner.events.close();
    }
}

/// Main coordinator object
///
/// See [library documentation](../index.html) for an example of constructing a Coord.
pub struct Coord {
    endpoint: Endpoint,
    inner: CoordRef,
    driver_handle: JoinHandle<Result<(), CoordError>>,
}

impl Coord {
    pub(crate) fn build_config(
        stateless_retry: bool,
        keylog: bool,
        certs: CertificateChain,
        key: PrivateKey,
        client_ca: Option<Certificate>,
    ) -> Result<ServerConfig, TLSError> {
        let mut qscb = ServerConfigBuilder::new({
            let mut qsc = ServerConfig::default();
            qsc.crypto = tls::build_rustls_server_config(client_ca.map(|c| c.as_der().to_vec()));
            qsc
        });
        qscb.protocols(ALPN_CONEC);
        qscb.use_stateless_retry(stateless_retry);
        if keylog {
            qscb.enable_keylog();
        }
        qscb.certificate(certs, key)?;
        Ok(qscb.build())
    }

    /// Construct a new coordinator and listen for Clients
    pub async fn new(config: CoordConfig) -> Result<Self, CoordError> {
        // build configuration
        let (cert, key) = config.cert_and_key;
        let qsc = Self::build_config(config.stateless_retry, config.keylog, cert, key, config.client_ca)?;

        // build QUIC endpoint
        let mut endpoint = Endpoint::builder();
        endpoint.listen(qsc);
        let (endpoint, incoming) = endpoint.bind(&config.laddr)?;

        let inner = CoordRef::new(incoming);
        let driver = CoordDriver(inner.clone());
        let driver_handle = tokio::spawn(async move { driver.await });

        Ok(Self {
            endpoint,
            inner,
            driver_handle,
        })
    }

    /// Report number of connected clients
    pub fn num_clients(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.clients.len()
    }

    /// Report number of active broadcast channels
    pub fn num_broadcasts(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.broadcasts.len()
    }

    /// Return the local address that Coord is bound to
    pub fn local_addr(&self) -> std::net::SocketAddr {
        // unwrap is safe because Coord always has a bound socket
        self.endpoint.local_addr().unwrap()
    }
}

def_flat_future!(Coord, (), CoordError, Join, driver_handle);
