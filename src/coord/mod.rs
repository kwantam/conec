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
use crate::types::{
    tagstream::TaggedInStream, ConecConn, ConecConnError, ConnectingOutStream, ConnectingOutStreamHandle,
    ControlMsg, CtrlStream, OutStream, OutStreamError,
};
use bchan::{BroadcastChan, BroadcastChanDriver, BroadcastChanRef};
use chan::{CoordChan, CoordChanDriver, CoordChanEvent, CoordChanRef};
pub use chan::{CoordChanError, NewInStream};
use config::CoordConfig;

use err_derive::Error;
use futures::{
    channel::{mpsc, oneshot},
    prelude::*,
};
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
    NewCoStream(String, u32, ConnectingOutStreamHandle),
    NewStreamReq(String, String, u32),
    NewStreamRes(String, u32, Result<(SendStream, RecvStream), ConnectionError>),
    NewChannelReq(String, String, u32, Vec<u8>),
    NewChannelRes(String, u32, SocketAddr, Vec<u8>),
    NewChannelErr(String, u32),
    NewBroadcastReq(String, OutStream, TaggedInStream),
}

struct CoordInner {
    incoming: Incoming,
    clients: HashMap<String, CoordChan>,
    broadcasts: HashMap<String, BroadcastChan>,
    driver: Option<Waker>,
    ref_count: usize,
    sender: mpsc::UnboundedSender<CoordEvent>,
    events: mpsc::UnboundedReceiver<CoordEvent>,
    is_sender: mpsc::UnboundedSender<NewInStream>,
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
                        let (inner, sender) = CoordChanRef::new(
                            conn,
                            ctrl,
                            ibi,
                            peer.clone(),
                            self.sender.clone(),
                            self.is_sender.clone(),
                        );

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
                NewCoStream(to, sid, handle) => {
                    if let Some(c_to) = self.clients.get(&to) {
                        c_to.send(CoordChanEvent::BiOut(sid, handle));
                    } else {
                        handle.send(Err(OutStreamError::NoSuchPeer(to))).ok();
                    }
                }
                NewChannelReq(from, to, sid, cert) => {
                    if let Some(c_to) = self.clients.get(&to) {
                        c_to.send(CoordChanEvent::NCReq(from, sid, cert));
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
                        c_chan.new_broadcast(send, recv);
                    } else {
                        let (inner, sender) = BroadcastChanRef::new(chan.clone(), self.sender.clone(), send, recv);
                        let driver = BroadcastChanDriver(inner.clone());
                        tokio::spawn(async move { driver.await });
                        let bchan = BroadcastChan { inner, sender };
                        self.broadcasts.insert(chan, bchan);
                    };
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
        loop {
            let mut keep_going = false;
            keep_going |= self.drive_accept(cx)?;
            keep_going |= self.handle_events(cx)?;
            if !keep_going {
                return Ok(());
            }
        }
    }
}

def_ref!(CoordInner, CoordRef, pub(self));
impl CoordRef {
    fn new(incoming: Incoming) -> (Self, IncomingStreams, mpsc::UnboundedSender<CoordEvent>) {
        let (sender, events) = mpsc::unbounded();
        let (is_sender, incoming_streams) = mpsc::unbounded();
        (
            Self(Arc::new(Mutex::new(CoordInner {
                incoming,
                clients: HashMap::new(),
                broadcasts: HashMap::new(),
                driver: None,
                ref_count: 0,
                sender: sender.clone(),
                events,
                is_sender,
            }))),
            incoming_streams,
            sender,
        )
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
        inner.is_sender.close_channel();
    }
}

/// A [Stream] of incoming data streams from Client or Coordinator.
///
/// See [library documentation](../index.html) for a usage example.
pub type IncomingStreams = mpsc::UnboundedReceiver<NewInStream>;

/// Main coordinator object
///
/// See [library documentation](../index.html) for an example of constructing a Coord.
pub struct Coord {
    endpoint: Endpoint,
    inner: CoordRef,
    sender: mpsc::UnboundedSender<CoordEvent>,
    driver_handle: JoinHandle<Result<(), CoordError>>,
    ctr: u32,
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
    pub async fn new(config: CoordConfig) -> Result<(Self, IncomingStreams), CoordError> {
        // build configuration
        let (cert, key) = config.cert_and_key;
        let qsc = Self::build_config(config.stateless_retry, config.keylog, cert, key, config.client_ca)?;

        // build QUIC endpoint
        let mut endpoint = Endpoint::builder();
        endpoint.listen(qsc);
        let (endpoint, incoming) = endpoint.bind(&config.laddr)?;

        let (inner, incoming_streams, sender) = CoordRef::new(incoming);
        let driver = CoordDriver(inner.clone());
        let driver_handle = tokio::spawn(async move { driver.await });

        Ok((
            Self {
                endpoint,
                inner,
                sender,
                driver_handle,
                ctr: 1u32 << 31,
            },
            incoming_streams,
        ))
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

    /// Open a new stream to a Client
    pub fn new_stream(&mut self, to: String) -> ConnectingOutStream {
        let ctr = self.ctr;
        self.ctr += 1;
        self.new_stream_with_id(to, ctr)
    }

    /// Open a new stream to a Client with an explicit stream-id
    ///
    /// The `sid` argument must be different for every call to this function for a given Client.
    /// If mixing calls to this function with calls to [new_stream](Coord::new_stream), avoid using
    /// `sid >= 1<<31`: those values are used automatically by that function.
    pub fn new_stream_with_id(&self, to: String, sid: u32) -> ConnectingOutStream {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .unbounded_send(CoordEvent::NewCoStream(to, sid, sender))
            .map_err(|e| {
                if let CoordEvent::NewCoStream(_, _, sender) = e.into_inner() {
                    sender.send(Err(OutStreamError::Event)).ok();
                } else {
                    unreachable!();
                }
            })
            .ok();
        ConnectingOutStream(receiver)
    }
}

def_flat_future!(Coord, (), CoordError, Join, driver_handle);
