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

mod chan;
pub(crate) mod config;
mod tls;

use crate::consts::{ALPN_CONEC, MAX_LOOPS};
use crate::types::{ConecConn, ConecConnError, ControlMsg, CtrlStream};
use chan::{CoordChan, CoordChanDriver, CoordChanEvent, CoordChanRef};
use config::CoordConfig;

use err_derive::Error;
use futures::channel::mpsc;
use futures::prelude::*;
use quinn::{
    crypto::rustls::TLSError, CertificateChain, ConnectionError, Endpoint, EndpointError, Incoming,
    IncomingBiStreams, PrivateKey, RecvStream, SendStream, ServerConfig, ServerConfigBuilder,
};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use tokio::task::{JoinError, JoinHandle};

///! Coordinator constructor and driver errors
#[derive(Debug, Error)]
pub enum CoordError {
    ///! Transport's incoming connections stream ended
    #[error(display = "Unexpected end of Incoming stream")]
    EndOfIncomingStream,
    ///! Error accepting new connection
    #[error(display = "Connection error: {:?}", _0)]
    Connect(#[source] ConnectionError),
    ///! Error connecting control channel to new Client
    #[error(display = "Error connecting control channel: {:?}", _0)]
    Control(#[source] ConecConnError),
    ///! Error setting up certificate chain
    #[error(display = "Certificate: {:?}", _0)]
    CertificateChain(#[source] TLSError),
    ///! Error binding port for transport
    #[error(display = "Binding port: {:?}", _0)]
    Bind(#[source] EndpointError),
    ///! Error from JoinHandle future
    #[error(display = "Join eror: {:?}", _0)]
    Join(#[source] JoinError),
}

enum CoordEvent {
    Accepted(ConecConn, CtrlStream, IncomingBiStreams, String),
    AcceptError(CoordError),
    ChanClose(String),
    NewStreamReq(String, String, u32),
    NewStreamRes(
        String,
        u32,
        Result<(SendStream, RecvStream), ConnectionError>,
    ),
}

struct CoordInner {
    incoming: Incoming,
    clients: HashMap<String, CoordChan>,
    driver: Option<Waker>,
    ref_count: usize,
    sender: mpsc::UnboundedSender<CoordEvent>,
    events: mpsc::UnboundedReceiver<CoordEvent>,
}

impl CoordInner {
    /// try to accept a new connection from a client
    fn drive_accept(&mut self, cx: &mut Context) -> Result<bool, CoordError> {
        let mut accepted = 0;
        loop {
            match self.incoming.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => Err(CoordError::EndOfIncomingStream),
                Poll::Ready(Some(incoming)) => {
                    let sender = self.sender.clone();
                    tokio::spawn(async move {
                        use CoordError::*;
                        use CoordEvent::*;
                        let (mut conn, ibi) = match incoming.await {
                            Err(e) => {
                                sender.unbounded_send(AcceptError(Connect(e))).unwrap();
                                return;
                            }
                            Ok(conn) => ConecConn::new(conn),
                        };
                        let (ctrl, peer) = match conn.connect_ctrl().await {
                            Err(e) => {
                                sender.unbounded_send(AcceptError(Control(e))).unwrap();
                                return;
                            }
                            Ok(ctrl_peer) => ctrl_peer,
                        };
                        sender
                            .unbounded_send(Accepted(conn, ctrl, ibi, peer))
                            .unwrap();
                    });
                    Ok(())
                }
            }?;
            accepted += 1;
            if accepted >= MAX_LOOPS {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// handle events arriving on self.events
    fn handle_events(&mut self, cx: &mut Context) -> bool {
        use CoordEvent::*;
        let mut accepted = 0;
        loop {
            match self.events.poll_next_unpin(cx) {
                Poll::Ready(Some(event)) => match event {
                    AcceptError(e) => {
                        tracing::warn!("got AcceptError: {}", e);
                    }
                    Accepted(conn, ctrl, ibi, peer) => {
                        if self.clients.get(&peer).is_some() {
                            tokio::spawn(async move {
                                let mut ctrl = ctrl;
                                ctrl.send(ControlMsg::HelloError("name in use".to_string()))
                                    .await
                                    .ok();
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
                            );

                            // spawn channel driver
                            let driver = CoordChanDriver(inner.clone());
                            tokio::spawn(async move { driver.await });

                            self.clients.insert(peer, CoordChan { inner, sender });
                        }
                    }
                    ChanClose(client) => {
                        // client channel closed --- drop it from the queue
                        self.clients.remove(&client);
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
                },
                Poll::Ready(None) => unreachable!("CoordInner owns a sender; something is wrong"),
                Poll::Pending => break,
            }
            accepted += 1;
            if accepted >= MAX_LOOPS {
                return true;
            }
        }
        false
    }
}

// a shared reference to a Coordinator
struct CoordRef(Arc<Mutex<CoordInner>>);

impl CoordRef {
    fn new(incoming: Incoming) -> Self {
        let (sender, events) = mpsc::unbounded();
        Self(Arc::new(Mutex::new(CoordInner {
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

#[must_use = "CoordDriver must be spawned!"]
struct CoordDriver(CoordRef);

impl Future for CoordDriver {
    type Output = Result<(), CoordError>;

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
            keep_going |= inner.handle_events(cx);
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

///! Main coordinator object
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
    ) -> Result<ServerConfig, TLSError> {
        let mut qscb = ServerConfigBuilder::new({
            let mut qsc = ServerConfig::default();
            qsc.crypto = tls::build_rustls_server_config();
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

    ///! Construct a new coordinator and listen for Clients
    pub async fn new(config: CoordConfig) -> Result<Self, CoordError> {
        // build configuration
        let (cert, key) = config.cert_and_key;
        let qsc = Self::build_config(config.stateless_retry, config.keylog, cert, key)?;

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

    ///! Report number of connected clients
    pub fn num_clients(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.clients.len()
    }

    ///! Return the local address that Coord is bound to
    pub fn local_addr(&self) -> std::net::SocketAddr {
        // unwrap is safe because Coord always has a bound socket
        self.endpoint.local_addr().unwrap()
    }
}

impl Future for Coord {
    type Output = Result<(), CoordError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match self.driver_handle.poll_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(CoordError::Join(e))),
            Poll::Ready(Ok(res)) => Poll::Ready(res),
        }
    }
}
