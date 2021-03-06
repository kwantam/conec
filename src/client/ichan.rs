// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use super::cchan::{ClientClientChan, ClientClientChanDriver, ClientClientChanRef};
use super::chan::ClientChanEvent;
use super::NewInStream;
use super::{ConnectingStream, ConnectingStreamError, ConnectingStreamHandle};
use crate::consts::MAX_LOOPS;
use crate::types::{ConecConn, ConecConnError, ControlMsg, CtrlStream};

use err_derive::Error;
use futures::{
    channel::{mpsc, oneshot},
    prelude::*,
};
use quinn::{Certificate, ClientConfig, ConnectionError, Endpoint, Incoming, IncomingBiStreams};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

def_cs_future!(
    ClosingChannel,
    ClosingChannelHandle,
    (),
    ClosingChannelError,
    doc = "A channel that is being closed"
);
/// Error variant output by [ClosingChannel]
#[derive(Debug, Error)]
pub enum ClosingChannelError {
    /// Tried to close a channel that doesn't exist
    #[error(display = "Nonexistent channel")]
    Nonexistent,
    /// New channel was canceled
    #[error(display = "Canceled: {:?}", _0)]
    Canceled(#[source] oneshot::Canceled),
    /// Error injecting event
    #[error(display = "Could not send event")]
    Event,
}
def_into_error!(ClosingChannelError);

/// Error variant output by the IncomingChannels driver
#[derive(Debug, Error)]
pub enum IncomingChannelsError {
    /// Incoming channels receiver disappeared
    #[error(display = "IncomingChannels receiver is gone")]
    ReceiverClosed,
    /// Transport unexpectedly stopped delivering new channels
    #[error(display = "Unexpected end of Incoming stream")]
    EndOfIncomingStream,
    /// Client's connection to Coordinator disappeared
    #[error(display = "Client is gone")]
    ClientClosed,
    /// Error accepting new connection
    #[error(display = "Connection error: {:?}", _0)]
    Connect(#[source] ConnectionError),
    /// Error connecting control channel to new Client
    #[error(display = "Error connecting control channel: {:?}", _0)]
    Control(#[source] ConecConnError),
    /// Error handing off result to driver
    #[error(display = "Error sending to driver: {:?}", _0)]
    Driver(#[source] mpsc::SendError),
    /// Events channel closed
    #[error(display = "Events channel closed")]
    EventsClosed,
}
def_into_error!(IncomingChannelsError);

/// Error variant when opening a new channel
#[derive(Debug, Error)]
pub enum NewChannelError {
    /// Could not parse certificate
    #[error(display = "Parsing certificate: {:?}", _0)]
    CertificateParse(#[source] quinn::ParseError),
    /// General certificate error
    #[error(display = "Certificate: {:?}", _0)]
    CertificateAuthority(#[source] webpki::Error),
    /// Certificate doesn't match what Coord told us
    #[error(display = "Certificate mismatch")]
    Certificate,
    /// Connecting failed
    #[error(display = "Connecting to peer: {:?}", _0)]
    Connecting(#[source] ConecConnError),
    /// Driver handoff failure before connecting
    #[error(display = "Handing off pre-connection")]
    DriverPre,
    /// Driver handoff failure after connecting
    #[error(display = "Handing off post-connection")]
    DriverPost,
    /// New channel was canceled
    #[error(display = "Canceled: {:?}", _0)]
    Canceled(#[source] oneshot::Canceled),
    /// Configuration error
    #[error(display = "Configuration error: no ichan driver")]
    Config,
    /// Coordinator returned error
    #[error(display = "Coordinator returned error")]
    Coord,
    /// Reused channel id
    #[error(display = "Reused channel id")]
    ChannelId,
    /// Channel already open to this peer
    #[error(display = "Duplicate channel peer")]
    Duplicate,
    /// Error injecting event
    #[error(display = "Could not send event")]
    Event,
}
def_into_error!(NewChannelError);

def_cs_future!(
    ConnectingChannel,
    pub(crate),
    ConnectingChannelHandle,
    pub(self),
    (),
    NewChannelError,
    doc = "A direct channel to a Client that is currently connecting"
);

pub(super) enum IncomingChannelsEvent {
    Certificate(String, Vec<u8>),
    Accepted(ConecConn, CtrlStream, IncomingBiStreams, String),
    Connected(
        ConecConn,
        CtrlStream,
        IncomingBiStreams,
        String,
        ConnectingChannelHandle,
    ),
    ChanClose(String, Option<ClosingChannelHandle>),
    NewChannel(String, SocketAddr, Vec<u8>, ConnectingChannelHandle),
    NewStream(String, u64, ConnectingStreamHandle),
    ChanInit(String, u64, ConnectingChannelHandle),
}

enum ChanHandle {
    Connecting(Vec<IncomingChannelsEvent>),
    Connected(ClientClientChan),
}

pub(super) struct IncomingChannelsInner {
    endpoint: Endpoint,
    id: String,
    keepalive: bool,
    channels: Incoming,
    ref_count: usize,
    driver: Option<Waker>,
    certs: HashMap<String, Vec<u8>>,
    chans: HashMap<String, ChanHandle>,
    strm_out: mpsc::UnboundedSender<NewInStream>,
    sender: mpsc::UnboundedSender<IncomingChannelsEvent>,
    events: mpsc::UnboundedReceiver<IncomingChannelsEvent>,
    client_config: ClientConfig,
    client_ca: Option<Certificate>,
    chan_sender: mpsc::UnboundedSender<ClientChanEvent>,
}

impl IncomingChannelsInner {
    fn drive_accept(&mut self, cx: &mut Context) -> Result<bool, IncomingChannelsError> {
        let mut recvd = 0;
        loop {
            let conn = match self.channels.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => Err(IncomingChannelsError::EndOfIncomingStream),
                Poll::Ready(Some(conn)) => Ok(conn),
            }?;
            let sender = self.sender.clone();
            tokio::spawn(async move {
                use IncomingChannelsError::*;
                if let Err(e) = async {
                    let (mut conn, mut ibi) = conn.await.map_err(Connect).map(ConecConn::new)?;
                    let (ctrl, peer) = conn.accept_ctrl(&mut ibi).await.map_err(Control)?;
                    sender
                        .unbounded_send(IncomingChannelsEvent::Accepted(conn, ctrl, ibi, peer))
                        .map_err(|e| Driver(e.into_send_error()))
                }
                .await
                {
                    tracing::warn!("ichan drive_accept: {:?}", e);
                }
            });
            recvd += 1;
            if recvd >= MAX_LOOPS {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn handle_events(&mut self, cx: &mut Context) -> Result<bool, IncomingChannelsError> {
        use IncomingChannelsEvent::*;
        let mut recvd = 0;
        loop {
            let event = match self.events.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => Err(IncomingChannelsError::EventsClosed),
                Poll::Ready(Some(event)) => Ok(event),
            }?;
            match event {
                Certificate(peer, cert) => {
                    self.certs.insert(peer, cert);
                }
                Accepted(conn, ctrl, ibi, peer) => {
                    // check that cert client gave us is consistent with what Coord told us
                    match (self.chans.get(&peer).is_some(), self.certs.get(&peer)) {
                        (false, Some(cert)) if &cert[..] == conn.get_cert_bytes() => {
                            let inner = ClientClientChanRef::new(
                                conn,
                                ctrl,
                                ibi,
                                self.id.clone(),
                                peer.clone(),
                                self.strm_out.clone(),
                                self.sender.clone(),
                            );
                            let driver = ClientClientChanDriver::new(inner.clone(), self.keepalive);
                            tokio::spawn(async move { driver.await });
                            self.chans.insert(peer, ChanHandle::Connected(ClientClientChan(inner)));
                        }
                        (dup, _) => {
                            tokio::spawn(async move {
                                let mut ctrl = ctrl;
                                let errmsg = if dup {
                                    ControlMsg::HelloError("duplicate channel".to_string())
                                } else {
                                    ControlMsg::HelloError("cert mismatch".to_string())
                                };
                                ctrl.send(errmsg).await.ok();
                                ctrl.finish().await.ok();
                                drop(ctrl);
                                drop(conn);
                            });
                        }
                    }
                }
                Connected(conn, ctrl, ibi, peer, handle) => {
                    let inner = ClientClientChanRef::new(
                        conn,
                        ctrl,
                        ibi,
                        self.id.clone(),
                        peer.clone(),
                        self.strm_out.clone(),
                        self.sender.clone(),
                    );
                    let driver = ClientClientChanDriver::new(inner.clone(), self.keepalive);
                    tokio::spawn(async move { driver.await });
                    handle.send(Ok(())).ok();

                    // save off channel and process any waiting events
                    use ChanHandle::*;
                    match self.chans.insert(peer, Connected(ClientClientChan(inner))) {
                        None => panic!("lost Connecting(..) struct"),
                        Some(Connected(_)) => panic!("got cchan Connecting(..) after already connected"),
                        Some(Connecting(mut events)) => {
                            for event in events.drain(..) {
                                self.sender
                                    .unbounded_send(event)
                                    .map_err(|e| {
                                        if let NewStream(_, _, handle) = e.into_inner() {
                                            handle.send(Err(ConnectingStreamError::Event)).ok();
                                        } else {
                                            unreachable!()
                                        }
                                    })
                                    .ok();
                            }
                        }
                    }
                }
                ChanClose(peer, handle) => {
                    let res = self.chans.remove(&peer);
                    if let Some(handle) = handle {
                        match res {
                            None => handle.send(Err(ClosingChannelError::Nonexistent)).ok(),
                            Some(_) => handle.send(Ok(())).ok(),
                        };
                    }
                }
                ChanInit(peer, _, handle) if self.chans.get(&peer).is_some() => {
                    handle.send(Err(NewChannelError::Duplicate)).ok();
                }
                ChanInit(peer, sid, handle) => {
                    self.chan_sender
                        .unbounded_send(ClientChanEvent::Channel(peer, sid, handle))
                        .map_err(|e| {
                            if let ClientChanEvent::Channel(_, _, handle) = e.into_inner() {
                                handle.send(Err(NewChannelError::Event)).ok();
                            } else {
                                unreachable!()
                            }
                        })
                        .ok();
                }
                NewChannel(peer, _, _, handle) if self.chans.get(&peer).is_some() => {
                    handle.send(Err(NewChannelError::Duplicate)).ok();
                }
                NewChannel(peer, addr, cert, handle) => {
                    // record this connection as in-progress
                    self.chans.insert(peer.clone(), ChanHandle::Connecting(Vec::new()));

                    // now actually connect
                    let mut endpoint = self.endpoint.clone();
                    let mut cfg = self.client_config.clone();
                    let trusted_cert = match self.client_ca.clone() {
                        Some(cert) => Ok(cert),
                        None => quinn::Certificate::from_der(&cert[..]),
                    };
                    let id = self.id.clone();
                    let sender = self.sender.clone();
                    tokio::spawn(async move {
                        match async {
                            // set up new config and connect
                            cfg.add_certificate_authority(trusted_cert?)?;
                            let (mut conn, ibi) =
                                ConecConn::connect(&mut endpoint, &peer, addr.into(), Some(cfg)).await?;

                            // connect control stream and check certs
                            let mut ctrl = conn.connect_ctrl(id).await?;
                            conn.read_cert_bytes();
                            if conn.get_cert_bytes() != &cert[..] {
                                ctrl.send(ControlMsg::HelloError("cert mismatch".to_string()))
                                    .await
                                    .ok();
                                ctrl.finish().await.ok();
                                return Err(NewChannelError::Certificate);
                            }

                            Ok((conn, ctrl, ibi))
                        }
                        .await
                        {
                            Err(e) => {
                                sender.unbounded_send(ChanClose(peer, None)).ok();
                                handle.send(Err(e)).ok();
                            }
                            Ok((conn, ctrl, ibi)) => {
                                sender
                                    .unbounded_send(Connected(conn, ctrl, ibi, peer, handle))
                                    .map_err(|e| {
                                        if let Connected(_, _, _, _, handle) = e.into_inner() {
                                            handle.send(Err(NewChannelError::DriverPost)).ok();
                                        } else {
                                            unreachable!()
                                        }
                                    })
                                    .ok();
                            }
                        };
                    });
                }
                NewStream(peer, sid, handle) => {
                    match self.chans.get_mut(&peer) {
                        Some(ChanHandle::Connected(chan)) => {
                            chan.new_stream(sid, handle);
                        }
                        Some(ChanHandle::Connecting(queue)) => {
                            queue.push(NewStream(peer, sid, handle));
                        }
                        _ => {
                            handle.send(Err(ConnectingStreamError::NoSuchPeer(peer))).ok();
                        }
                    };
                }
            };
            recvd += 1;
            if recvd >= MAX_LOOPS {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn run_driver(&mut self, cx: &mut Context) -> Result<(), IncomingChannelsError> {
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

def_ref!(IncomingChannelsInner, IncomingChannelsRef);
impl IncomingChannelsRef {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        endpoint: Endpoint,
        id: String,
        keepalive: bool,
        channels: Incoming,
        strm_out: mpsc::UnboundedSender<NewInStream>,
        client_config: ClientConfig,
        client_ca: Option<Certificate>,
        chan_sender: mpsc::UnboundedSender<ClientChanEvent>,
    ) -> (Self, mpsc::UnboundedSender<IncomingChannelsEvent>) {
        let (sender, events) = mpsc::unbounded();
        (
            Self(Arc::new(Mutex::new(IncomingChannelsInner {
                endpoint,
                id,
                keepalive,
                channels,
                ref_count: 0,
                driver: None,
                certs: HashMap::new(),
                chans: HashMap::new(),
                strm_out,
                sender: sender.clone(),
                events,
                client_config,
                client_ca,
                chan_sender,
            }))),
            sender,
        )
    }
}

def_driver!(IncomingChannelsRef, IncomingChannelsDriver, IncomingChannelsError);
impl Drop for IncomingChannelsDriver {
    fn drop(&mut self) {
        // if the driver dies, it takes everything with it
        let mut inner = self.0.lock().unwrap();
        inner.certs.clear();
        inner.chans.clear();
        inner.strm_out.disconnect();
        inner.sender.close_channel();
        inner.events.close();
    }
}

pub(super) struct IncomingChannels {
    #[allow(dead_code)]
    inner: IncomingChannelsRef,
    sender: mpsc::UnboundedSender<IncomingChannelsEvent>,
}

impl IncomingChannels {
    pub(super) fn new(inner: IncomingChannelsRef, sender: mpsc::UnboundedSender<IncomingChannelsEvent>) -> Self {
        Self { inner, sender }
    }

    pub(super) fn new_channel(&self, to: String, sid: u64) -> ConnectingChannel {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .unbounded_send(IncomingChannelsEvent::ChanInit(to, sid, sender))
            .map_err(|e| {
                if let IncomingChannelsEvent::ChanInit(_, _, sender) = e.into_inner() {
                    sender.send(Err(NewChannelError::Event)).ok();
                } else {
                    unreachable!()
                }
            })
            .ok();

        ConnectingChannel(receiver)
    }

    pub(super) fn new_stream(&self, to: String, sid: u64) -> ConnectingStream {
        use IncomingChannelsEvent::NewStream;
        let (res, sender) = ConnectingStream::new(None);
        self.sender
            .unbounded_send(NewStream(to, sid, sender.unwrap())) // unwrap is safe because we called new(None)
            .map_err(|e| {
                if let NewStream(_, _, sender) = e.into_inner() {
                    sender.send(Err(ConnectingStreamError::Event)).ok();
                } else {
                    unreachable!()
                }
            })
            .ok();

        res
    }

    pub(super) fn close_channel(&self, peer: String) -> ClosingChannel {
        let (sender, receiver) = oneshot::channel();
        use IncomingChannelsEvent::ChanClose;
        self.sender
            .unbounded_send(ChanClose(peer, Some(sender)))
            .map_err(|e| {
                if let ChanClose(_, Some(sender)) = e.into_inner() {
                    sender.send(Err(ClosingChannelError::Event)).ok();
                } else {
                    unreachable!()
                }
            })
            .ok();

        ClosingChannel(receiver)
    }

    pub(super) fn get_sender(&self) -> mpsc::UnboundedSender<IncomingChannelsEvent> {
        self.sender.clone()
    }
}
