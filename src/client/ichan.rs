// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use super::cchan::{ClientClientChan, ClientClientChanDriver, ClientClientChanRef};
use super::NewInStream;
use crate::consts::MAX_LOOPS;
use crate::types::{ConecConn, ConecConnError, ControlMsg, CtrlStream};

use err_derive::Error;
use futures::{channel::mpsc, prelude::*};
use quinn::{ConnectionError, Endpoint, Incoming, IncomingBiStreams};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

///! Error variant output by [IncomingChannelsDriver]
#[derive(Debug, Error)]
pub enum IncomingChannelsError {
    ///! Incoming channels receiver disappeared
    #[error(display = "IncomingChannels receiver is gone")]
    ReceiverClosed,
    ///! Transport unexpectedly stopped delivering new channels
    #[error(display = "Unexpected end of Incoming stream")]
    EndOfIncomingStream,
    ///! Client's connection to Coordinator disappeared
    #[error(display = "Client is gone")]
    ClientClosed,
    ///! Error accepting new connection
    #[error(display = "Connection error: {:?}", _0)]
    Connect(#[source] ConnectionError),
    ///! Error connecting control channel to new Client
    #[error(display = "Error connecting control channel: {:?}", _0)]
    Control(#[source] ConecConnError),
}

#[allow(clippy::large_enum_variant)]
pub(super) enum IncomingChannelsEvent {
    Certificate(String, Vec<u8>),
    Accepted(ConecConn, CtrlStream, IncomingBiStreams, String),
    ChanClose(String),
}

pub(super) struct IncomingChannelsInner {
    endpoint: Endpoint,
    id: String,
    keepalive: bool,
    channels: Incoming,
    ref_count: usize,
    driver: Option<Waker>,
    certs: HashMap<String, Vec<u8>>,
    chans: HashMap<String, ClientClientChan>,
    strm_out: mpsc::UnboundedSender<NewInStream>,
    sender: mpsc::UnboundedSender<IncomingChannelsEvent>,
    events: mpsc::UnboundedReceiver<IncomingChannelsEvent>,
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
                let (mut conn, mut ibi) = match conn.await {
                    Err(e) => {
                        tracing::warn!("drive_accept: {:?}", Connect(e));
                        return;
                    }
                    Ok(new_conn) => ConecConn::new(new_conn),
                };
                let (ctrl, peer) = match conn.accept_ctrl(&mut ibi).await {
                    Err(e) => {
                        tracing::warn!("drive_accept: {:?}", Control(e));
                        return;
                    }
                    Ok(ctrl_peer) => ctrl_peer,
                };
                sender
                    .unbounded_send(IncomingChannelsEvent::Accepted(conn, ctrl, ibi, peer))
                    .ok();
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
                Poll::Ready(None) => unreachable!("we own a sender"),
                Poll::Ready(Some(event)) => event,
            };
            match event {
                Certificate(peer, cert) => {
                    self.certs.insert(peer, cert);
                }
                Accepted(conn, ctrl, ibi, peer) => {
                    // check that cert client gave us is consistent with what Coord told us
                    match self.certs.get(&peer) {
                        Some(cert) if &cert[..] == conn.get_cert_bytes() => {
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
                            self.chans.insert(peer, ClientClientChan(inner));
                        }
                        _ => {
                            tokio::spawn(async move {
                                let mut ctrl = ctrl;
                                ctrl.send(ControlMsg::HelloError("cert mismatch".to_string()))
                                    .await
                                    .ok();
                                ctrl.finish().await.ok();
                                drop(ctrl);
                                drop(conn);
                            });
                        }
                    }
                }
                ChanClose(peer) => {
                    self.chans.remove(&peer);
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

def_ref!(IncomingChannelsInner, IncomingChannelsRef);
impl IncomingChannelsRef {
    pub(super) fn new(
        endpoint: Endpoint,
        id: String,
        keepalive: bool,
        channels: Incoming,
        strm_out: mpsc::UnboundedSender<NewInStream>,
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
            }))),
            sender,
        )
    }
}

def_driver!(
    IncomingChannelsRef,
    IncomingChannelsDriver,
    IncomingChannelsError
);
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
