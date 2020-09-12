// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use super::ichan::{ConnectingChannelHandle, IncomingChannelsEvent, NewChannelError};
use super::{ConnectingStream, ConnectingStreamError, ConnectingStreamHandle, HolepunchEvent};
use crate::consts::{MAX_LOOPS, STRICT_CTRL};
use crate::types::{ConecConn, ControlMsg, CtrlStream, StreamTo};
use crate::util;

use err_derive::Error;
use futures::{channel::mpsc, prelude::*};
use std::collections::{HashMap, VecDeque};
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use tokio::time::{interval, Duration, Interval};
use tokio_serde::{formats::SymmetricalBincode, SymmetricallyFramed};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

/// Client channel driver errors
#[derive(Debug, Error)]
pub enum ClientChanError {
    /// Peer closed connection
    #[error(display = "Peer closed connection")]
    PeerClosed,
    /// Polling the control channel failed
    #[error(display = "Stream poll: {:?}", _0)]
    StreamPoll(#[error(source, no_from)] io::Error),
    /// Writing to the control channel failed
    #[error(display = "Control sink: {:?}", _0)]
    Sink(#[error(source, no_from)] util::SinkError),
    /// Coordinator sent an unexpected message
    #[error(display = "Unexpected message from coordinator")]
    WrongMessage(ControlMsg),
    /// Coordinator sent us a message about a nonexistent stream-id
    #[error(display = "Coord response about nonexistent strmid {}", _0)]
    NonexistentStrOrCh(u64),
    /// Coordinator sent us a message about a stale stream-id
    #[error(display = "Coord response about stale strmid {}", _0)]
    StaleStrOrCh(u64),
    /// Another client driver died
    #[error(display = "Another client driver died")]
    OtherDriverHup,
    /// Keepalive timer disappeared unexpectedly
    #[error(display = "Keepalive timer disappered unexpectedly")]
    KeepaliveTimer,
    /// Events channel closed
    #[error(display = "Events channel closed")]
    EventsClosed,
}
def_into_error!(ClientChanError);

pub(super) enum ClientChanEvent {
    Stream(String, u64, ConnectingStreamHandle),
    Broadcast(String, u64, ConnectingStreamHandle),
    Channel(String, u64, ConnectingChannelHandle),
}

pub(super) struct ClientChanInner {
    conn: ConecConn,
    ctrl: CtrlStream,
    ref_count: usize,
    driver: Option<Waker>,
    to_send: VecDeque<ControlMsg>,
    new_streams: HashMap<u64, Option<ConnectingStreamHandle>>,
    new_channels: HashMap<u64, Option<(String, ConnectingChannelHandle)>>,
    flushing: bool,
    keepalive: Option<Interval>,
    ichan_sender: mpsc::UnboundedSender<IncomingChannelsEvent>,
    holepunch_sender: Option<mpsc::UnboundedSender<HolepunchEvent>>,
    listen: bool,
    events: mpsc::UnboundedReceiver<ClientChanEvent>,
}

impl ClientChanInner {
    fn new_stream(&mut self, chan: ConnectingStreamHandle, sid: StreamTo) {
        let bi = self.conn.open_bi();
        tokio::spawn(async move {
            chan.send(
                async {
                    // get the new stream
                    let (send, recv) = bi.await.map_err(ConnectingStreamError::OpenBi)?;

                    // write sid to it
                    let mut write_stream = SymmetricallyFramed::new(
                        FramedWrite::new(send, LengthDelimitedCodec::new()),
                        SymmetricalBincode::<StreamTo>::default(),
                    );
                    write_stream.send(sid).await.map_err(ConnectingStreamError::InitMsg)?;
                    write_stream.flush().await.map_err(ConnectingStreamError::Flush)?;

                    // send resulting OutStream and InStream to the receiver
                    let outstream = write_stream.into_inner();
                    let instream = FramedRead::new(recv, LengthDelimitedCodec::new());
                    Ok((outstream, instream))
                }
                .await,
            )
            .ok();
        });
    }

    fn handle_events(&mut self, cx: &mut Context) -> Result<bool, ClientChanError> {
        match self.keepalive.as_mut().map_or(Poll::Pending, |k| k.poll_next_unpin(cx)) {
            Poll::Pending => Ok(()),
            Poll::Ready(None) => Err(ClientChanError::KeepaliveTimer),
            Poll::Ready(Some(_)) => {
                self.to_send.push_back(ControlMsg::KeepAlive);
                Ok(())
            }
        }?;

        use ClientChanEvent::*;
        let mut recvd = 0;
        loop {
            let event = match self.events.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => Err(ClientChanError::EventsClosed),
                Poll::Ready(Some(event)) => Ok(event),
            }?;
            let is_broadcast = matches!(&event, Broadcast(_, _, _));
            match event {
                Stream(peer, sid, handle) | Broadcast(peer, sid, handle) => {
                    if self.new_streams.get(&sid).is_some() {
                        handle.send(Err(ConnectingStreamError::StreamId)).ok();
                    } else {
                        let cons_msg = if is_broadcast {
                            ControlMsg::NewBroadcastReq
                        } else {
                            ControlMsg::NewStreamReq
                        };
                        self.to_send.push_back(cons_msg(peer, sid));
                        self.new_streams.insert(sid, Some(handle));
                    }
                }
                Channel(peer, sid, handle) => {
                    if self.new_channels.get(&sid).is_some() {
                        handle.send(Err(NewChannelError::ChannelId)).ok();
                    } else {
                        self.to_send.push_back(ControlMsg::NewChannelReq(peer.clone(), sid));
                        self.new_channels.insert(sid, Some((peer, handle)));
                    }
                }
            };
            recvd += 1;
            if recvd >= MAX_LOOPS {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn get_new_str_or_ch<T>(sid: u64, hash: &mut HashMap<u64, Option<T>>) -> Result<T, ClientChanError> {
        if let Some(chan) = hash.get_mut(&sid) {
            if let Some(chan) = chan.take() {
                Ok(chan)
            } else {
                Err(ClientChanError::StaleStrOrCh(sid))
            }
        } else {
            Err(ClientChanError::NonexistentStrOrCh(sid))
        }
    }

    fn drive_ctrl_recv(&mut self, cx: &mut Context) -> Result<bool, ClientChanError> {
        use ControlMsg::*;
        let mut recvd = 0;
        loop {
            let msg = match self.ctrl.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => Err(ClientChanError::PeerClosed),
                Poll::Ready(Some(Err(e))) => Err(ClientChanError::StreamPoll(e)),
                Poll::Ready(Some(Ok(msg))) => Ok(msg),
            }?;
            match msg {
                NewStreamOk(sid) | NewBroadcastOk(sid) => {
                    let chan = Self::get_new_str_or_ch(sid, &mut self.new_streams)?;
                    let sid = if let NewStreamOk(_) = msg {
                        StreamTo::Client(sid)
                    } else {
                        StreamTo::Broadcast(sid)
                    };
                    self.new_stream(chan, sid);
                    Ok(())
                }
                NewChannelOk(sid, addr, cert) => {
                    let (peer, chan) = Self::get_new_str_or_ch(sid, &mut self.new_channels)?;
                    self.ichan_sender
                        .unbounded_send(IncomingChannelsEvent::NewChannel(peer, addr, cert, chan))
                        .map_err(|e| {
                            if let IncomingChannelsEvent::NewChannel(_, _, _, chan) = e.into_inner() {
                                chan.send(Err(NewChannelError::DriverPre)).ok();
                            } else {
                                unreachable!();
                            }
                        })
                        .ok();
                    Ok(())
                }
                NewStreamErr(sid) | NewBroadcastErr(sid) => {
                    let chan = Self::get_new_str_or_ch(sid, &mut self.new_streams)?;
                    chan.send(Err(ConnectingStreamError::Coord)).ok();
                    Ok(())
                }
                NewChannelErr(sid) => {
                    let (_, chan) = Self::get_new_str_or_ch(sid, &mut self.new_channels)?;
                    chan.send(Err(NewChannelError::Coord)).ok();
                    Ok(())
                }
                CertReq(peer, sid, cert, addr) => {
                    if self.listen {
                        self.to_send.push_back(CertOk(peer.clone(), sid));
                        if let Some(holepunch_sender) = self.holepunch_sender.as_mut() {
                            holepunch_sender
                                .unbounded_send(addr)
                                .or(Err(ClientChanError::OtherDriverHup))?;
                        }
                        self.ichan_sender
                            .unbounded_send(IncomingChannelsEvent::Certificate(peer, cert))
                            .or(Err(ClientChanError::OtherDriverHup))
                    } else {
                        self.to_send.push_back(CertNok(peer, sid));
                        Ok(())
                    }
                }
                KeepAlive => Ok(()),
                _ => {
                    let err = ClientChanError::WrongMessage(msg);
                    if STRICT_CTRL {
                        Err(err)
                    } else {
                        tracing::warn!("ClientChanInner::drive_ctrl_recv: {:?}", err);
                        Ok(())
                    }
                }
            }?;
            recvd += 1;
            if recvd >= MAX_LOOPS {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn drive_ctrl_send(&mut self, cx: &mut Context) -> Result<bool, ClientChanError> {
        util::drive_ctrl_send(cx, &mut self.flushing, &mut self.ctrl, &mut self.to_send)
            .map_err(ClientChanError::Sink)
    }

    fn run_driver(&mut self, cx: &mut Context) -> Result<(), ClientChanError> {
        loop {
            let mut keep_going = false;
            keep_going |= self.handle_events(cx)?;
            keep_going |= self.drive_ctrl_recv(cx)?;
            if !self.to_send.is_empty() || self.flushing {
                keep_going |= self.drive_ctrl_send(cx)?;
            }
            if !keep_going {
                return Ok(());
            }
        }
    }
}

def_ref!(ClientChanInner, ClientChanRef);
impl ClientChanRef {
    pub(super) fn new(
        conn: ConecConn,
        ctrl: CtrlStream,
        ichan_sender: mpsc::UnboundedSender<IncomingChannelsEvent>,
        holepunch_sender: Option<mpsc::UnboundedSender<HolepunchEvent>>,
        events: mpsc::UnboundedReceiver<ClientChanEvent>,
        listen: bool,
    ) -> Self {
        Self(Arc::new(Mutex::new(ClientChanInner {
            conn,
            ctrl,
            ref_count: 0,
            driver: None,
            to_send: VecDeque::new(),
            new_streams: HashMap::new(),
            new_channels: HashMap::new(),
            flushing: false,
            keepalive: None,
            ichan_sender,
            holepunch_sender,
            listen,
            events,
        })))
    }
}

def_driver!(pub(self), ClientChanRef; pub(super), ClientChanDriver; ClientChanError);
impl ClientChanDriver {
    pub fn new(inner: ClientChanRef, keepalive: bool) -> Self {
        if keepalive {
            let inner_locked = &mut inner.lock().unwrap();
            inner_locked.keepalive.replace(interval(Duration::new(6, 666666666)));
        }
        Self(inner)
    }
}

impl Drop for ClientChanDriver {
    fn drop(&mut self) {
        // if the driver dies, it takes everything with it
        let mut inner = self.0.lock().unwrap();
        inner.conn.close(b"client chan driver died");
        inner.to_send.clear();
        inner.new_streams.clear();
        inner.new_channels.clear();
        inner.keepalive.take();
        inner.ichan_sender.close_channel();
        if let Some(holepunch_sender) = inner.holepunch_sender.take() {
            holepunch_sender.close_channel();
        }
        inner.events.close();
    }
}

pub(super) struct ClientChan {
    #[allow(dead_code)]
    inner: ClientChanRef,
    sender: mpsc::UnboundedSender<ClientChanEvent>,
}

impl ClientChan {
    pub(super) fn new(inner: ClientChanRef, sender: mpsc::UnboundedSender<ClientChanEvent>) -> Self {
        Self { inner, sender }
    }

    pub(super) fn new_stream(&self, to: String, sid: u64) -> ConnectingStream {
        self.new_x(to, sid, ClientChanEvent::Stream)
    }

    pub(super) fn new_broadcast(&self, to: String, sid: u64) -> ConnectingStream {
        self.new_x(to, sid, ClientChanEvent::Broadcast)
    }

    fn new_x<F>(&self, to: String, sid: u64, cons_msg: F) -> ConnectingStream
    where
        F: Fn(String, u64, ConnectingStreamHandle) -> ClientChanEvent,
    {
        use ClientChanEvent::*;
        let (res, sender) = ConnectingStream::new(None);
        self.sender
            .unbounded_send(cons_msg(to, sid, sender.unwrap())) // unwrap is safe because we called new(None)
            .map_err(|e| {
                let sender = match e.into_inner() {
                    Stream(_, _, sender) | Broadcast(_, _, sender) => sender,
                    _ => unreachable!(),
                };
                sender.send(Err(ConnectingStreamError::Event)).ok();
            })
            .ok();

        res
    }

    pub(super) fn get_sender(&self) -> mpsc::UnboundedSender<ClientChanEvent> {
        self.sender.clone()
    }
}
