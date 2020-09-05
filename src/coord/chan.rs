// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use super::CoordEvent;
use crate::consts::{MAX_LOOPS, STRICT_CTRL};
use crate::types::{
    outstream_init, tagstream::TaggedInStream, ConecConn, ConnectingOutStreamHandle, ControlMsg, CtrlStream,
    InStream, OutStream, OutStreamError, StreamPeer, StreamTo,
};
use crate::util;

use err_derive::Error;
use futures::{channel::mpsc, prelude::*};
use quinn::{ConnectionError, IncomingBiStreams, RecvStream, SendStream};
use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use tokio_serde::{formats::SymmetricalBincode, SymmetricallyFramed};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

/// Output by [IncomingStreams](super::IncomingStreams)
pub type NewInStream = (String, u32, OutStream, InStream);

/// Coordinator channel driver errors
#[derive(Debug, Error)]
pub enum CoordChanError {
    /// Received a request for an unknown stream-id
    #[error(display = "Unknown streamid")]
    UnknownSid,
    /// Peer closed connection
    #[error(display = "Peer closed connection")]
    PeerClosed,
    /// Polling the control channel failed
    #[error(display = "Stream poll: {:?}", _0)]
    StreamPoll(#[error(source, no_from)] io::Error),
    /// Writing to the control channel failed
    #[error(display = "Control sink: {:?}", _0)]
    Sink(#[error(source, no_from)] util::SinkError),
    /// Client sent an unexpected message
    #[error(display = "Unexpected message from coordinator")]
    WrongMessage(ControlMsg),
    /// Writing to master Coord driver failed
    #[error(display = "Sending CoordEvent: {:?}", _0)]
    SendCoordEvent(#[source] mpsc::SendError),
    /// Transport unexpectedly stopped delivering new streams
    #[error(display = "Unexpected end of Bi stream")]
    EndOfBiStream,
    /// Error while accepting new stream from transport
    #[error(display = "Accepting Bi stream: {:?}", _0)]
    AcceptBiStream(#[source] ConnectionError),
    /// Error while opening new transport stream
    #[error(display = "Opening Bi stream: {:?}", _0)]
    OpenBiStream(#[error(no_from, source)] ConnectionError),
    /// Events channel closed
    #[error(display = "Events channel closed")]
    EventsClosed,
}

pub(super) struct CoordChanInner {
    conn: ConecConn,
    ctrl: CtrlStream,
    ibi: IncomingBiStreams,
    peer: String,
    coord: mpsc::UnboundedSender<CoordEvent>,
    sender: mpsc::UnboundedSender<CoordChanEvent>,
    events: mpsc::UnboundedReceiver<CoordChanEvent>,
    ref_count: usize,
    driver: Option<Waker>,
    to_send: VecDeque<ControlMsg>,
    flushing: bool,
    new_streams: HashMap<u32, (SendStream, RecvStream)>,
    new_broadcasts: HashMap<u32, String>,
    is_sender: mpsc::UnboundedSender<NewInStream>,
    sids: HashSet<u32>,
}

#[allow(clippy::large_enum_variant)]
pub(super) enum CoordChanEvent {
    NSErr(u32),
    NSReq(String, u32),
    NSRes(u32, Result<(SendStream, RecvStream), ConnectionError>),
    NCErr(u32),
    NCReq(String, u32, Vec<u8>),
    NCRes(u32, SocketAddr, Vec<u8>),
    BiIn(StreamTo, OutStream, InStream),
    BiOut(u32, ConnectingOutStreamHandle),
}

impl CoordChanInner {
    // read the next message from the recv channel
    fn drive_ctrl_recv(&mut self, cx: &mut Context) -> Result<bool, CoordChanError> {
        let mut recvd = 0;
        loop {
            let msg = match self.ctrl.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => Err(CoordChanError::PeerClosed),
                Poll::Ready(Some(Err(e))) => Err(CoordChanError::StreamPoll(e)),
                Poll::Ready(Some(Ok(msg))) => Ok(msg),
            }?;
            match msg {
                ControlMsg::NewStreamReq(to, sid) => self
                    .coord
                    .unbounded_send(CoordEvent::NewStreamReq(self.peer.clone(), to, sid))
                    .map_err(|e| CoordChanError::SendCoordEvent(e.into_send_error())),
                ControlMsg::NewChannelReq(to, sid) => self
                    .coord
                    .unbounded_send(CoordEvent::NewChannelReq(
                        self.peer.clone(),
                        to,
                        sid,
                        self.conn.get_cert_bytes().to_vec(),
                    ))
                    .map_err(|e| CoordChanError::SendCoordEvent(e.into_send_error())),
                ControlMsg::CertOk(to, sid) => {
                    let addr = self.conn.remote_addr();
                    let cert = self.conn.get_cert_bytes().to_vec();
                    self.coord
                        .unbounded_send(CoordEvent::NewChannelRes(to, sid, addr, cert))
                        .map_err(|e| CoordChanError::SendCoordEvent(e.into_send_error()))
                }
                ControlMsg::CertNok(to, sid) => self
                    .coord
                    .unbounded_send(CoordEvent::NewChannelErr(to, sid))
                    .map_err(|e| CoordChanError::SendCoordEvent(e.into_send_error())),
                ControlMsg::NewBroadcastReq(chan, sid) => {
                    self.to_send.push_back(ControlMsg::NewBroadcastOk(sid));
                    self.new_broadcasts.insert(sid, chan);
                    Ok(())
                }
                ControlMsg::KeepAlive => {
                    self.to_send.push_back(ControlMsg::KeepAlive);
                    Ok(())
                }
                _ => {
                    let err = CoordChanError::WrongMessage(msg);
                    if STRICT_CTRL {
                        Err(err)
                    } else {
                        tracing::warn!("CoordChanInner::drive_ctrl_recv: {:?}", err);
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

    // send something on the send channel
    fn drive_ctrl_send(&mut self, cx: &mut Context) -> Result<bool, CoordChanError> {
        util::drive_ctrl_send(cx, &mut self.flushing, &mut self.ctrl, &mut self.to_send)
            .map_err(CoordChanError::Sink)
    }

    fn handle_events(&mut self, cx: &mut Context) -> Result<bool, CoordChanError> {
        use CoordChanEvent::*;
        let mut accepted = 0;
        loop {
            let event = match self.events.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => Err(CoordChanError::EventsClosed),
                Poll::Ready(Some(event)) => Ok(event),
            }?;
            match event {
                NSErr(sid) => self.to_send.push_back(ControlMsg::NewStreamErr(sid)),
                NSRes(sid, res) => match res {
                    Err(_) => {
                        self.to_send.push_back(ControlMsg::NewStreamErr(sid));
                    }
                    Ok(send_recv) => {
                        self.to_send.push_back(ControlMsg::NewStreamOk(sid));
                        self.new_streams.insert(sid, send_recv);
                    }
                },
                NSReq(to, sid) => {
                    let coord = self.coord.clone();
                    let bi = self.conn.open_bi();
                    tokio::spawn(async move {
                        coord.unbounded_send(CoordEvent::NewStreamRes(to, sid, bi.await)).ok();
                    });
                }
                NCErr(sid) => self.to_send.push_back(ControlMsg::NewChannelErr(sid)),
                NCReq(to, sid, cert) => self.to_send.push_back(ControlMsg::CertReq(to, sid, cert)),
                NCRes(sid, addr, cert) => self.to_send.push_back(ControlMsg::NewChannelOk(sid, addr, cert)),
                BiIn(sid, n_send, n_recv) => match sid {
                    StreamTo::Broadcast(sid) => {
                        let tagged_recv = TaggedInStream::new(n_recv, self.peer.clone());
                        let res = self
                            .new_broadcasts
                            .remove(&sid)
                            .ok_or(CoordChanError::UnknownSid)
                            .and_then(|chan| {
                                self.coord
                                    .unbounded_send(CoordEvent::NewBroadcastReq(chan, n_send, tagged_recv))
                                    .map_err(|e| CoordChanError::SendCoordEvent(e.into_send_error()))
                            });
                        if res.is_err() {
                            self.to_send.push_back(ControlMsg::NewBroadcastErr(sid));
                        }
                    }
                    StreamTo::Client(sid) => {
                        if let Some((send, recv)) = self.new_streams.remove(&sid) {
                            let from = self.peer.clone();
                            tokio::spawn(async move {
                                let send = match outstream_init(send, StreamPeer::Client(from), sid).await {
                                    Ok(send) => send,
                                    Err(e) => {
                                        tracing::warn!("CoordChan::handle_events: BiIn: Client: {:?}", e);
                                        return;
                                    }
                                };
                                let recv = FramedRead::new(recv, LengthDelimitedCodec::new());

                                // forward all messages in both directions
                                let fw1 = n_recv.map(|b| b.map(|bb| bb.freeze())).forward(send);
                                let fw2 = recv.map(|b| b.map(|bb| bb.freeze())).forward(n_send);
                                let (sf, rf) = futures::future::join(fw1, fw2).await;
                                sf.ok();
                                rf.ok();
                            });
                        } else {
                            self.to_send.push_back(ControlMsg::NewStreamErr(sid));
                        }
                    }
                    StreamTo::Coord(sid) => {
                        self.is_sender
                            .unbounded_send((self.peer.clone(), sid, n_send, n_recv))
                            .ok();
                    }
                },
                BiOut(sid, handle) => {
                    if self.sids.contains(&sid) {
                        handle.send(Err(OutStreamError::StreamId)).ok();
                    } else {
                        self.sids.insert(sid);
                        let bi = self.conn.open_bi();
                        tokio::spawn(async move {
                            handle
                                .send(
                                    bi.err_into::<OutStreamError>()
                                        .and_then(|(send, recv)| async {
                                            outstream_init(send, StreamPeer::Coord, sid).await.map(|send| {
                                                (send, FramedRead::new(recv, LengthDelimitedCodec::new()))
                                            })
                                        })
                                        .await,
                                )
                                .ok();
                        });
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

    fn drive_ibi_recv(&mut self, cx: &mut Context) -> Result<bool, CoordChanError> {
        let mut recvd = 0;
        loop {
            let (send, recv) = match self.ibi.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => Err(CoordChanError::EndOfBiStream),
                Poll::Ready(Some(r)) => r.map_err(CoordChanError::AcceptBiStream),
            }?;
            let sender = self.sender.clone();
            tokio::spawn(async move {
                let mut read_stream = SymmetricallyFramed::new(
                    FramedRead::new(recv, LengthDelimitedCodec::new()),
                    SymmetricalBincode::<StreamTo>::default(),
                );
                let sid = match read_stream.try_next().await {
                    Err(e) => {
                        tracing::warn!("drive_ibi_recv: {:?}", e);
                        return;
                    }
                    Ok(None) => {
                        tracing::warn!("drive_ibi_recv: unexpected end of stream");
                        return;
                    }
                    Ok(Some(sid)) => sid,
                };
                let recv = read_stream.into_inner();
                let send = FramedWrite::new(send, LengthDelimitedCodec::new());
                sender.unbounded_send(CoordChanEvent::BiIn(sid, send, recv)).ok();
            });
            recvd += 1;
            if recvd >= MAX_LOOPS {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn run_driver(&mut self, cx: &mut Context) -> Result<(), CoordChanError> {
        loop {
            let mut keep_going = false;
            keep_going |= self.drive_ctrl_recv(cx)?;
            keep_going |= self.handle_events(cx)?;
            if !self.to_send.is_empty() || self.flushing {
                keep_going |= self.drive_ctrl_send(cx)?;
            }
            keep_going |= self.drive_ibi_recv(cx)?;
            if !keep_going {
                return Ok(());
            }
        }
    }
}

def_ref!(CoordChanInner, CoordChanRef);
impl CoordChanRef {
    pub(super) fn new(
        conn: ConecConn,
        ctrl: CtrlStream,
        ibi: IncomingBiStreams,
        peer: String,
        coord: mpsc::UnboundedSender<CoordEvent>,
        is_sender: mpsc::UnboundedSender<NewInStream>,
    ) -> (Self, mpsc::UnboundedSender<CoordChanEvent>) {
        let mut to_send = VecDeque::new();
        // send hello at startup
        to_send.push_back(ControlMsg::CoHello);
        let (sender, events) = mpsc::unbounded();
        (
            Self(Arc::new(Mutex::new(CoordChanInner {
                conn,
                ctrl,
                ibi,
                peer,
                coord,
                sender: sender.clone(),
                events,
                ref_count: 0,
                driver: None,
                to_send,
                flushing: false,
                new_streams: HashMap::new(),
                new_broadcasts: HashMap::new(),
                is_sender,
                sids: HashSet::new(),
            }))),
            sender,
        )
    }
}

def_driver!(CoordChanRef, CoordChanDriver, CoordChanError);
impl Drop for CoordChanDriver {
    fn drop(&mut self) {
        let mut inner = self.0.lock().unwrap();
        // tell the coordinator that this channel is dead
        inner
            .coord
            .unbounded_send(CoordEvent::ChanClose(inner.peer.clone()))
            .ok();

        // just take down this channel, don't stop the whole world
        inner.conn.close(b"coord driver died");
        inner.coord.disconnect();
        inner.sender.close_channel();
        inner.events.close();
        inner.to_send.clear();
        inner.new_streams.clear();
        inner.is_sender.disconnect();
        inner.sids.clear();
    }
}

pub(super) struct CoordChan {
    #[allow(dead_code)]
    pub(super) inner: CoordChanRef,
    pub(super) sender: mpsc::UnboundedSender<CoordChanEvent>,
}

impl CoordChan {
    pub(super) fn send(&self, event: CoordChanEvent) {
        // ignore errors: can't be full because it's unbounded,
        // and if the receiver disappeared then the driver must
        // have sent a ChanClose event to the Coord
        self.sender.unbounded_send(event).ok();
    }
}
