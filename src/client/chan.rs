// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use super::ichan::{IncomingChannelsEvent, NewChannelError};
use super::{ConnectingStream, ConnectingStreamError, ConnectingStreamHandle, HolepunchEvent};
use crate::consts::{MAX_LOOPS, STRICT_CTRL};
use crate::types::{ConecConn, ControlMsg, CtrlStream, StreamTo};
use crate::util;

use err_derive::Error;
use futures::{
    channel::{mpsc, oneshot},
    prelude::*,
};
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
}
def_into_error!(ClientChanError);

def_cs_future!(
    ConnectingChannel,
    pub(crate),
    ConnectingChannelHandle,
    pub(self),
    (),
    NewChannelError,
    doc = "A direct channel to a Client that is currently connecting"
);

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
}

impl ClientChanInner {
    fn wake(&mut self) {
        if let Some(task) = self.driver.take() {
            task.wake();
        }
    }

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

    fn handle_events(&mut self, cx: &mut Context) -> Result<(), ClientChanError> {
        match self.keepalive.as_mut().map_or(Poll::Pending, |k| k.poll_next_unpin(cx)) {
            Poll::Pending => Ok(()),
            Poll::Ready(None) => Err(ClientChanError::KeepaliveTimer),
            Poll::Ready(Some(_)) => {
                self.to_send.push_back(ControlMsg::KeepAlive);
                Ok(())
            }
        }
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
            self.handle_events(cx)?;
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
    }
}

pub(super) struct ClientChan(pub(super) ClientChanRef);

// XXX should we do this asynchronously via a channel instead?
//     lock contention on a client channel seems like it should be low
impl ClientChan {
    pub(super) fn new_stream(&self, to: String, sid: u64) -> ConnectingStream {
        // the new stream future is a channel that will contain the resulting stream
        let (sender, receiver) = oneshot::channel();
        let mut inner = self.0.lock().unwrap();

        // make sure this stream hasn't already been used
        if inner.new_streams.get(&sid).is_some() {
            sender.send(Err(ConnectingStreamError::StreamId)).ok();
        } else {
            inner.to_send.push_back(ControlMsg::NewStreamReq(to, sid));
            inner.new_streams.insert(sid, Some(sender));
            inner.wake();
        }

        ConnectingStream(receiver)
    }

    pub(super) fn new_channel(&self, to: String, sid: u64) -> ConnectingChannel {
        // future that will return the result from the coordinator
        let (sender, receiver) = oneshot::channel();
        let mut inner = self.0.lock().unwrap();

        if inner.new_channels.get(&sid).is_some() {
            sender.send(Err(NewChannelError::ChannelId)).ok();
        } else {
            inner.to_send.push_back(ControlMsg::NewChannelReq(to.clone(), sid));
            inner.new_channels.insert(sid, Some((to, sender)));
            inner.wake();
        }

        ConnectingChannel(receiver)
    }

    pub(super) fn new_broadcast(&self, chan: String, sid: u64) -> ConnectingStream {
        let (sender, receiver) = oneshot::channel();
        let mut inner = self.0.lock().unwrap();

        // make sure this stream hasn't already been used
        if inner.new_streams.get(&sid).is_some() {
            sender.send(Err(ConnectingStreamError::StreamId)).ok();
        } else {
            inner.to_send.push_back(ControlMsg::NewBroadcastReq(chan, sid));
            inner.new_streams.insert(sid, Some(sender));
            inner.wake();
        }

        ConnectingStream(receiver)
    }
}
