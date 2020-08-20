// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use super::{ichan::{IncomingChannelsEvent, NewChannelError}, StreamPeer};
use crate::consts::{MAX_LOOPS, STRICT_CTRL};
use crate::types::{
    ConecConn, ConnectingOutStream, ConnectingOutStreamHandle, ControlMsg, CtrlStream,
    OutStreamError, StreamTo,
};
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

///! Client channel driver errors
#[derive(Debug, Error)]
pub enum ClientChanError {
    ///! Peer closed connection
    #[error(display = "Peer closed connection")]
    PeerClosed,
    ///! Polling the control channel failed
    #[error(display = "Stream poll: {:?}", _0)]
    StreamPoll(#[error(source, no_from)] io::Error),
    ///! Writing to the control channel failed
    #[error(display = "Control sink: {:?}", _0)]
    Sink(#[error(source, no_from)] util::SinkError),
    ///! Coordinator sent an unexpected message
    #[error(display = "Unexpected message from coordinator")]
    WrongMessage(ControlMsg),
    ///! Coordinator sent us a message about a nonexistent stream-id
    #[error(display = "Coord response about nonexistent strmid {}", _0)]
    NonexistentStrOrCh(u32),
    ///! Coordinator sent us a message about a stale stream-id
    #[error(display = "Coord response about stale strmid {}", _0)]
    StaleStrOrCh(u32),
    ///! Another client driver died
    #[error(display = "Another client driver died")]
    OtherDriverHup,
    ///! Keepalive timer disappeared unexpectedly
    #[error(display = "Keepalive timer disappered unexpectedly")]
    KeepaliveTimer,
}

///! A direct channel to a Client that is currently connecting
pub struct ConnectingChannel(oneshot::Receiver<Result<(), NewChannelError>>);
type ConnectingChannelHandle = oneshot::Sender<Result<(), NewChannelError>>;
impl Future for ConnectingChannel {
    type Output = Result<(), NewChannelError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match self.0.poll_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(NewChannelError::Canceled(e))),
            Poll::Ready(Ok(res)) => Poll::Ready(res),
        }
    }
}

pub(super) struct ClientChanInner {
    conn: ConecConn,
    ctrl: CtrlStream,
    ref_count: usize,
    driver: Option<Waker>,
    to_send: VecDeque<ControlMsg>,
    new_streams: HashMap<u32, Option<ConnectingOutStreamHandle>>,
    new_channels: HashMap<u32, Option<(String, ConnectingChannelHandle)>>,
    flushing: bool,
    keepalive: Option<Interval>,
    ichan_sender: Option<mpsc::UnboundedSender<IncomingChannelsEvent>>,
}

impl ClientChanInner {
    fn wake(&mut self) {
        if let Some(task) = self.driver.take() {
            task.wake();
        }
    }

    fn new_stream(&mut self, chan: ConnectingOutStreamHandle, sid: StreamTo) {
        let bi = self.conn.open_bi();
        tokio::spawn(async move {
            // get the new streams
            let (send, recv) = match bi.await {
                Ok(sr) => sr,
                Err(e) => {
                    chan.send(Err(OutStreamError::OpenBi(e))).ok();
                    return;
                }
            };

            // write sid to it
            let mut write_stream = SymmetricallyFramed::new(
                FramedWrite::new(send, LengthDelimitedCodec::new()),
                SymmetricalBincode::<StreamTo>::default(),
            );
            if let Err(e) = write_stream.send(sid).await {
                chan.send(Err(OutStreamError::InitMsg(e))).ok();
                return;
            }
            if let Err(e) = write_stream.flush().await {
                chan.send(Err(OutStreamError::Flush(e))).ok();
                return;
            };

            // send resulting OutStream to the receiver
            let outstream = write_stream.into_inner();
            let instream = FramedRead::new(recv, LengthDelimitedCodec::new());
            chan.send(Ok((outstream, instream))).ok();
        });
    }

    fn handle_events(&mut self, cx: &mut Context) -> Result<(), ClientChanError> {
        match self
            .keepalive
            .as_mut()
            .map_or(Poll::Pending, |k| k.poll_next_unpin(cx))
        {
            Poll::Pending => Ok(()),
            Poll::Ready(None) => Err(ClientChanError::KeepaliveTimer),
            Poll::Ready(Some(_)) => {
                self.to_send.push_back(ControlMsg::KeepAlive);
                Ok(())
            }
        }
    }

    fn get_new_str_or_ch<T>(
        sid: u32,
        hash: &mut HashMap<u32, Option<T>>,
    ) -> Result<T, ClientChanError> {
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
        let mut recvd = 0;
        loop {
            let msg = match self.ctrl.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => Err(ClientChanError::PeerClosed),
                Poll::Ready(Some(Err(e))) => Err(ClientChanError::StreamPoll(e)),
                Poll::Ready(Some(Ok(msg))) => Ok(msg),
            }?;
            match msg {
                ControlMsg::NewStreamOk(sid) => {
                    let chan = Self::get_new_str_or_ch(sid, &mut self.new_streams)?;
                    let sid = StreamTo::Client(sid);
                    self.new_stream(chan, sid);
                    Ok(())
                }
                ControlMsg::NewStreamErr(sid) => {
                    let chan = Self::get_new_str_or_ch(sid, &mut self.new_streams)?;
                    chan.send(Err(OutStreamError::Coord)).ok();
                    Ok(())
                }
                ControlMsg::NewChannelOk(sid, addr, cert) => {
                    let (peer, chan) = Self::get_new_str_or_ch(sid, &mut self.new_channels)?;
                    if let Some(ref sender) = self.ichan_sender {
                        sender.unbounded_send(IncomingChannelsEvent::NewChannel(peer, addr, cert, chan))
                            .map_err(|e| {
                                if let IncomingChannelsEvent::NewChannel(_, _, _, chan) = e.into_inner() {
                                    chan.send(Err(NewChannelError::DriverPre)).ok();
                                } else {
                                    unreachable!();
                                }
                            })
                            .ok();
                    } else {
                        chan.send(Err(NewChannelError::Config)).ok();
                    }
                    Ok(())
                }
                ControlMsg::NewChannelErr(sid) => {
                    let (_, chan) = Self::get_new_str_or_ch(sid, &mut self.new_channels)?;
                    chan.send(Err(NewChannelError::Coord)).ok();
                    Ok(())
                }
                ControlMsg::CertReq(peer, sid, cert) => {
                    if let Some(ref sender) = self.ichan_sender {
                        self.to_send
                            .push_back(ControlMsg::CertOk(peer.clone(), sid));
                        sender
                            .unbounded_send(IncomingChannelsEvent::Certificate(peer, cert))
                            .or(Err(ClientChanError::OtherDriverHup))
                    } else {
                        self.to_send.push_back(ControlMsg::CertNok(peer, sid));
                        Ok(())
                    }
                }
                ControlMsg::KeepAlive => Ok(()),
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
        ichan_sender: Option<mpsc::UnboundedSender<IncomingChannelsEvent>>,
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
        })))
    }
}

def_driver!(pub(self), ClientChanRef; pub(super), ClientChanDriver; ClientChanError);
impl ClientChanDriver {
    pub fn new(inner: ClientChanRef, keepalive: bool) -> Self {
        if keepalive {
            let inner_locked = &mut inner.lock().unwrap();
            inner_locked
                .keepalive
                .replace(interval(Duration::new(6, 666666666)));
        }
        Self(inner)
    }
}

impl Drop for ClientChanDriver {
    fn drop(&mut self) {
        // if the driver dies, it takes everything with it
        let mut inner = self.0.lock().unwrap();
        inner.ctrl.close();
        inner.conn.close(b"client chan driver died");
        inner.to_send.clear();
        inner.new_streams.clear();
        inner.new_channels.clear();
        inner.keepalive.take();
        if let Some(s) = inner.ichan_sender.take() {
            s.close_channel();
        }
    }
}

pub(super) struct ClientChan(pub(super) ClientChanRef);

// XXX should we do this asynchronously via a channel instead?
//     lock contention on a client channel seems like it should be low
impl ClientChan {
    pub(super) fn new_stream(&self, to: StreamPeer, sid: u32) -> ConnectingOutStream {
        // the new stream future is a channel that will contain the resulting stream
        let (sender, receiver) = oneshot::channel();
        let mut inner = self.0.lock().unwrap();

        // make sure this stream hasn't already been used
        if inner.new_streams.get(&sid).is_some() {
            sender.send(Err(OutStreamError::StreamId)).ok();
        } else if to.is_coord() {
            // record that we've used this sid
            inner.new_streams.insert(sid, None);
            // send the coordinator a request and record the send side of the channel
            let sid = StreamTo::Coord(sid);
            inner.new_stream(sender, sid);
        } else {
            inner
                .to_send
                .push_back(ControlMsg::NewStreamReq(to.into_id().unwrap(), sid));
            inner.new_streams.insert(sid, Some(sender));
            inner.wake();
        }

        ConnectingOutStream(receiver)
    }

    pub(super) fn new_channel(&self, to: String, sid: u32) -> ConnectingChannel {
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
}
