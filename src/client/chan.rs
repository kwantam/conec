// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use super::StreamPeer;
use crate::consts::{MAX_LOOPS, STRICT_CTRL};
use crate::types::{
    ConecConn, ConnectingOutStream, ConnectingOutStreamHandle, ControlMsg, CtrlStream,
    OutStreamError, StreamTo,
};
use crate::util;

use err_derive::Error;
use futures::{channel::oneshot, prelude::*};
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
    ///! Specified stream id was not unique
    #[error(display = "New stream sid must be unique")]
    StreamNameInUse,
    ///! Coordinator sent an unexpected message
    #[error(display = "Unexpected message from coordinator")]
    WrongMessage(ControlMsg),
    ///! Coordinator sent us a message about a nonexistent stream-id
    #[error(display = "Coord response about nonexistent strmid {}", _0)]
    NonexistentStream(u32),
    ///! Coordinator sent us a message about a stale stream-id
    #[error(display = "Coord response about stale strmid {}", _0)]
    StaleStream(u32),
    ///! Incoming stream driver died
    #[error(display = "Incoming driver hung up")]
    IncomingDriverHup,
    ///! Keepalive timer disappeared unexpectedly
    #[error(display = "Keepalive timer disappered unexpectedly")]
    KeepaliveTimer,
}

pub(super) struct ClientChanInner {
    conn: ConecConn,
    ctrl: CtrlStream,
    incs_bye_in: oneshot::Receiver<()>,
    incs_bye_out: Option<oneshot::Sender<()>>,
    ref_count: usize,
    driver: Option<Waker>,
    to_send: VecDeque<ControlMsg>,
    new_streams: HashMap<u32, Option<ConnectingOutStreamHandle>>,
    flushing: bool,
    keepalive: Option<Interval>,
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
        match self.keepalive.as_mut().map_or(Poll::Pending, |k| k.poll_next_unpin(cx)) {
            Poll::Pending => Ok(()),
            Poll::Ready(None) => Err(ClientChanError::KeepaliveTimer),
            Poll::Ready(Some(_)) => {
                self.to_send.push_back(ControlMsg::KeepAlive);
                Ok(())
            }
        }?;
        match self.incs_bye_in.poll_unpin(cx) {
            Poll::Pending => Ok(()),
            _ => Err(ClientChanError::IncomingDriverHup),
        }?;
        Ok(())
    }

    fn get_new_stream(&mut self, sid: u32) -> Result<ConnectingOutStreamHandle, ClientChanError> {
        if let Some(chan) = self.new_streams.get_mut(&sid) {
            if let Some(chan) = chan.take() {
                Ok(chan)
            } else {
                Err(ClientChanError::StaleStream(sid))
            }
        } else {
            Err(ClientChanError::NonexistentStream(sid))
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
                    let chan = self.get_new_stream(sid)?;
                    let sid = StreamTo::Client(sid);
                    self.new_stream(chan, sid);
                    Ok(())
                }
                ControlMsg::NewStreamErr(sid) => {
                    let chan = self.get_new_stream(sid)?;
                    chan.send(Err(OutStreamError::Coord)).ok();
                    Ok(())
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
                },
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
}

pub(super) struct ClientChanRef(Arc<Mutex<ClientChanInner>>);

impl std::ops::Deref for ClientChanRef {
    type Target = Mutex<ClientChanInner>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Clone for ClientChanRef {
    fn clone(&self) -> Self {
        self.lock().unwrap().ref_count += 1;
        Self(self.0.clone())
    }
}

impl Drop for ClientChanRef {
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

impl ClientChanRef {
    pub(super) fn new(
        conn: ConecConn,
        ctrl: CtrlStream,
    ) -> (Self, oneshot::Sender<()>, oneshot::Receiver<()>) {
        let (i_client, incs_bye_in) = oneshot::channel();
        let (incs_bye_out, i_bye) = oneshot::channel();
        (
            Self(Arc::new(Mutex::new(ClientChanInner {
                conn,
                ctrl,
                incs_bye_in,
                incs_bye_out: Some(incs_bye_out),
                ref_count: 0,
                driver: None,
                to_send: VecDeque::new(),
                new_streams: HashMap::new(),
                flushing: false,
                keepalive: None,
            }))),
            i_client,
            i_bye,
        )
    }
}

#[must_use = "ClientChanDriver must be spawned!"]
pub(super) struct ClientChanDriver(ClientChanRef);

impl ClientChanDriver {
    pub fn new(inner: ClientChanRef, keepalive: bool) -> Self {
        if keepalive {
            let inner_locked = &mut inner.lock().unwrap();
            inner_locked.keepalive.replace(interval(Duration::new(6, 666666666)));
        }
        Self(inner)
    }
}

impl Future for ClientChanDriver {
    type Output = Result<(), ClientChanError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let inner = &mut *self.0.lock().unwrap();
        match &inner.driver {
            Some(w) if w.will_wake(cx.waker()) => (),
            _ => inner.driver = Some(cx.waker().clone()),
        };
        loop {
            let mut keep_going = false;
            inner.handle_events(cx)?;
            keep_going |= inner.drive_ctrl_recv(cx)?;
            if !inner.to_send.is_empty() || inner.flushing {
                keep_going |= inner.drive_ctrl_send(cx)?;
            }
            if !keep_going {
                break;
            }
        }
        if inner.ref_count == 0 {
            // driver is lonely
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

impl Drop for ClientChanDriver {
    fn drop(&mut self) {
        let inner = &mut *self.0.lock().unwrap();
        // tell the incoming stream driver that we died
        inner.incs_bye_out.take().unwrap().send(()).ok();
        inner.keepalive.take();
    }
}

pub(super) struct ClientChan(pub(super) ClientChanRef);

impl ClientChan {
    pub(super) fn new_stream(&self, to: StreamPeer, sid: u32) -> ConnectingOutStream {
        // the new stream future is a channel that will contain the resulting stream
        let (sender, receiver) = oneshot::channel();
        let inner = &mut *self.0.lock().unwrap();

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
}
