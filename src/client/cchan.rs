// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use super::ichan::IncomingChannelsEvent;
use super::istream::{IncomingStreamsInner, NewInStream, StreamId};
use super::{ConnectingOutStreamHandle, OutStreamError};
use crate::consts::{MAX_LOOPS, STRICT_CTRL};
use crate::types::{outstream_init, ConecConn, ControlMsg, CtrlStream};
use crate::util;

use err_derive::Error;
use futures::{channel::mpsc, prelude::*};
use quinn::{ConnectionError, IncomingBiStreams};
use std::collections::{HashSet, VecDeque};
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use tokio::time::{interval, Duration, Interval};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

/// Client-client channel driver errors
#[derive(Debug, Error)]
pub enum ClientClientChanError {
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
    /// Keepalive timer disappeared unexpectedly
    #[error(display = "Keepalive timer disappered unexpectedly")]
    KeepaliveTimer,
    /// Local receiver closed
    #[error(display = "Local receiver closed")]
    ReceiverClosed,
    /// Transport unexpectedly stopped delivering new streams
    #[error(display = "Unexpected end of Bi stream")]
    EndOfBiStream,
    /// Error while accepting new stream from transport
    #[error(display = "Accepting Bi stream: {:?}", _0)]
    AcceptBiStream(#[source] ConnectionError),
}
def_into_error!(ClientClientChanError);

pub(super) struct ClientClientChanInner {
    conn: ConecConn,
    ctrl: CtrlStream,
    ibi: IncomingBiStreams,
    id: String,
    peer: String,
    sender: mpsc::UnboundedSender<NewInStream>,
    ichan: mpsc::UnboundedSender<IncomingChannelsEvent>,
    ref_count: usize,
    driver: Option<Waker>,
    to_send: VecDeque<ControlMsg>,
    flushing: bool,
    keepalive: Option<Interval>,
    sids: HashSet<u64>,
}

impl ClientClientChanInner {
    fn handle_events(&mut self, cx: &mut Context) -> Result<(), ClientClientChanError> {
        match self.sender.poll_ready(cx) {
            Poll::Ready(Err(_)) => Err(ClientClientChanError::ReceiverClosed),
            _ => Ok(()),
        }?;

        match self.keepalive.as_mut().map_or(Poll::Pending, |k| k.poll_next_unpin(cx)) {
            Poll::Pending => Ok(()),
            Poll::Ready(None) => Err(ClientClientChanError::KeepaliveTimer),
            Poll::Ready(Some(_)) => {
                self.to_send.push_back(ControlMsg::KeepAlive);
                Ok(())
            }
        }
    }

    fn drive_streams_recv(&mut self, cx: &mut Context) -> Result<bool, ClientClientChanError> {
        let mut recvd = 0;
        loop {
            let (send, recv) = match self.ibi.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => Err(ClientClientChanError::EndOfBiStream),
                Poll::Ready(Some(r)) => r.map_err(ClientClientChanError::AcceptBiStream),
            }?;
            IncomingStreamsInner::instream_init(send, recv, self.sender.clone(), StreamId::Direct);
            recvd += 1;
            if recvd >= MAX_LOOPS {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn drive_ctrl_recv(&mut self, cx: &mut Context) -> Result<bool, ClientClientChanError> {
        let mut recvd = 0;
        loop {
            match self.ctrl.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => Err(ClientClientChanError::PeerClosed),
                Poll::Ready(Some(Err(e))) => Err(ClientClientChanError::StreamPoll(e)),
                Poll::Ready(Some(Ok(msg))) => match msg {
                    ControlMsg::KeepAlive | ControlMsg::CoHello => Ok(()),
                    _ => {
                        let err = ClientClientChanError::WrongMessage(msg);
                        if STRICT_CTRL {
                            Err(err)
                        } else {
                            tracing::warn!("ClientClientChanInner::drive_ctrl_recv: {:?}", err);
                            Ok(())
                        }
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

    fn drive_ctrl_send(&mut self, cx: &mut Context) -> Result<bool, ClientClientChanError> {
        util::drive_ctrl_send(cx, &mut self.flushing, &mut self.ctrl, &mut self.to_send)
            .map_err(ClientClientChanError::Sink)
    }

    fn run_driver(&mut self, cx: &mut Context) -> Result<(), ClientClientChanError> {
        loop {
            let mut keep_going = false;
            self.handle_events(cx)?;
            keep_going |= self.drive_streams_recv(cx)?;
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

def_ref!(ClientClientChanInner, ClientClientChanRef);
impl ClientClientChanRef {
    pub(super) fn new(
        conn: ConecConn,
        ctrl: CtrlStream,
        ibi: IncomingBiStreams,
        id: String,
        peer: String,
        sender: mpsc::UnboundedSender<NewInStream>,
        ichan: mpsc::UnboundedSender<IncomingChannelsEvent>,
    ) -> Self {
        let mut to_send = VecDeque::new();
        // send hello at startup
        to_send.push_back(ControlMsg::CoHello);
        Self(Arc::new(Mutex::new(ClientClientChanInner {
            conn,
            ctrl,
            ibi,
            id,
            peer,
            sender,
            ichan,
            ref_count: 0,
            driver: None,
            to_send,
            flushing: false,
            keepalive: None,
            sids: HashSet::new(),
        })))
    }
}

def_driver!(pub(self), ClientClientChanRef; pub(super), ClientClientChanDriver; ClientClientChanError);
impl ClientClientChanDriver {
    pub(super) fn new(inner: ClientClientChanRef, keepalive: bool) -> Self {
        if keepalive {
            let inner_locked = &mut inner.lock().unwrap();
            inner_locked.keepalive.replace(interval(Duration::new(6, 666666666)));
        }
        Self(inner)
    }
}

impl Drop for ClientClientChanDriver {
    fn drop(&mut self) {
        let mut inner = self.0.lock().unwrap();
        // tell ichan that we're closing now
        inner
            .ichan
            .unbounded_send(IncomingChannelsEvent::ChanClose(inner.peer.clone(), None))
            .ok();

        // take down just our connection
        inner.conn.close(b"chan driver died");
        inner.sender.disconnect();
        inner.ichan.disconnect();
        inner.to_send.clear();
        inner.keepalive.take();
        inner.sids.clear();
    }
}

pub(super) struct ClientClientChan(pub(super) ClientClientChanRef);

impl ClientClientChan {
    // XXX should sid also be unique w.r.t. proxied streams?
    //     maybe: push uniqueness check up into Client?
    pub(super) fn new_stream(&self, sid: u64, handle: ConnectingOutStreamHandle) {
        let mut inner = self.0.lock().unwrap();

        // make sure this stream hasn't already been used
        if inner.sids.contains(&sid) {
            handle.send(Err(OutStreamError::StreamId)).ok();
        } else {
            inner.sids.insert(sid);
            let bi = inner.conn.open_bi();
            let id = inner.id.clone();
            tokio::spawn(async move {
                handle
                    .send(
                        bi.err_into::<OutStreamError>()
                            .and_then(|(send, recv)| async {
                                outstream_init(send, id, sid)
                                    .await
                                    .map(|send| (send, FramedRead::new(recv, LengthDelimitedCodec::new())))
                            })
                            .await,
                    )
                    .ok();
            });
        }
    }
}
