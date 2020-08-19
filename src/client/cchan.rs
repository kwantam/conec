// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use super::istream::{IncomingStreamsInner, NewInStream};
use crate::consts::{MAX_LOOPS, STRICT_CTRL};
use crate::types::{
    outstream_init, ConecConn, ConnectingOutStream, ControlMsg,
    CtrlStream, OutStreamError,
};
use crate::util;

use err_derive::Error;
use futures::channel::{mpsc, oneshot};
use futures::prelude::*;
use quinn::{ConnectionError, IncomingBiStreams};
use std::collections::{HashSet, VecDeque};
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use tokio::time::{interval, Duration, Interval};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

///! Client-client channel driver errors
#[derive(Debug, Error)]
pub enum ClientClientChanError {
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
    ///! Keepalive timer disappeared unexpectedly
    #[error(display = "Keepalive timer disappered unexpectedly")]
    KeepaliveTimer,
    ///! Local receiver closed
    #[error(display = "Local receiver closed")]
    ReceiverClosed,
    ///! Transport unexpectedly stopped delivering new streams
    #[error(display = "Unexpected end of Bi stream")]
    EndOfBiStream,
    ///! Error while accepting new stream from transport
    #[error(display = "Accepting Bi stream: {:?}", _0)]
    AcceptBiStream(#[source] ConnectionError),
}

/// Client-to-client channel
pub(super) struct ClientClientChan {
    pub(super) conn: ConecConn,
    pub(super) ctrl: CtrlStream,
    pub(super) ibi: IncomingBiStreams,
    pub(super) peer: String,
}

pub(super) struct ClientClientChanInner {
    conn: ConecConn,
    ctrl: CtrlStream,
    ibi: IncomingBiStreams,
    id: String,
    sender: mpsc::UnboundedSender<NewInStream>,
    //bye_out: Option<oneshot::Sender<()>>,
    //bye_in: oneshot::Receiver<()>,
    ref_count: usize,
    driver: Option<Waker>,
    to_send: VecDeque<ControlMsg>,
    flushing: bool,
    keepalive: Option<Interval>,
    sids: HashSet<u32>,
}

impl ClientClientChanInner {
    fn handle_events(&mut self, cx: &mut Context) -> Result<(), ClientClientChanError> {
        match self.sender.poll_ready(cx) {
            Poll::Ready(Err(_)) => Err(ClientClientChanError::ReceiverClosed),
            _ => Ok(()),
        }?;

        match self
            .keepalive
            .as_mut()
            .map_or(Poll::Pending, |k| k.poll_next_unpin(cx))
        {
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
            IncomingStreamsInner::stream_init(send, recv, self.sender.clone());
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
                    ControlMsg::KeepAlive => Ok(()),
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
        sender: mpsc::UnboundedSender<NewInStream>,
    ) -> Self {
        Self(Arc::new(Mutex::new(ClientClientChanInner {
            conn,
            ctrl,
            ibi,
            id,
            sender,
            ref_count: 0,
            driver: None,
            to_send: VecDeque::new(),
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
            inner_locked
                .keepalive
                .replace(interval(Duration::new(6, 666666666)));
        }
        Self(inner)
    }
}

pub(super) struct CClientClientChan(pub(super) ClientClientChanRef);

impl CClientClientChan {
    // XXX sid should also be unique w.r.t. proxied streams!!!
    //     maybe: push uniqueness check up into Client?
    pub(super) fn new_stream(&self, sid: u32) -> ConnectingOutStream {
        let (sender, receiver) = oneshot::channel();
        let inner = &mut *self.0.lock().unwrap();

        // make sure this stream hasn't already been used
        if inner.sids.contains(&sid) {
            sender.send(Err(OutStreamError::StreamId)).ok();
        } else {
            inner.sids.insert(sid);
            let bi = inner.conn.open_bi();
            let id = inner.id.clone();
            tokio::spawn(async move {
                let (send, recv) = match bi
                    .err_into::<OutStreamError>()
                    .and_then(|(send, recv)| async move {
                        outstream_init(send, Some(id), sid)
                            .await
                            .map(|send| (send, FramedRead::new(recv, LengthDelimitedCodec::new())))
                    })
                    .await
                {
                    Err(e) => {
                        sender.send(Err(e)).ok();
                        return;
                    }
                    Ok(send_recv) => send_recv,
                };
                sender.send(Ok((send, recv))).ok();
            });
        }

        ConnectingOutStream(receiver)
    }
}
