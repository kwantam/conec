// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use super::ClientChanError;
use crate::consts::{MAX_LOOPS, STRICT_CTRL};
use crate::types::{ConecConn, ConecConnError, ControlMsg, CtrlStream};
use crate::util;

use err_derive::Error;
use futures::channel::{mpsc, oneshot};
use futures::prelude::*;
use quinn::{ConnectionError, Incoming, IncomingBiStreams};
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use tokio::time::{interval, Duration, Interval};

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
    sender: mpsc::UnboundedSender<ClientEvent>,
    //bye_out: Option<oneshot::Sender<()>>,
    //bye_in: oneshot::Receiver<()>,
    ref_count: usize,
    driver: Option<Waker>,
    to_send: VecDeque<ControlMsg>,
    flushing: bool,
    keepalive: Option<Interval>,
}

impl ClientClientChanInner {
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

    fn drive_ctrl_recv(&mut self, cx: &mut Context) -> Result<bool, ClientChanError> {
        let mut recvd = 0;
        loop {
            match self.ctrl.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => Err(ClientChanError::PeerClosed),
                Poll::Ready(Some(Err(e))) => Err(ClientChanError::StreamPoll(e)),
                Poll::Ready(Some(Ok(msg))) => match msg {
                    ControlMsg::KeepAlive => Ok(()),
                    _ => {
                        let err = ClientChanError::WrongMessage(msg);
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

    fn drive_ctrl_send(&mut self, cx: &mut Context) -> Result<bool, ClientChanError> {
        util::drive_ctrl_send(cx, &mut self.flushing, &mut self.ctrl, &mut self.to_send)
            .map_err(ClientChanError::Sink)
    }

    fn run_driver(&mut self, cx: &mut Context) -> Result<bool, ClientChanError> {
        let mut keep_going = false;
        self.handle_events(cx)?;
        //keep_going |= self.drive_accept(cx)?;
        keep_going |= self.drive_ctrl_recv(cx)?;
        if !self.to_send.is_empty() || self.flushing {
            keep_going |= self.drive_ctrl_send(cx)?;
        }
        Ok(keep_going)
    }
}

pub(super) type ClientEvent = ();

def_ref!(ClientClientChanInner, ClientClientChanRef);
impl ClientClientChanRef {
    pub(super) fn new(
        conn: ConecConn,
        ctrl: CtrlStream,
        ibi: IncomingBiStreams,
        sender: mpsc::UnboundedSender<ClientEvent>,
    ) -> Self {
        Self(Arc::new(Mutex::new(ClientClientChanInner {
            conn,
            ctrl,
            ibi,
            sender,
            ref_count: 0,
            driver: None,
            to_send: VecDeque::new(),
            flushing: false,
            keepalive: None,
        })))
    }
}

def_driver!(pub(self), ClientClientChanRef; pub(super), ClientClientChanDriver; ClientChanError);
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
