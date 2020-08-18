// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use crate::consts::MAX_LOOPS;
use crate::types::{ConecConn, ConecConnError, CtrlStream};

use err_derive::Error;
use futures::channel::{mpsc, oneshot};
use futures::prelude::*;
use quinn::{ConnectionError, Incoming, IncomingBiStreams};
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

/// Client-to-client channel
pub struct ClientClientChan {
    conn: ConecConn,
    ctrl: CtrlStream,
    ibi: IncomingBiStreams,
    peer: String,
}

#[allow(clippy::large_enum_variant)]
enum IncomingChannelsEvent {
    Certificate(String, Vec<u8>),
    Accepted(ConecConn, CtrlStream, IncomingBiStreams, String),
}

pub(super) struct IncomingChannelsInner {
    client: Option<oneshot::Sender<()>>,
    bye: oneshot::Receiver<()>,
    channels: Incoming,
    ref_count: usize,
    driver: Option<Waker>,
    certs: HashMap<String, Vec<u8>>,
    chan_out: mpsc::UnboundedSender<ClientClientChan>,
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

        match self.chan_out.poll_ready(cx) {
            Poll::Ready(Err(_)) => Err(IncomingChannelsError::ReceiverClosed),
            _ => Ok(()),
        }?;

        match self.bye.poll_unpin(cx) {
            Poll::Pending => Ok(()),
            _ => Err(IncomingChannelsError::ClientClosed),
        }?;

        let mut recvd = 0;
        loop {
            match self.events.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => unreachable!("we own a sender"),
                Poll::Ready(Some(event)) => match event {
                    Certificate(peer, cert) => {
                        self.certs.insert(peer, cert);
                    }
                    Accepted(conn, ctrl, ibi, peer) => {
                        // loop up cert and make sure it's OK
                    }
                }
            }
            recvd += 1;
            if recvd >= MAX_LOOPS {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

pub(super) struct IncomingChannelsRef(Arc<Mutex<IncomingChannelsInner>>);

impl std::ops::Deref for IncomingChannelsRef {
    type Target = Mutex<IncomingChannelsInner>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Clone for IncomingChannelsRef {
    fn clone(&self) -> Self {
        self.lock().unwrap().ref_count += 1;
        Self(self.0.clone())
    }
}

impl Drop for IncomingChannelsRef {
    fn drop(&mut self) {
        let inner = &mut *self.lock().unwrap();
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

impl IncomingChannelsRef {
    pub(super) fn new(
        client: oneshot::Sender<()>,
        bye: oneshot::Receiver<()>,
        channels: Incoming,
        chan_out: mpsc::UnboundedSender<ClientClientChan>,
    ) -> Self {
        let (sender, events) = mpsc::unbounded();
        Self(Arc::new(Mutex::new(IncomingChannelsInner {
            client: Some(client),
            bye,
            channels,
            ref_count: 0,
            driver: None,
            certs: HashMap::new(),
            chan_out,
            sender,
            events,
        })))
    }
}

#[must_use = "IncomingChannelsDriver must be spawned!"]
pub(super) struct IncomingChannelsDriver(pub(super) IncomingChannelsRef);

impl Future for IncomingChannelsDriver {
    type Output = Result<(), IncomingChannelsError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let inner = &mut *self.0.lock().unwrap();
        match &inner.driver {
            Some(w) if w.will_wake(cx.waker()) => (),
            _ => inner.driver = Some(cx.waker().clone()),
        };
        loop {
            let mut keep_going = false;
            keep_going |= inner.drive_accept(cx)?;
            keep_going |= inner.handle_events(cx)?;
            if !keep_going {
                break;
            }
        }
        if inner.ref_count == 0 {
            // I think we're alone now
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

impl Drop for IncomingChannelsDriver {
    fn drop(&mut self) {
        let mut inner = self.0.lock().unwrap();
        // tell client we died
        inner.client.take().unwrap().send(()).ok();
    }
}
