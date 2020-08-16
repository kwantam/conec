// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use crate::consts::MAX_LOOPS;
use crate::types::{InStream, OutStream};

use err_derive::Error;
use futures::channel::{mpsc, oneshot};
use futures::prelude::*;
use quinn::{ConnectionError, IncomingBiStreams};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use tokio_serde::{formats::SymmetricalBincode, SymmetricallyFramed};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

///! A [Stream] of incoming data streams from Client or Coordinator.
///
/// See [library documentation](../index.html) for a usage example.
pub type IncomingStreams = mpsc::UnboundedReceiver<NewInStream>;

///! Ok variant output by [ConnectingInStream] future
pub type NewInStream = (Option<String>, u32, OutStream, InStream);

///! Error variant output by [IncomingStreamsDriver]
#[derive(Debug, Error)]
pub enum IncomingStreamsError {
    ///! Transport unexpectedly stopped delivering new streams
    #[error(display = "Unexpected end of Bi stream")]
    EndOfBiStream,
    ///! Client's connection to Coordinator disappeared
    #[error(display = "Client is gone")]
    ClientClosed,
    ///! Incoming streams receiver disappeared
    #[error(display = "IncomingStreams receiver is gone")]
    ReceiverClosed,
    ///! Error while accepting new stream from transport
    #[error(display = "Accepting Bi stream: {:?}", _0)]
    AcceptBiStream(#[source] ConnectionError),
}

pub(super) struct IncomingStreamsInner {
    client: Option<oneshot::Sender<()>>,
    bye: oneshot::Receiver<()>,
    streams: IncomingBiStreams,
    ref_count: usize,
    driver: Option<Waker>,
    sender: mpsc::UnboundedSender<NewInStream>,
}

impl IncomingStreamsInner {
    fn drive_streams_recv(&mut self, cx: &mut Context) -> Result<bool, IncomingStreamsError> {
        match self.sender.poll_ready(cx) {
            Poll::Ready(Err(_)) => Err(IncomingStreamsError::ReceiverClosed),
            _ => Ok(()),
        }?;

        match self.bye.poll_unpin(cx) {
            Poll::Pending => Ok(()),
            _ => Err(IncomingStreamsError::ClientClosed),
        }?;

        let mut recvd = 0;
        loop {
            let (send, recv) = match self.streams.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => Err(IncomingStreamsError::EndOfBiStream),
                Poll::Ready(Some(r)) => r.map_err(IncomingStreamsError::AcceptBiStream),
            }?;
            let sender = self.sender.clone();
            tokio::spawn(async move {
                let mut read_stream = SymmetricallyFramed::new(
                    FramedRead::new(recv, LengthDelimitedCodec::new()),
                    SymmetricalBincode::<(Option<String>, u32)>::default(),
                );
                let (peer, chanid) = match read_stream.try_next().await {
                    Err(e) => {
                        tracing::warn!("drive_streams_recv: {:?}", e);
                        return;
                    }
                    Ok(msg) => match msg {
                        Some(peer_chanid) => peer_chanid,
                        None => {
                            tracing::warn!("drive_streams_recv: unexpected end of stream");
                            return;
                        }
                    },
                };
                let instream = read_stream.into_inner();
                let outstream = FramedWrite::new(send, LengthDelimitedCodec::new());
                sender
                    .unbounded_send((peer, chanid, outstream, instream))
                    .ok();
            });
            recvd += 1;
            if recvd >= MAX_LOOPS {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

pub(super) struct IncomingStreamsRef(Arc<Mutex<IncomingStreamsInner>>);

impl std::ops::Deref for IncomingStreamsRef {
    type Target = Mutex<IncomingStreamsInner>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Clone for IncomingStreamsRef {
    fn clone(&self) -> Self {
        self.lock().unwrap().ref_count += 1;
        Self(self.0.clone())
    }
}

impl Drop for IncomingStreamsRef {
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

impl IncomingStreamsRef {
    pub(super) fn new(
        client: oneshot::Sender<()>,
        bye: oneshot::Receiver<()>,
        streams: IncomingBiStreams,
    ) -> (Self, mpsc::UnboundedReceiver<NewInStream>) {
        let (sender, incoming) = mpsc::unbounded();
        (
            Self(Arc::new(Mutex::new(IncomingStreamsInner {
                client: Some(client),
                bye,
                streams,
                ref_count: 0,
                driver: None,
                sender,
            }))),
            incoming,
        )
    }
}

#[must_use = "IncomingStreamsDriver must be spawned!"]
pub(super) struct IncomingStreamsDriver(pub(super) IncomingStreamsRef);

impl Future for IncomingStreamsDriver {
    type Output = Result<(), IncomingStreamsError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let inner = &mut *self.0.lock().unwrap();
        match &inner.driver {
            Some(w) if w.will_wake(cx.waker()) => (),
            _ => inner.driver = Some(cx.waker().clone()),
        };
        loop {
            if !inner.drive_streams_recv(cx)? {
                break;
            }
        }
        if inner.ref_count == 0 {
            // driver is alone now; die
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

impl Drop for IncomingStreamsDriver {
    fn drop(&mut self) {
        let mut inner = self.0.lock().unwrap();
        // tell the coord chan eriver that we died
        inner.client.take().unwrap().send(()).ok();
    }
}
