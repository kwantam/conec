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
use futures::channel::oneshot;
use futures::prelude::*;
use quinn::{ConnectionError, IncomingBiStreams};
use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use tokio_serde::{formats::SymmetricalBincode, SymmetricallyFramed};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

///! Error variant output by [ConnectingInStream] future
#[derive(Debug, Error)]
pub enum InStreamError {
    ///! Failed to read the initial message from the stream
    #[error(display = "Reading InitMsg from stream: {:?}", _0)]
    StreamRead(#[source] io::Error),
    ///! Failed to parse initial message
    #[error(display = "Deserializing InitMsg")]
    InitMsg,
    ///! Connection was canceled
    #[error(display = "Incoming connection canceled: {:?}", _0)]
    Canceled(#[source] oneshot::Canceled),
}

///! Ok variant output by [ConnectingInStream] future
pub type NewInStream = (String, u32, OutStream, InStream);

///! An incoming stream that is currently connecting
pub struct ConnectingInStream(oneshot::Receiver<Result<NewInStream, InStreamError>>);

impl Future for ConnectingInStream {
    type Output = Result<NewInStream, InStreamError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let inner = &mut self.0;
        match inner.poll_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(InStreamError::Canceled(e))),
            Poll::Ready(Ok(Err(e))) => Poll::Ready(Err(e)),
            Poll::Ready(Ok(Ok(i))) => Poll::Ready(Ok(i)),
        }
    }
}

///! Error variant output by [IncomingStreams]
#[derive(Debug, Error)]
pub enum IncomingStreamsError {
    ///! Transport unexpectedly stopped delivering new streams
    #[error(display = "Unexpected end of Bi stream")]
    EndOfBiStream,
    ///! Client's connection to Coordinator disappeared
    #[error(display = "Client is gone")]
    ClientClosed,
    ///! Error while accepting new stream from transport
    #[error(display = "Accepting Bi stream: {:?}", _0)]
    AcceptBiStream(#[source] ConnectionError),
}

pub(super) struct IncomingStreamsInner {
    client: Option<oneshot::Sender<()>>,
    bye: oneshot::Receiver<()>,
    streams: IncomingBiStreams,
    incoming: VecDeque<ConnectingInStream>,
    ref_count: usize,
    driver: Option<Waker>,
    driver_lost: bool,
    incoming_reader: Option<Waker>,
}

impl IncomingStreamsInner {
    fn drive_streams_recv(&mut self, cx: &mut Context) -> Result<bool, IncomingStreamsError> {
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
            let (sender, receiver) = oneshot::channel();
            tokio::spawn(async move {
                let mut read_stream = SymmetricallyFramed::new(
                    FramedRead::new(recv, LengthDelimitedCodec::new()),
                    SymmetricalBincode::<(String, u32)>::default(),
                );
                let (peer, chanid) = match read_stream.try_next().await {
                    Err(e) => {
                        sender.send(Err(InStreamError::StreamRead(e))).ok();
                        return;
                    }
                    Ok(msg) => match msg {
                        Some(peer_chanid) => peer_chanid,
                        None => {
                            sender.send(Err(InStreamError::InitMsg)).ok();
                            return;
                        }
                    },
                };
                let instream = read_stream.into_inner();
                let outstream = FramedWrite::new(send, LengthDelimitedCodec::new());
                sender.send(Ok((peer, chanid, outstream, instream))).ok();
            });
            self.incoming.push_back(ConnectingInStream(receiver));
            if let Some(task) = self.incoming_reader.take() {
                task.wake();
            }
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
    ) -> Self {
        Self(Arc::new(Mutex::new(IncomingStreamsInner {
            client: Some(client),
            bye,
            streams,
            incoming: VecDeque::new(),
            ref_count: 0,
            driver: None,
            driver_lost: false,
            incoming_reader: None,
        })))
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
        inner.driver_lost = true;
        inner.client.take().unwrap().send(()).ok();
        if let Some(task) = inner.incoming_reader.take() {
            task.wake();
        }
    }
}

///! A [Stream] of incoming data streams from the Coordinator
///
/// See [library documentation](../index.html) for a usage example.
pub struct IncomingStreams(pub(super) IncomingStreamsRef);

impl Stream for IncomingStreams {
    type Item = ConnectingInStream;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let inner = &mut *self.0.lock().unwrap();
        if let Some(inc) = inner.incoming.pop_front() {
            Poll::Ready(Some(inc))
        } else if inner.driver_lost {
            Poll::Ready(None)
        } else {
            inner.incoming_reader = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}
