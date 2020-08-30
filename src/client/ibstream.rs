// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use crate::consts::{BCAST_QUEUE, MAX_LOOPS};
use crate::types::{InStream, OutStream, OutStreamError};

use arraydeque::{ArrayDeque, Wrapping};
use bytes::BytesMut;
use err_derive::Error;
use futures::{channel::oneshot, prelude::*};
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

def_cs_future!(
    ConnectingBcastStream,
    pub(super),
    ConnectingBcastStreamHandle,
    pub(super),
    (OutStream, BcastInStream),
    OutStreamError,
    doc = "A broadcast stream that is connecting"
);

type BcastQueue = ArrayDeque<[BytesMut; BCAST_QUEUE], Wrapping>;

/// Err variant returned by BcastInStream
#[derive(Debug, Error)]
pub enum BcastInStreamError {
    /// Client lagged and missed some messages. Stream can still be read.
    #[error(display = "Lagged and dropped {} messages", _0)]
    Lagged(usize),
    /// Polling the input stream failed
    #[error(display = "Stream poll: {:?}", _0)]
    StreamPoll(#[error(source, no_from)] io::Error),
    /// Codec error (see tokio_serde::formats codecs)
    #[error(display = "Codec error: {:?}", _0)]
    Codec(#[source] io::Error),
}

impl From<BcastInStreamError> for io::Error {
    fn from(err: BcastInStreamError) -> Self {
        io::Error::new(io::ErrorKind::Other, err)
    }
}

pub(super) struct BcastInStreamInner {
    recv: InStream,
    queue: BcastQueue,
    ref_count: usize,
    driver: Option<Waker>,
    lagged: usize,
    closed: bool,
    reader: Option<Waker>,
}

impl BcastInStreamInner {
    fn drive_recv(&mut self, cx: &mut Context) -> Result<Option<bool>, BcastInStreamError> {
        let mut recvd = 0;
        loop {
            let msg = match self.recv.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => {
                    self.closed = true;
                    break;
                }
                Poll::Ready(Some(Err(e))) => Err(BcastInStreamError::StreamPoll(e)),
                Poll::Ready(Some(Ok(msg))) => Ok(msg),
            }?;
            if self.queue.push_back(msg).is_some() {
                self.lagged += 1;
            }
            recvd += 1;
            if recvd >= MAX_LOOPS {
                return Ok(Some(true));
            }
        }
        if recvd > 0 {
            Ok(None)
        } else {
            Ok(Some(false))
        }
    }

    fn run_driver(&mut self, cx: &mut Context) -> Result<(), BcastInStreamError> {
        loop {
            match self.drive_recv(cx)? {
                None => break,
                Some(keep_going) => {
                    if let Some(task) = self.reader.take() {
                        task.wake();
                    }
                    if !keep_going {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

def_ref!(BcastInStreamInner, BcastInStreamRef);
impl BcastInStreamRef {
    pub(super) fn new(recv: InStream) -> Self {
        Self(Arc::new(Mutex::new(BcastInStreamInner {
            recv,
            queue: ArrayDeque::new(),
            ref_count: 0,
            driver: None,
            lagged: 0,
            closed: false,
            reader: None,
        })))
    }
}

def_driver!(BcastInStreamRef, BcastInStreamDriver, BcastInStreamError);

/// Incoming messages from a broadcast stream, or an error indicating the receiver lagged
pub struct BcastInStream(pub(super) BcastInStreamRef);

impl futures::stream::FusedStream for BcastInStream {
    fn is_terminated(&self) -> bool {
        let inner = self.0.lock().unwrap();
        inner.lagged == 0 && inner.closed
    }
}

impl Stream for BcastInStream {
    type Item = Result<BytesMut, BcastInStreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let mut inner = self.0.lock().unwrap();

        // save off the waker --- driver will use it when status changes
        match &inner.reader {
            Some(w) if w.will_wake(cx.waker()) => (),
            _ => inner.reader = Some(cx.waker().clone()),
        };

        // if we lost messages, indicate as much
        if inner.lagged != 0 {
            let lagged = inner.lagged;
            inner.lagged = 0;
            return Poll::Ready(Some(Err(BcastInStreamError::Lagged(lagged))));
        }

        // if we are closed, indicate that too
        if inner.closed {
            return Poll::Ready(None);
        }

        // otherwise, return something from the queue, if it exists
        match inner.queue.pop_front() {
            None => Poll::Pending,
            Some(item) => Poll::Ready(Some(Ok(item))),
        }
    }
}
