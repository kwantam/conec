// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use super::InStream;
use crate::consts::MAX_LOOPS;

use bytes::BytesMut;
use err_derive::Error;
use futures::prelude::*;
use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

/// Err variant returned by NonblockingInStream
#[derive(Debug, Error)]
pub enum NonblockingInStreamError {
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

// this is needed for doing recv.try_next().await?
impl From<NonblockingInStreamError> for io::Error {
    fn from(err: NonblockingInStreamError) -> Self {
        io::Error::new(io::ErrorKind::Other, err)
    }
}

struct BytesMutQueue {
    queue: VecDeque<BytesMut>,
    buflen: usize,
}

impl BytesMutQueue {
    fn new(buflen: usize) -> Self {
        let queue = VecDeque::with_capacity(buflen);
        Self { queue, buflen }
    }

    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    fn push_back(&mut self, msg: BytesMut) -> Option<BytesMut> {
        let ret = if self.queue.len() >= self.buflen {
            self.queue.pop_front()
        } else {
            None
        };
        self.queue.push_back(msg);
        ret
    }

    fn pop_front(&mut self) -> Option<BytesMut> {
        self.queue.pop_front()
    }
}

pub(super) struct NblkInStreamInner {
    recv: InStream,
    queue: BytesMutQueue,
    ref_count: usize,
    driver: Option<Waker>,
    lagged: usize,
    closed: bool,
    reader: Option<Waker>,
}

impl NblkInStreamInner {
    fn drive_recv(&mut self, cx: &mut Context) -> Result<Option<bool>, NonblockingInStreamError> {
        if self.closed {
            return Ok(None); // don't poll stream again if it was previously closed
        }

        let mut recvd = 0;
        loop {
            let msg = match self.recv.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => {
                    self.closed = true;
                    recvd = 1; // force reader wakeup
                    break;
                }
                Poll::Ready(Some(Err(e))) => Err(NonblockingInStreamError::StreamPoll(e)),
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
        if recvd == 0 {
            Ok(None)
        } else {
            Ok(Some(false))
        }
    }

    fn run_driver(&mut self, cx: &mut Context) -> Result<(), NonblockingInStreamError> {
        loop {
            match self.drive_recv(cx)? {
                None => break, // nothing received; don't wake waiting reader
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

def_ref!(NblkInStreamInner, NblkInStreamRef, pub(self));
impl NblkInStreamRef {
    fn new(recv: InStream, buflen: usize) -> Self {
        Self(Arc::new(Mutex::new(NblkInStreamInner {
            recv,
            queue: BytesMutQueue::new(buflen),
            ref_count: 0,
            driver: None,
            lagged: 0,
            closed: false,
            reader: None,
        })))
    }
}

def_driver!(pub(self), NblkInStreamRef; pub(self), NblkInStreamDriver; NonblockingInStreamError);

/// An adapter to make an InStream non-blocking from the sender's perspective
///
/// By default, OutStreams are blocking: receiving client(s) have finite buffering, and
/// once it is full they must drain the buffer before another message can be sent. This
/// can produce undesirable behavior, especially with broadcast streams where some clients
/// are slow to read.
///
/// This adapter can be used to prevent the slow receiver problem. Specifically, any client
/// that wraps an InStream with this adapter will automatically read messages into
/// a ring buffer upon arrival. If the ring buffer becomes full, the oldest message will be
/// overwritten. At the next read, the client will get a [NonblockingInStreamError::Lagged]
/// error indicating that they have missed some number of messages, after which they can resume
/// reading messages from the stream as normal.
///
/// Note that to prevent blocking for broadcast streams, *all* clients must apply this adapter
/// to their InStream. This library does not enforce this---it is up to the application to do so.
/// It is possible to mix nonblocking and blocking clients, e.g., making only the slow clients
/// nonblocking.
///
/// This adapter is compatible with tokio-serde's Framed struct, and in particular it should
/// work with any of the tokio_serde::formats codecs. See `tests.rs` and the [crate-level
/// documentation](index.html#making-streams-non-blocking) for examples.
pub struct NonblockingInStream(NblkInStreamRef);

impl NonblockingInStream {
    /// Create a new NonblockingInStream from an InStream, with a buffer of size `buflen`
    pub fn new(recv: InStream, buflen: usize) -> Self {
        let inner = NblkInStreamRef::new(recv, buflen);
        let driver = NblkInStreamDriver(inner.clone());
        tokio::spawn(async move { driver.await });
        Self(inner)
    }
}

impl futures::stream::FusedStream for NonblockingInStream {
    fn is_terminated(&self) -> bool {
        let inner = self.0.lock().unwrap();
        inner.lagged == 0 && inner.queue.is_empty() && inner.closed
    }
}

impl Stream for NonblockingInStream {
    type Item = Result<BytesMut, NonblockingInStreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let mut inner = self.0.lock().unwrap();
        // cancel pending wakeup request --- below, we restore if necessary.
        let reader = inner.reader.take();

        // if we lost messages, indicate as much
        if inner.lagged != 0 {
            let lagged = inner.lagged;
            inner.lagged = 0;
            return Poll::Ready(Some(Err(NonblockingInStreamError::Lagged(lagged))));
        }

        // otherwise, return something from the queue, if it exists
        match inner.queue.pop_front() {
            Some(item) => Poll::Ready(Some(Ok(item))),
            None => {
                if inner.closed {
                    // now we are closed
                    Poll::Ready(None)
                } else {
                    // save off the waker --- driver will use it when status changes
                    inner.reader.replace(match reader {
                        Some(w) if w.will_wake(cx.waker()) => w,
                        _ => cx.waker().clone(),
                    });
                    Poll::Pending
                }
            }
        }
    }
}
