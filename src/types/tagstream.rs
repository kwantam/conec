// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use super::InStream;

use bytes::{Buf, BufMut, BytesMut};
use futures::prelude::*;
use std::io;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Coordinator tags messages to broadcast streams with the sender's ID.
/// This adapter drops the sender's ID and returns only the message that was sent.
pub struct TaglessBroadcastInStream<T, E>(T, PhantomData<*const E>);

impl<T: Unpin, E> Unpin for TaglessBroadcastInStream<T, E> {}

impl<T, E> TaglessBroadcastInStream<T, E> {
    /// Create from the InStream returned by [Client::new_broadcast](crate::Client::new_broadcast).
    pub fn new(recv: T) -> Self {
        Self(recv, PhantomData)
    }

    /// Consume `self`, returning the enclosed InStream
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T, E> Stream for TaglessBroadcastInStream<T, E>
where
    T: Stream<Item = Result<BytesMut, E>> + Unpin,
    io::Error: Into<E>,
{
    type Item = Result<BytesMut, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        match self.0.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(mut buf))) => {
                let buf_len = buf.len();
                let id_len = (&buf[buf_len - 4..]).get_u32() as usize;
                if buf_len < id_len + 4 {
                    return Poll::Ready(Some(Err(io::Error::new(io::ErrorKind::InvalidData, "IdLength").into())));
                }
                buf.truncate(buf_len - id_len - 4);
                Poll::Ready(Some(Ok(buf)))
            }
            p => p,
        }
    }
}

pub(crate) struct TaggedInStream {
    recv: InStream,
    tag: Vec<u8>,
}

impl TaggedInStream {
    pub(crate) fn new(recv: InStream, id: String) -> Self {
        let tag = {
            let id_len = id.len();
            let mut tmp = id.into_bytes();
            tmp.put_u32(id_len as u32);
            tmp
        };
        Self { recv, tag }
    }
}

impl Stream for TaggedInStream {
    type Item = Result<BytesMut, io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        match self.recv.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(mut buf))) => {
                buf.put(self.tag.as_ref());
                Poll::Ready(Some(Ok(buf)))
            }
            p => p, // everything else passes through
        }
    }
}
