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
use std::str::from_utf8;
use std::task::{Context, Poll};
use tokio_serde::Deserializer;

/// Coordinator tags messages to broadcast streams with the sender's ID.
/// This adapter drops the sender's ID and returns only the data.
pub struct TaglessBroadcastInStream<T, E>(T, PhantomData<*const E>);

// Unpin just when the incoming stream and the codec are both Unpin
impl<T: Unpin, E> Unpin for TaglessBroadcastInStream<T, E> {}

impl<T, E> TaglessBroadcastInStream<T, E> {
    /// Create from a stream
    pub fn new(recv: T) -> Self {
        Self(recv, PhantomData)
    }

    /// Consume `self`, returning the enclosed stream
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
        use std::io::{Error, ErrorKind::InvalidData};
        match self.0.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(mut buf))) => {
                let buf_len = buf.len();
                let id_len = (&buf[buf_len - 4..]).get_u32() as usize;
                if buf_len < id_len + 4 {
                    return Poll::Ready(Some(Err(Error::new(InvalidData, "IdLength").into())));
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

/// Coordinator tags messages to broadcast streams with the sender's ID.
/// This adapter returns a tuple `(id: String, data: BytesMut)`.
pub struct TaggedBroadcastInStream<T, E>(T, PhantomData<*const E>);

// Unpin just when the incoming stream and the codec are both Unpin
impl<T: Unpin, E> Unpin for TaggedBroadcastInStream<T, E> {}

impl<T, E> TaggedBroadcastInStream<T, E> {
    /// Create from a stream
    pub fn new(recv: T) -> Self {
        Self(recv, PhantomData)
    }

    /// Consume `self`, returning the enclosed stream
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T, E> Stream for TaggedBroadcastInStream<T, E>
where
    T: Stream<Item = Result<BytesMut, E>> + Unpin,
    io::Error: Into<E>,
{
    type Item = Result<(String, BytesMut), E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use std::io::{Error, ErrorKind::InvalidData};
        match self.0.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(Some(Ok(mut buf))) => {
                let buf_len = buf.len();
                let id_len = (&buf[buf_len - 4..]).get_u32() as usize;
                if buf_len < id_len + 4 {
                    return Poll::Ready(Some(Err(Error::new(InvalidData, "IdLength").into())));
                }
                let id = String::from(match from_utf8(&buf[buf_len - 4 - id_len..buf_len - 4]) {
                    Ok(s) => s,
                    Err(_) => return Poll::Ready(Some(Err(Error::new(InvalidData, "Utf8").into()))),
                });
                buf.truncate(buf_len - id_len - 4);
                Poll::Ready(Some(Ok((id, buf))))
            }
        }
    }
}

/// This adapter applies a [Deserializer](tokio_serde::Deserializer) to the
/// `data` output from [TaggedBroadcastInStream], returning `(id: String, data: T)`
/// for T the output of the Deserializer.
pub struct TaggedDeserializer<T, D, O, Et, Ec>(T, D, PhantomData<*const (O, Et, Ec)>);

// Unpin just when the incoming stream and the codec are both Unpin
impl<T: Unpin, D: Unpin, O, Et, Ec> Unpin for TaggedDeserializer<T, D, O, Et, Ec> {}

impl<T, D: Unpin, O, Et, Ec> TaggedDeserializer<T, D, O, Et, Ec> {
    /// Create from a stream and a deserializer
    pub fn new(recv: T, deserializer: D) -> Self {
        Self(recv, deserializer, PhantomData)
    }

    /// Consume `self`, returning the enclosed stream
    pub fn into_inner(self) -> T {
        self.0
    }

    fn get_td(&mut self) -> (&mut T, Pin<&mut D>) {
        (&mut self.0, Pin::new(&mut self.1))
    }
}

impl<T, D, O, Et, Ec> Stream for TaggedDeserializer<T, D, O, Et, Ec>
where
    T: Stream<Item = Result<(String, BytesMut), Et>> + Unpin,
    D: Deserializer<O, Error = Ec> + Unpin,
    io::Error: Into<Et>,
    Ec: Into<io::Error>,
{
    type Item = Result<(String, O), Et>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let (recv, deser) = self.get_td();
        match recv.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(Some(Ok((id, buf)))) => {
                let res = match deser.deserialize(&buf) {
                    Ok(r) => r,
                    Err(e) => return Poll::Ready(Some(Err(e.into().into()))),
                };
                Poll::Ready(Some(Ok((id, res))))
            }
        }
    }
}
