// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use bytes::{Bytes, BytesMut};
use futures::prelude::*;
use quinn::{RecvStream, SendStream};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

pub(crate) type FramedRecvStream = FramedRead<RecvStream, LengthDelimitedCodec>;
pub(crate) type FramedSendStream = FramedWrite<SendStream, LengthDelimitedCodec>;

pub struct InStream {
    s_recv: FramedRecvStream,
}

impl InStream {
    pub(crate) fn from_framed(s_recv: FramedRecvStream) -> Self {
        InStream { s_recv }
    }
}

impl Stream for InStream {
    type Item = io::Result<BytesMut>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.s_recv.poll_next_unpin(cx)
    }
}

pub struct OutStream {
    s_send: FramedSendStream,
}

impl OutStream {
    pub(crate) fn from_framed(s_send: FramedSendStream) -> Self {
        OutStream { s_send }
    }

    pub async fn finish(&mut self) -> io::Result<()> {
        self.s_send.flush().await?;
        self.s_send
            .get_mut()
            .finish()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }
}

impl Sink<Bytes> for OutStream {
    type Error = io::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.s_send.poll_ready_unpin(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Bytes) -> Result<(), Self::Error> {
        self.s_send.start_send_unpin(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.s_send.poll_flush_unpin(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.s_send.poll_close_unpin(cx)
    }
}

/*
pub struct InOutStream {
    s_send: OutStream,
    s_recv: InStream,
}

impl InOutStream {
    pub fn new(s_send: OutStream, s_recv: InStream) -> Self {
        InOutStream { s_send, s_recv }
    }

    pub async fn finish(&mut self) -> io::Result<()> {
        self.s_send.finish().await
    }
}

impl Stream for InOutStream {
    type Item = io::Result<BytesMut>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.s_recv.poll_next_unpin(cx)
    }
}

impl Sink<Bytes> for InOutStream {
    type Error = io::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.s_send.poll_ready_unpin(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Bytes) -> Result<(), Self::Error> {
        self.s_send.start_send_unpin(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.s_send.poll_flush_unpin(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.s_send.poll_close_unpin(cx)
    }
}
*/
