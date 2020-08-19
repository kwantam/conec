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
use futures::{channel::mpsc, prelude::*};
use quinn::{ConnectionError, IncomingBiStreams, RecvStream, SendStream};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use tokio_serde::{formats::SymmetricalBincode, SymmetricallyFramed};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

///! A [Stream] of incoming data streams from Client or Coordinator.
///
/// See [library documentation](../index.html) for a usage example.
pub type IncomingStreams = mpsc::UnboundedReceiver<NewInStream>;

///! Output by [IncomingStreams]
pub type NewInStream = (Option<String>, u32, OutStream, InStream);

///! Error variant output by [IncomingStreamsDriver]
#[derive(Debug, Error)]
pub enum IncomingStreamsError {
    ///! Transport unexpectedly stopped delivering new streams
    #[error(display = "Unexpected end of Bi stream")]
    EndOfBiStream,
    ///! Incoming streams receiver disappeared
    #[error(display = "IncomingStreams receiver is gone")]
    ReceiverClosed,
    ///! Error while accepting new stream from transport
    #[error(display = "Accepting Bi stream: {:?}", _0)]
    AcceptBiStream(#[source] ConnectionError),
}

pub(super) struct IncomingStreamsInner {
    ibi: IncomingBiStreams,
    ref_count: usize,
    driver: Option<Waker>,
    sender: mpsc::UnboundedSender<NewInStream>,
}

impl IncomingStreamsInner {
    pub(super) fn stream_init(
        send: SendStream,
        recv: RecvStream,
        sender: mpsc::UnboundedSender<NewInStream>,
    ) {
        tokio::spawn(async move {
            let mut read_stream = SymmetricallyFramed::new(
                FramedRead::new(recv, LengthDelimitedCodec::new()),
                SymmetricalBincode::<(Option<String>, u32)>::default(),
            );
            let (peer, chanid) = match read_stream.try_next().await {
                Err(e) => {
                    tracing::warn!("stream_init: {:?}", e);
                    return;
                }
                Ok(msg) => match msg {
                    Some(peer_chanid) => peer_chanid,
                    None => {
                        tracing::warn!("stream_init: unexpected end of stream");
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
    }

    fn handle_events(&mut self, cx: &mut Context) -> Result<(), IncomingStreamsError> {
        match self.sender.poll_ready(cx) {
            Poll::Ready(Err(_)) => Err(IncomingStreamsError::ReceiverClosed),
            _ => Ok(()),
        }
    }

    fn drive_streams_recv(&mut self, cx: &mut Context) -> Result<bool, IncomingStreamsError> {
        let mut recvd = 0;
        loop {
            let (send, recv) = match self.ibi.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => Err(IncomingStreamsError::EndOfBiStream),
                Poll::Ready(Some(r)) => r.map_err(IncomingStreamsError::AcceptBiStream),
            }?;
            Self::stream_init(send, recv, self.sender.clone());
            recvd += 1;
            if recvd >= MAX_LOOPS {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn run_driver(&mut self, cx: &mut Context) -> Result<(), IncomingStreamsError> {
        loop {
            self.handle_events(cx)?;
            let keep_going = self.drive_streams_recv(cx)?;
            if !keep_going {
                return Ok(());
            }
        }
    }
}

def_ref!(IncomingStreamsInner, IncomingStreamsRef);
impl IncomingStreamsRef {
    pub(super) fn new(ibi: IncomingBiStreams, sender: mpsc::UnboundedSender<NewInStream>) -> Self {
        Self(Arc::new(Mutex::new(IncomingStreamsInner {
            ibi,
            ref_count: 0,
            driver: None,
            sender,
        })))
    }
}

def_driver!(
    IncomingStreamsRef,
    IncomingStreamsDriver,
    IncomingStreamsError
);
