// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

/*!
This module defines the future used by connecting streams.
*/

use super::chan::{ClientChanEvent, ConnectingChannel};
use super::ichan::{IncomingChannelsEvent, NewChannelError};
use crate::types::{InStream, OutStream};

use err_derive::Error;
use futures::{
    channel::{mpsc, oneshot},
    prelude::*,
};
use quinn::ConnectionError;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Error variant output by [ConnectingStream] future
#[derive(Debug, Error)]
pub enum ConnectingStreamError {
    /// Error injecting event
    #[error(display = "Could not send event")]
    Event,
    /// Coordinator sent us an error
    #[error(display = "Coordinator responded with error")]
    Coord,
    /// Reused stream id
    #[error(display = "Reused stream id")]
    StreamId,
    /// Failed to send initial message
    #[error(display = "Sending initial message: {:?}", _0)]
    InitMsg(#[error(source, no_from)] io::Error),
    /// Failed to flush initial message
    #[error(display = "Flushing init message: {:?}", _0)]
    Flush(#[error(source, no_from)] io::Error),
    /// Failed to open bidirectional channel
    #[error(display = "Opening bidirectional channel: {:?}", _0)]
    OpenBi(#[source] ConnectionError),
    /// Opening channel was canceled
    #[error(display = "Outgoing connection canceled: {:?}", _0)]
    Canceled(#[source] oneshot::Canceled),
    /// OutStream requested for invalid peer name
    #[error(display = "No such peer: {:?}", _0)]
    NoSuchPeer(String),
    /// Failed to initialize stream
    #[error(display = "Stream initialization: {:?}", _0)]
    InitStream(#[source] io::Error),
}
def_into_error!(ConnectingStreamError);

pub(super) type ConnectingStreamHandle = oneshot::Sender<Result<(OutStream, InStream), ConnectingStreamError>>;

type ChannelData = (
    ConnectingChannel,
    mpsc::UnboundedSender<ClientChanEvent>,
    mpsc::UnboundedSender<IncomingChannelsEvent>,
    String,
    u64,
);

/// An outgoing stream that is connecting
pub struct ConnectingStream {
    srec: oneshot::Receiver<Result<(OutStream, InStream), ConnectingStreamError>>,
    connchan: Option<(ChannelData, ConnectingStreamHandle)>,
}

impl ConnectingStream {
    pub(super) fn new(chandata: Option<ChannelData>) -> (Self, Option<ConnectingStreamHandle>) {
        let (ssnd, srec) = oneshot::channel();
        if let Some(chandata) = chandata {
            (
                Self {
                    srec,
                    connchan: Some((chandata, ssnd)),
                },
                None,
            )
        } else {
            (Self { srec, connchan: None }, Some(ssnd))
        }
    }
}

impl Future for ConnectingStream {
    type Output = Result<(OutStream, InStream), ConnectingStreamError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        if let Some(((ref mut chan, _, _, _, _), _)) = self.connchan.as_mut() {
            if let Poll::Ready(rdy) = chan.poll_unpin(cx) {
                use ClientChanEvent::Stream as CCStream;
                use IncomingChannelsEvent::NewStream as ICStream;
                let ((_, csnd, isnd, to, sid), sender) = self.connchan.take().unwrap();

                if let Err(sender) = match rdy {
                    Ok(()) | Err(NewChannelError::Duplicate) => {
                        isnd.unbounded_send(ICStream(to, sid, sender)).map_err(IntoHandle::into)
                    }
                    _ => csnd.unbounded_send(CCStream(to, sid, sender)).map_err(IntoHandle::into),
                } {
                    sender.send(Err(ConnectingStreamError::Event)).ok();
                }
            } else {
                return Poll::Pending;
            }
        }

        match self.srec.poll_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(ConnectingStreamError::Canceled(e))),
            Poll::Ready(Ok(res)) => Poll::Ready(res),
        }
    }
}

trait IntoHandle {
    fn into(e: Self) -> ConnectingStreamHandle;
}

impl IntoHandle for mpsc::TrySendError<ClientChanEvent> {
    fn into(e: mpsc::TrySendError<ClientChanEvent>) -> ConnectingStreamHandle {
        if let ClientChanEvent::Stream(_, _, handle) = e.into_inner() {
            handle
        } else {
            panic!("bad conversion")
        }
    }
}

impl IntoHandle for mpsc::TrySendError<IncomingChannelsEvent> {
    fn into(e: mpsc::TrySendError<IncomingChannelsEvent>) -> ConnectingStreamHandle {
        if let IncomingChannelsEvent::NewStream(_, _, handle) = e.into_inner() {
            handle
        } else {
            panic!("bad conversion")
        }
    }
}
