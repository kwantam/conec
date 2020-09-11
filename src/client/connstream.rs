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

use crate::types::{InStream, OutStream};

use err_derive::Error;
use futures::{channel::oneshot, prelude::*};
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

def_cs_future!(
    ConnectingStream,
    pub(super),
    ConnectingStreamHandle,
    pub(super),
    (OutStream, InStream),
    ConnectingStreamError,
    doc = "An outgoing stream that is connecting"
);
